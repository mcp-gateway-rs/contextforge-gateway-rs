use std::sync::Arc;

use axum::Router;
use http::Request;
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use rustls::ServerConfig;
use rustls_pki_types::{self, CertificateDer, PrivateKeyDer, pem::PemObject};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tower::Service;
use tracing::{error, info, warn};

use crate::{Config, Error, transports::tcp::Tcp};

pub struct DownstreamTls {
    tcp: Tcp,
    server_config: ServerConfig,
}

impl TryFrom<&Config> for Option<DownstreamTls> {
    type Error = Error;

    fn try_from(config: &Config) -> Result<Self, Self::Error> {
        match (config.tls_address, config.server_certificate.clone(), config.server_private_key.clone()) {
            (Some(address), Some(certificate), Some(private_key)) => {
                let certificates = CertificateDer::pem_file_iter(&certificate)?.flatten().collect::<Vec<_>>();
                let private_key = PrivateKeyDer::from_pem_file(&private_key)?;
                let server_config = ServerConfig::builder_with_protocol_versions(rustls::ALL_VERSIONS)
                    .with_no_client_auth()
                    .with_single_cert(certificates, private_key)?;

                if let Some(tcp_address) = config.address
                    && tcp_address == address
                {
                    return Err("Invalid configuration TCP and TLS ports are the same ".into());
                }

                let tcp = Tcp::new(address);
                Ok(Some(DownstreamTls { tcp, server_config }))
            },
            (None, ..) => Ok(None),
            (Some(_), ..) => Err("Invalid tls config... configuration missing ".into()),
        }
    }
}

impl DownstreamTls {
    pub async fn handle_tls(self, service: Router) -> crate::Result<()> {
        let DownstreamTls { tcp, server_config } = self;
        info!("Starting TLS listener at {}", tcp.address);
        let tcp_listener: TcpListener = tcp.try_into()?;

        let tls_acceptor = TlsAcceptor::from(Arc::new(server_config));

        loop {
            tokio::select! {
                    maybe_stream = tcp_listener.accept() => {
                        let tower_service = service.clone();
                        let tls_acceptor = tls_acceptor.clone();

                        if let Ok((tcp_stream, addr)) = maybe_stream {
                            tokio::spawn(async move {
                                let Ok(stream) = tls_acceptor.accept(tcp_stream).await else {
                                    error!("error during tls handshake connection from {}", addr);
                                    return;
                                };

                                let stream = TokioIo::new(stream);

                                let hyper_service = hyper::service::service_fn(move |request: Request<Incoming>| {
                                    tower_service.clone().call(request)
                                });

                                let ret = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                                    .serve_connection_with_upgrades(stream, hyper_service)
                                    .await;

                                if let Err(err) = ret {
                                    warn!("error serving connection from {addr}: {err}");
                                }
                            })
                        } else {
                            warn!("Problem during TCP handshake {maybe_stream:?}");
                            return Err(maybe_stream.expect_err("Expect this to work").into());
                        };
                    }
                    _= tokio::signal::ctrl_c()=>{
                        return Ok(())
                    }

            }
        }
    }
}
