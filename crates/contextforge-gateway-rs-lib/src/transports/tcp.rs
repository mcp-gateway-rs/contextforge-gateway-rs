use std::net::SocketAddr;

use axum::Router;
use tokio::net::{TcpListener, TcpSocket};
use tracing::info;

use crate::Config;

pub struct Tcp {
    pub address: SocketAddr,
}

impl Tcp {
    pub fn new(address: SocketAddr) -> Self {
        Self { address }
    }

    pub async fn handle_tcp(self, service: Router) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        info!("Starting TCP listener at {}", self.address);
        let tcp_listener: TcpListener = self.try_into()?;

        Ok(axum::serve(tcp_listener, service)
            .with_graceful_shutdown(async {
                tokio::signal::ctrl_c().await.ok();
                info!("Shutting down...");
            })
            .await?)
    }
}

impl TryFrom<&Config> for Option<Tcp> {
    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

    fn try_from(config: &Config) -> Result<Self, Self::Error> {
        match config.address.clone() {
            Some(address) => Ok(Some(Tcp { address })),
            None => Ok(None),
        }
    }
}

impl TryInto<TcpListener> for Tcp {
    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

    fn try_into(self) -> Result<TcpListener, Self::Error> {
        let address = self.address;
        let socket = if address.is_ipv4() { TcpSocket::new_v4()? } else { TcpSocket::new_v6()? };
        socket.set_reuseaddr(true)?;
        socket.set_keepalive(true)?;
        #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
        socket.set_reuseport(true)?;
        socket.bind(address)?;
        Ok(socket.listen(1024)?)
    }
}
