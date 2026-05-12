use std::{
    fs::{self, File},
    io::{Cursor, Read},
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
};

use axum_jwt_auth::JwtDecoder;
use chrono::{Duration, Utc};
use clap::{Parser, ValueEnum};
use http::uri::Authority;
use openid::{CompactJson, CustomClaims, StandardClaims};
use redis::{ConnectionAddr, IntoConnectionInfo, RedisError};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

use crate::{const_values::CONEXT_FORGE_GATEWAY_AUDIENCE, user_config_store::UserConfigStore};

#[derive(Clone)]
pub struct ContextForgeGatewayAppState {
    pub(crate) jwt_token_decoder: Arc<dyn JwtDecoder<ContextForgeGatewayClaims> + Send + Sync>,
    pub(crate) config_store: Arc<dyn UserConfigStore + Send + Sync>,
    pub(crate) config: Config,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ContextForgeGatewayClaims {
    pub additional_claim: Option<String>,
    #[serde(flatten)]
    pub standard_claims: StandardClaims,
}

impl CustomClaims for ContextForgeGatewayClaims {
    fn standard_claims(&self) -> &StandardClaims {
        &self.standard_claims
    }
}

impl CompactJson for ContextForgeGatewayClaims {}

pub type RedisClient = redis::Client;

#[derive(Debug, Clone)]
pub enum RedisConfig {
    PlainText { host: String, port: u16 },

    Tls { host: String, port: u16, trust_bundle: Vec<u8> },
    MTls { host: String, port: u16, trust_bundle: Vec<u8>, client_cert: Vec<u8>, client_key: Vec<u8> },
}

impl TryFrom<RedisConfig> for RedisClient {
    type Error = RedisError;

    fn try_from(redis_config: RedisConfig) -> Result<Self, Self::Error> {
        match redis_config {
            RedisConfig::PlainText { host, port } => {
                Ok(RedisClient::open(ConnectionAddr::Tcp(host, port).into_connection_info()?)?)
            },
            RedisConfig::Tls { host, port, trust_bundle } => RedisClient::build_with_tls(
                format!("rediss://{host}:{port}"),
                redis::TlsCertificates { client_tls: None, root_cert: Some(trust_bundle) },
            ),
            RedisConfig::MTls { host, port, trust_bundle, client_cert, client_key } => RedisClient::build_with_tls(
                format!("rediss://{host}:{port}"),
                redis::TlsCertificates {
                    client_tls: Some(redis::ClientTlsConfig { client_cert, client_key }),
                    root_cert: Some(trust_bundle),
                },
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum UpstreamConnectionMode {
    PlainTextOrTls,
    PlainTextOrMTls,
    TlsOnly,
    MtlsOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
#[derive(Default)]
pub enum RedisConnectionMode {
    PlainText,
    #[default]
    Tls,
    Mtls,
}

#[derive(Debug, Clone, Parser, Default)]
#[command(name = "contextforge-gateway-rs")]
#[command(about = "Minimal, fast and experimental Gateway/Dataplane for ContextForge")]
pub struct Config {
    #[arg(long, env = "CONTEXTFORGE_GATEWAY_RS_ADDRESS")]
    pub address: Option<SocketAddr>,

    #[arg(long, env = "CONTEXTFORGE_GATEWAY_RS_TOKEN_VERIFICATION_PUBLIC_KEY")]
    pub token_verification_public_key: PathBuf,

    #[cfg(feature = "with_tools")]
    #[arg(long, env = "CONTEXTFORGE_GATEWAY_RS_TOKEN_VERIFICATION_PRIVATE_KEY")]
    pub token_verification_private_key: PathBuf,

    #[arg(long, env = "CONTEXTFORGE_GATEWAY_RS_ENABLE_OPEN_TELEMETRY")]
    pub enable_open_telemetry: Option<bool>,

    #[arg(long, env = "CONTEXTFORGE_GATEWAY_RS_GATEWAY_CPUS")]
    pub number_of_cpus: Option<usize>,

    #[arg(long, env = "CONTEXTFORGE_GATEWAY_RS_SINGLE_RUNTIME")]
    pub single_runtime: Option<bool>,

    #[arg(long, env = "CONTEXTFORGE_GATEWAY_RS_TLS_ADDRESS")]
    pub tls_address: Option<SocketAddr>,

    #[arg(long, env = "CONTEXTFORGE_GATEWAY_RS_TLS_SERVER_PRIVATE_KEY")]
    pub server_private_key: Option<PathBuf>,

    #[arg(long, env = "CONTEXTFORGE_GATEWAY_RS_TLS_SERVER_CERTIFICATE")]
    pub server_certificate: Option<PathBuf>,

    #[arg(long, env = "CONTEXTFORGE_GATEWAY_RS_UPSTREAM_CONNECTION_MODE")]
    pub upstream_connection_mode: Option<UpstreamConnectionMode>,

    #[arg(long, env = "CONTEXTFORGE_GATEWAY_RS_TLS_UPSTREAM_PRIVATE_KEY")]
    pub upstream_private_key: Option<PathBuf>,

    #[arg(long, env = "CONTEXTFORGE_GATEWAY_RS_TLS_UPSTREAM_CERTIFICATE")]
    pub upstream_certificate: Option<PathBuf>,

    #[arg(long, env = "CONTEXTFORGE_GATEWAY_RS_TLS_UPSTREAM_TRUST_BUNDLE")]
    pub upstream_trust_bundle: Option<PathBuf>,

    #[arg(long, env = "CONTEXTFORGE_GATEWAY_RS_REDIS_HOSTNAME")]
    pub redis_address: String,
    #[arg(long, env = "CONTEXTFORGE_GATEWAY_RS_REDIS_PORT")]
    pub redis_port: u16,

    #[arg(long, env = "CONTEXTFORGE_GATEWAY_RS_REDIS_CONNECTION_MODE")]
    pub redis_mode: RedisConnectionMode,

    #[arg(long, env = "CONTEXTFORGE_GATEWAY_RS_REDIS_TLS_REDIS_TRUST_BUNDLE")]
    pub redis_tls_trust_bundle: Option<PathBuf>,

    #[arg(long, env = "CONTEXTFORGE_GATEWAY_RS_REDIS_TLS_REDIS_CLIENT_PRIVATE_KEY")]
    pub redis_tls_client_private_key: Option<PathBuf>,

    #[arg(long, env = "CONTEXTFORGE_GATEWAY_RS_REDIS_TLS_REDIS_CLIENT_CERTIFICATE")]
    pub redis_tls_client_certificate: Option<PathBuf>,
}

#[derive(Error, Debug)]
pub enum ConfigValidationError {
    #[error("Redis Configuration Error")]
    RedisConfigurationError(String),
}

impl TryFrom<&Config> for RedisConfig {
    fn try_from(value: &Config) -> Result<Self, Self::Error> {
        let _: Authority = format!("{}:{}", value.redis_address, value.redis_port)
            .parse::<Authority>()
            .map_err(|e| ConfigValidationError::RedisConfigurationError(e.to_string()))?;

        match value.redis_mode {
            RedisConnectionMode::PlainText => {
                Ok(Self::PlainText { host: value.redis_address.clone(), port: value.redis_port })
            },
            RedisConnectionMode::Tls => {
                let Some(trust_bundle) = &value.redis_tls_trust_bundle else {
                    return Err(ConfigValidationError::RedisConfigurationError(format!(
                        "Trust bundle is required for Redis {:?}",
                        value.redis_mode
                    )));
                };

                let trust_bundle = validate_certs(trust_bundle)?;

                Ok(Self::Tls { host: value.redis_address.clone(), port: value.redis_port, trust_bundle })
            },
            RedisConnectionMode::Mtls => {
                let Some(trust_bundle) = &value.redis_tls_trust_bundle else {
                    return Err(ConfigValidationError::RedisConfigurationError(format!(
                        "Trust bundle is required for Redis {:?}",
                        value.redis_mode
                    )));
                };

                let trust_bundle = validate_certs(trust_bundle)?;

                let Some(certificate) = &value.redis_tls_client_certificate else {
                    return Err(ConfigValidationError::RedisConfigurationError(format!(
                        "Client certificate is required for Redis {:?}",
                        value.redis_mode
                    )));
                };

                let client_cert = validate_certs(certificate)?;

                let Some(key) = &value.redis_tls_client_private_key else {
                    return Err(ConfigValidationError::RedisConfigurationError(format!(
                        "Client key is required for Redis {:?}",
                        value.redis_mode
                    )));
                };

                let client_key = validate_key(key)?;

                Ok(Self::MTls {
                    host: value.redis_address.clone(),
                    port: value.redis_port,
                    trust_bundle,
                    client_cert,
                    client_key,
                })
            },
        }
    }

    type Error = ConfigValidationError;
}

fn validate_certs(path: &PathBuf) -> Result<Vec<u8>, ConfigValidationError> {
    let mut buf = Vec::new();
    File::open(path)
        .map_err(|e| ConfigValidationError::RedisConfigurationError(e.to_string()))?
        .read_to_end(&mut buf)
        .map_err(|e| ConfigValidationError::RedisConfigurationError(e.to_string()))?;
    let mut cursor = Cursor::new(buf);

    let mut count = 0;
    for cert in rustls_pemfile::certs(&mut cursor) {
        if let Err(e) = cert {
            return Err(ConfigValidationError::RedisConfigurationError(e.to_string()));
        }
        count += 1;
    }
    if count == 0 {
        Err(ConfigValidationError::RedisConfigurationError("No certificates provided".to_owned()))
    } else {
        Ok(cursor.into_inner())
    }
}

fn validate_key(path: &PathBuf) -> Result<Vec<u8>, ConfigValidationError> {
    let mut buf = Vec::new();
    File::open(path)
        .map_err(|e| ConfigValidationError::RedisConfigurationError(e.to_string()))?
        .read_to_end(&mut buf)
        .map_err(|e| ConfigValidationError::RedisConfigurationError(e.to_string()))?;
    let mut cursor = Cursor::new(buf);

    if let Ok(Some(_)) = rustls_pemfile::private_key(&mut cursor) {
        Ok(cursor.into_inner())
    } else {
        Err(ConfigValidationError::RedisConfigurationError("Private key is wrong".to_owned()))
    }
}

impl TryFrom<&Config> for reqwest::Client {
    type Error = crate::Error;

    fn try_from(config: &Config) -> Result<Self, Self::Error> {
        let builder = reqwest::Client::builder();
        let builder = match config.upstream_connection_mode.as_ref() {
            None | Some(UpstreamConnectionMode::TlsOnly) => builder.https_only(true),
            Some(UpstreamConnectionMode::PlainTextOrTls) => builder.https_only(false),
            Some(UpstreamConnectionMode::PlainTextOrMTls) => {
                builder.https_only(false).identity(extract_identity(config)?)
            },
            Some(UpstreamConnectionMode::MtlsOnly) => builder.https_only(true).identity(extract_identity(config)?),
        };

        let builder = if let Some(trust_bundle) = config.upstream_trust_bundle.as_ref() {
            let mut buf = Vec::new();
            File::open(trust_bundle)?.read_to_end(&mut buf)?;
            let certificates = reqwest::Certificate::from_pem_bundle(&buf)?;
            builder.tls_certs_merge(certificates)
        } else {
            builder
        };

        Ok(builder.build()?)
    }
}

fn extract_identity(config: &Config) -> crate::Result<reqwest::Identity> {
    match (config.upstream_private_key.as_ref(), config.upstream_certificate.as_ref()) {
        (Some(private_key), Some(certificate)) => {
            let cert = fs::read(certificate)?;
            let key = fs::read(private_key)?;
            Ok(reqwest::Identity::from_pkcs8_pem(&cert, &key)?)
        },

        _ => Err("Invalid/missing configuration".into()),
    }
}

#[derive(Deserialize, Serialize)]
pub struct DefaultClaims {
    iss: Url,
    sub: String,
    aud: String,
    exp: i64,
    iat: Option<i64>,
    userinfo: openid::Userinfo,
}

impl DefaultClaims {
    pub fn new(user_id: String) -> Self {
        let url = "http://contextforge-gateway-rs".parse().expect("Expecting this to work");
        let audience = CONEXT_FORGE_GATEWAY_AUDIENCE.to_owned();
        let user_info = openid::Userinfo { sub: user_id.clone(), ..Default::default() };
        Self {
            iss: url,
            sub: user_id,
            aud: audience,
            exp: (Utc::now() + Duration::hours(1)).timestamp(),
            iat: Some(Utc::now().timestamp()),

            userinfo: user_info,
        }
    }
}
