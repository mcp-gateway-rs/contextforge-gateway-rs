use std::{
    fs::{self, File},
    io::{Cursor, Read},
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
};

use chrono::Duration;
use clap::{Parser, ValueEnum};
use http::uri::Authority;
use jsonwebtoken::DecodingKey;
use redis::{ConnectionAddr, IntoConnectionInfo, RedisError};
use rustls_pki_types::{CertificateDer, PrivatePkcs8KeyDer, pem::PemObject};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use typed_builder::TypedBuilder;
use uuid::Uuid;

use crate::{
    const_values::{CONTEXT_FORGE_GATEWAY_AUDIENCE, CONTEXT_FORGE_GATEWAY_ISSUER},
    user_config_store::UserConfigStore,
};

#[derive(Clone)]
pub struct JwtTokenDecoders {
    pub rs: Option<DecodingKey>,
    pub hmac_sha: Option<DecodingKey>,
}

#[derive(Clone)]
pub struct ContextForgeGatewayAppState {
    pub(crate) jwt_token_decoding_keys: JwtTokenDecoders,
    pub(crate) config_store: Arc<dyn UserConfigStore + Send + Sync>,
    pub(crate) config: Config,
}

#[derive(Clone, Debug, Serialize, Deserialize, TypedBuilder)]
pub struct User {
    email: String,
    full_name: String,
    is_admin: bool,
    auth_provider: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, TypedBuilder)]
pub struct Scopes {
    server_id: Option<String>,
    permissions: Vec<String>,
    ip_restrictions: Vec<String>,
    time_restrictions: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, TypedBuilder)]
pub struct ContextForgeClaims {
    pub sub: String,
    pub jti: String,
    pub token_use: String,
    pub iat: Option<u64>,
    pub iss: String,
    pub aud: String,
    pub exp: u64,
    pub teams: Option<Vec<String>>,
    pub user: User,
    pub scopes: Scopes,
}

impl ContextForgeClaims {
    pub fn new(user_id: &str) -> Self {
        let audience = CONTEXT_FORGE_GATEWAY_AUDIENCE.to_owned();
        let start = std::time::SystemTime::now();
        let now = start.duration_since(std::time::UNIX_EPOCH).expect("Time went backwards").as_secs();
        Self {
            iss: CONTEXT_FORGE_GATEWAY_ISSUER.to_owned(),
            sub: user_id.to_owned(),
            aud: audience,
            exp: now + Duration::hours(1).num_seconds().cast_unsigned(),
            iat: Some(now),
            jti: Uuid::new_v4().to_string(),
            token_use: "api".to_owned(),
            teams: Some(vec!["team_awesome".to_owned()]),
            user: User::builder()
                .email(user_id.to_owned())
                .auth_provider("api_token".to_owned())
                .full_name("API Token User".to_owned())
                .is_admin(true)
                .build(),
            scopes: Scopes::builder()
                .server_id(Some("my_id".to_owned()))
                .ip_restrictions(vec!["192.169.1.0/24".to_owned()])
                .permissions(vec!["tools.read".to_owned(), "servers.use".to_owned()])
                .time_restrictions(None)
                .build(),
        }
    }
}

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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
#[derive(Default)]
pub enum LogRotation {
    Minutely,
    #[default]
    Hourly,
    Daily,
    Never,
}

#[derive(Debug, Clone, Parser, Default)]
#[command(name = "contextforge-gateway-rs")]
#[command(about = "Minimal, fast and experimental Gateway/Dataplane for ContextForge")]
pub struct Config {
    #[arg(long, env = "CONTEXTFORGE_GATEWAY_RS_ADDRESS")]
    pub address: Option<SocketAddr>,

    #[arg(long, env = "CONTEXTFORGE_GATEWAY_RS_TOKEN_VERIFICATION_PUBLIC_KEY")]
    pub token_verification_public_key: Option<PathBuf>,

    #[cfg(feature = "with_tools")]
    #[arg(long, env = "CONTEXTFORGE_GATEWAY_RS_TOKEN_VERIFICATION_PRIVATE_KEY")]
    pub token_verification_private_key: PathBuf,

    #[arg(long, env = "CONTEXTFORGE_GATEWAY_RS_TOKEN_SECRET")]
    pub token_verification_secret: Option<String>,

    #[arg(long, env = "CONTEXTFORGE_GATEWAY_RS_ENABLE_OPEN_TELEMETRY")]
    pub enable_open_telemetry: Option<bool>,

    #[arg(long, env = "CONTEXTFORGE_GATEWAY_RS_GATEWAY_CPUS")]
    pub number_of_cpus: Option<usize>,

    #[arg(long, env = "CONTEXTFORGE_GATEWAY_RS_SINGLE_RUNTIME")]
    pub single_runtime: Option<bool>,

    #[arg(long, env = "CONTEXTFORGE_GATEWAY_RS_RUNTIME_PLUGINS_ENABLED")]
    pub runtime_plugins_enabled: Option<bool>,

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

    #[arg(long, env = "CONTEXTFORGE_GATEWAY_LOG_NAME")]
    pub log_name: Option<String>,

    #[arg(long, env = "CONTEXTFORGE_GATEWAY_LOG_ROTATION")]
    pub log_rotation: Option<LogRotation>,
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

    let certs = CertificateDer::pem_reader_iter(&mut cursor)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| ConfigValidationError::RedisConfigurationError(e.to_string()))?;

    if certs.is_empty() {
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

    let _ = PrivatePkcs8KeyDer::from_pem_slice(&buf)
        .map_err(|_| ConfigValidationError::RedisConfigurationError("Private key is invalid".to_owned()))?;
    Ok(buf)
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
            let mut cert = fs::read(certificate)?;
            let key = fs::read(private_key)?;
            cert.extend(key);
            Ok(reqwest::Identity::from_pem(&cert)?)
        },

        _ => Err("Invalid/missing configuration".into()),
    }
}
