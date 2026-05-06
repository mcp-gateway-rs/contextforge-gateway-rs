use std::{
    fs::{self, File},
    io::Read,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
};

use axum_jwt_auth::JwtDecoder;
use chrono::{Duration, Utc};
use clap::{Parser, ValueEnum};
use http::uri::Authority;
use openid::{CompactJson, CustomClaims, StandardClaims};
use redis::{ConnectionAddr, IntoConnectionInfo};
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
pub struct RedisConfig {
    address: String,
    port: u16,
}

impl IntoConnectionInfo for RedisConfig {
    fn into_connection_info(self) -> redis::RedisResult<redis::ConnectionInfo> {
        ConnectionAddr::Tcp(self.address, self.port).into_connection_info()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum UpstreamConnectionMode {
    PlainTextAndTls,
    PlainTextAndMTls,
    TlsOnly,
    MtlsOnly,
}

#[derive(Debug, Clone, Parser, Default)]
#[command(name = "contextforge-gateway-rs")]
#[command(about = "Minimal, fast and experimental Gateway/Dataplane for ContextForge")]
pub struct Config {
    #[arg(long, env = "CONTEXTFORGE_GATEWAY_RS_ADDRESS")]
    pub address: Option<SocketAddr>,
    #[arg(long, env = "CONTEXTFORGE_GATEWAY_RS_REDIS_HOSTNAME")]
    pub redis_address: String,
    #[arg(long, env = "CONTEXTFORGE_GATEWAY_RS_REDIS_PORT")]
    pub redis_port: u16,

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
        Ok(Self { address: value.redis_address.clone(), port: value.redis_port })
    }

    type Error = ConfigValidationError;
}

impl TryFrom<&Config> for reqwest::Client {
    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

    fn try_from(config: &Config) -> Result<Self, Self::Error> {
        let builder = reqwest::Client::builder();
        let builder = match config.upstream_connection_mode.as_ref() {
            None | Some(UpstreamConnectionMode::TlsOnly) => builder.https_only(true),
            Some(UpstreamConnectionMode::PlainTextAndTls) => builder.https_only(false),
            Some(UpstreamConnectionMode::PlainTextAndMTls) => {
                builder.https_only(false).identity(extract_identity(config)?)
            },
            Some(UpstreamConnectionMode::MtlsOnly) => builder.https_only(true).identity(extract_identity(config)?),
        };

        let builder = if let Some(trust_bundle) = config.upstream_trust_bundle.as_ref() {
            let mut buf = Vec::new();
            File::open(trust_bundle)?.read_to_end(&mut buf)?;
            let certificates = reqwest::Certificate::from_pem_bundle(&mut buf)?;
            builder.tls_certs_merge(certificates)
        } else {
            builder
        };
        // let mut header_map = HeaderMap::new();
        // //header_map.insert(http::header::HOST, HeaderValue::from_static("127.0.0.1"));
        //let builder = builder.indefault_headers(header_map);

        Ok(builder.build()?)
    }
}

fn extract_identity(config: &Config) -> Result<reqwest::Identity, Box<dyn std::error::Error + Send + Sync + 'static>> {
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
