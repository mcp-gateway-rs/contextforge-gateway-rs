use std::{fs, sync::Arc};

use axum::middleware;
use futures::FutureExt;
use jsonwebtoken::DecodingKey;
use rmcp::transport::{
    StreamableHttpServerConfig,
    streamable_http_server::{session::local::LocalSessionManager, tower::StreamableHttpService},
};
mod common;
mod const_values;
mod gateway;
mod layers;
mod runtime_hooks;
mod transports;

#[cfg(test)]
mod tests;

#[cfg(feature = "with_tools")]
mod tools;

mod user_config_store;
pub use common::{RedisClient, RedisConfig, UpstreamConnectionMode};
use gateway::McpService;
use layers::session_id::SessionId;
use tower_http::cors::{Any, CorsLayer};
use transports::{DownstreamTls, Tcp};
use typed_builder::TypedBuilder;
pub use user_config_store::RedisUserConfigStore;
pub use user_config_store::{ConfigStoreError, UserConfigStore};

pub use crate::common::{Config, LogRotation};
pub use crate::runtime_hooks::{
    GatewayToolRuntime, RuntimeHookError, RuntimeHookState, ToolArgumentsUpdate, ToolPreCallResult,
};

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type Result<T> = std::result::Result<T, Error>;

use crate::{
    common::{ContextForgeGatewayAppState, JwtTokenDecoders},
    gateway::LocalUserSessionStore,
    layers::{
        claims_id::claims_layer, session_id::SessionIdLayer, user_config_store::user_config_store_layer,
        virtual_host_id::virtual_host_id_layer,
    },
};

#[derive(Clone, TypedBuilder)]
#[builder(field_defaults(setter(prefix = "with_")))]
pub struct Gateway {
    config: Config,
    session_manager: Arc<LocalSessionManager>,
    #[builder(default, setter(strip_option))]
    user_config_store: Option<Arc<dyn UserConfigStore + std::marker::Send + Sync>>,
    #[builder(default, setter(strip_option))]
    plugin_runtime: Option<Arc<dyn GatewayToolRuntime>>,
}

impl Gateway {
    pub async fn run_gateway(self) -> Result<()> {
        let config = &self.config;
        let session_manager = self.session_manager;
        let user_config_store = match self.user_config_store {
            Some(store) => store,
            None => Arc::new(get_config_store(config).await?),
        };
        let runtime_plugins_enabled = config.runtime_plugins_enabled.unwrap_or(false);

        let user_session_store = LocalUserSessionStore::new();
        let plugin_runtime = self.plugin_runtime;
        if runtime_plugins_enabled && let Some(plugin_runtime) = &plugin_runtime {
            plugin_runtime.initialize().await?;
        }
        let mcp_plugin_runtime = if runtime_plugins_enabled { plugin_runtime } else { None };

        let streamable_config = StreamableHttpServerConfig::default().disable_allowed_hosts();

        let reqwest_backend_client = reqwest::Client::try_from(config)?;

        // Create streamable HTTP service
        let mcp_service: StreamableHttpService<McpService<LocalUserSessionStore>, LocalSessionManager> =
            StreamableHttpService::new(
                move || {
                    Ok(McpService::builder()
                        .with_user_session_store(user_session_store.clone())
                        .with_http_client(reqwest_backend_client.clone())
                        .with_plugin_runtime(mcp_plugin_runtime.clone())
                        .build())
                },
                session_manager,
                streamable_config,
            );

        let cors_layer = CorsLayer::new().allow_origin(Any).allow_methods(Any).allow_headers(Any).expose_headers(Any);

        let rs_decoding_key = config.token_verification_public_key.as_ref().map(|path| {
            let Ok(key) =
                fs::read(path).map_err(|e| format!("Error when creating local decoder {e:?} {}", path.display()))
            else {
                return Err(format!("Error when creating local decoder. Can't read path {}", path.display()));
            };

            let Ok(key) = DecodingKey::from_rsa_pem(&key) else {
                return Err(format!("Error when creating local decoder. Can't read the key {}", path.display()));
            };
            Ok(key)
        });

        let mcp_add_state: ContextForgeGatewayAppState = ContextForgeGatewayAppState {
            jwt_token_decoding_keys: JwtTokenDecoders {
                rs: rs_decoding_key.transpose()?,
                hmac_sha: config
                    .token_verification_secret
                    .as_ref()
                    .map(|token| DecodingKey::from_secret(token.as_bytes())),
            },

            config_store: Arc::clone(&user_config_store),
            config: config.clone(),
        };

        let app = axum::Router::new()
            .nest_service("/servers/{virtual_host_name}/mcp", mcp_service)
            .layer(middleware::from_fn_with_state(mcp_add_state.clone(), user_config_store_layer))
            .layer(middleware::from_fn_with_state(mcp_add_state.clone(), claims_layer))
            .layer(SessionIdLayer)
            .layer(middleware::from_fn(virtual_host_id_layer))
            .layer(cors_layer);

        #[cfg(feature = "with_tools")]
        let app = tools::add_tools(app);

        let app = app.with_state(mcp_add_state);
        let app = axum::Router::new().nest("/contextforge-rs", app);

        let mut handlers = vec![];

        if let Some(tcp) = Option::<Tcp>::try_from(config)? {
            handlers.push(tcp.handle_tcp(app.clone()).boxed());
        }

        if let Some(tls) = Option::<DownstreamTls>::try_from(config)? {
            handlers.push(tls.handle_tls(app.clone()).boxed());
        }

        let _ = futures::future::join_all(handlers).await;

        Ok(())
    }
}

pub async fn get_config_store(config: &Config) -> Result<RedisUserConfigStore> {
    let redis_config = RedisConfig::try_from(config)?;
    RedisUserConfigStore::new(&RedisClient::try_from(redis_config)?).await
}
