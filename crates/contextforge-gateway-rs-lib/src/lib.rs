use std::{fs, sync::Arc};

use axum::middleware;
use axum_jwt_auth::LocalDecoder;
use futures::FutureExt;
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use rmcp::transport::{
    StreamableHttpServerConfig,
    streamable_http_server::{session::local::LocalSessionManager, tower::StreamableHttpService},
};
mod common;
mod const_values;
mod gateway;
mod layers;
mod transports;

#[cfg(test)]
mod tests;

#[cfg(feature = "with_tools")]
mod tools;

mod user_config_store;
pub use common::{RedisClient, RedisConfig};
use gateway::McpService;
use layers::session_id::SessionId;
use tower_http::cors::{Any, CorsLayer};
use transports::{DownstreamTls, Tcp};
use typed_builder::TypedBuilder;
pub use user_config_store::RedisUserConfigStore;

pub use crate::common::Config;

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type Result<T> = std::result::Result<T, Error>;

use crate::{
    common::ContextForgeGatewayAppState,
    const_values::CONEXT_FORGE_GATEWAY_AUDIENCE,
    gateway::LocalUserSessionStore,
    layers::{
        claims_id::claims_layer, session_id::SessionIdLayer, user_config_store::user_config_store_layer,
        virtual_host_id::virtual_host_id_layer,
    },
    user_config_store::UserConfigStore,
};

#[derive(Clone, TypedBuilder)]
#[builder(field_defaults(setter(prefix = "with_")))]
pub struct Gateway {
    config: Config,
    session_manager: Arc<LocalSessionManager>,
    user_config_store: Arc<dyn UserConfigStore + std::marker::Send + Sync>,
}

impl Gateway {
    pub async fn run_gateway(self) -> Result<()> {
        let config = &self.config;
        let session_manager = self.session_manager;
        let user_config_store = self.user_config_store;

        let user_session_store = LocalUserSessionStore::new();

        let streamable_config = StreamableHttpServerConfig::default().disable_allowed_hosts();

        let reqwest_backend_client = reqwest::Client::try_from(config)?;

        // Create streamable HTTP service
        let mcp_service: StreamableHttpService<McpService<LocalUserSessionStore>, LocalSessionManager> =
            StreamableHttpService::new(
                move || {
                    Ok(McpService::builder()
                        .with_user_session_store(user_session_store.clone())
                        .with_http_client(reqwest_backend_client.clone())
                        .build())
                },
                session_manager,
                streamable_config,
            );

        let cors_layer = CorsLayer::new().allow_origin(Any).allow_methods(Any).allow_headers(Any).expose_headers(Any);

        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_audience(&[CONEXT_FORGE_GATEWAY_AUDIENCE]);

        let local_docoder = LocalDecoder::builder()
            .keys(vec![
                DecodingKey::from_rsa_pem(&fs::read(&config.token_verification_public_key).map_err(|e| {
                    format!(
                        "Error when creating local decoder {e:?} {}",
                        config.token_verification_public_key.display()
                    )
                })?)
                .map_err(|e| {
                    format!(
                        "Error when creating local decoder {e:?} {}",
                        config.token_verification_public_key.display()
                    )
                })?,
            ])
            .validation(validation)
            .build()
            .map_err(|e| format!("Error when creating local decoder {e:?}"))?;
        let mcp_add_state: ContextForgeGatewayAppState = ContextForgeGatewayAppState {
            jwt_token_decoder: Arc::new(local_docoder),
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
