use async_trait::async_trait;
use redis::{Client, RedisError, cmd};

use crate::error::GatewayPluginRuntimeError;

const RUNTIME_PLUGIN_CONFIG_KEY: &str = "ContextForgeGatewayRuntimePluginConfig";

#[async_trait]
pub trait RuntimePluginConfigStore: Send + Sync {
    async fn get_config(&self) -> Result<Option<serde_json::Value>, GatewayPluginRuntimeError>;
}

pub(crate) fn cpex_config_from_document(
    document: &serde_json::Value,
) -> Result<serde_json::Value, GatewayPluginRuntimeError> {
    let Some(object) = document.as_object() else {
        return Err(GatewayPluginRuntimeError::ConfigWrongFormat);
    };
    let version = object.get("version").and_then(serde_json::Value::as_u64);
    let cpex = object.get("cpex");
    match (version, cpex) {
        (Some(1), Some(cpex)) => Ok(cpex.clone()),
        _ => Err(GatewayPluginRuntimeError::ConfigWrongFormat),
    }
}

#[derive(Clone)]
pub struct RedisRuntimePluginConfigStore {
    redis_client: Client,
}

impl RedisRuntimePluginConfigStore {
    pub fn new(redis_client: Client) -> Self {
        Self { redis_client }
    }
}

#[async_trait]
impl RuntimePluginConfigStore for RedisRuntimePluginConfigStore {
    async fn get_config(&self) -> Result<Option<serde_json::Value>, GatewayPluginRuntimeError> {
        let Ok(mut connection) = self.redis_client.get_multiplexed_async_connection().await else {
            return Err(GatewayPluginRuntimeError::ConfigStoreUnavailable);
        };

        let maybe_config: Result<Option<Vec<u8>>, RedisError> =
            cmd("GET").arg(RUNTIME_PLUGIN_CONFIG_KEY).take().query_async(&mut connection).await;
        let Some(config) = maybe_config.map_err(|_| GatewayPluginRuntimeError::ConfigStoreUnavailable)? else {
            return Ok(None);
        };

        let document = decode_config_document(&config)?;
        Ok(Some(document))
    }
}

fn decode_config_document(config: &[u8]) -> Result<serde_json::Value, GatewayPluginRuntimeError> {
    if config.iter().copied().find(|byte| !byte.is_ascii_whitespace()).is_some_and(|byte| matches!(byte, b'{' | b'[')) {
        return serde_json::from_slice::<serde_json::Value>(config)
            .map_err(|_| GatewayPluginRuntimeError::ConfigWrongFormat);
    }

    rmp_serde::decode::from_slice::<serde_json::Value>(config)
        .or_else(|_| serde_json::from_slice::<serde_json::Value>(config))
        .map_err(|_| GatewayPluginRuntimeError::ConfigWrongFormat)
}
