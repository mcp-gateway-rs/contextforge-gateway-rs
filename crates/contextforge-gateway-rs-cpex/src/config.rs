use async_trait::async_trait;
use redis::{
    Client, RedisError,
    aio::{ConnectionManager, ConnectionManagerConfig},
    cmd,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::error::GatewayPluginRuntimeError;
use cpex_core::config::CpexConfig;

const RUNTIME_PLUGIN_CONFIG_KEY: &str = "ContextForgeGatewayRuntimePluginConfig";

#[async_trait]
pub(crate) trait RuntimePluginConfigStore: Send + Sync {
    async fn get_config(&self) -> Result<Option<LoadedRuntimePluginConfig>, GatewayPluginRuntimeError>;
}

#[derive(Clone, Debug)]
pub(crate) struct LoadedRuntimePluginConfig {
    pub(crate) document: RuntimePluginConfigDocument,
    pub(crate) fingerprint: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct RuntimePluginConfigDocument {
    pub(crate) version: u8,
    pub(crate) cpex: CpexConfig,
}

impl RuntimePluginConfigDocument {
    pub(crate) fn cpex_config(&self) -> Result<CpexConfig, GatewayPluginRuntimeError> {
        if self.version != 1 {
            return Err(GatewayPluginRuntimeError::ConfigWrongFormat);
        }
        Ok(self.cpex.clone())
    }
}

pub(crate) struct RedisRuntimePluginConfigStore {
    redis_client: Client,
    connection: Mutex<Option<ConnectionManager>>,
}

impl RedisRuntimePluginConfigStore {
    pub(crate) fn new(redis_client: Client) -> Self {
        Self { redis_client, connection: Mutex::new(None) }
    }

    async fn connection(&self) -> Result<ConnectionManager, GatewayPluginRuntimeError> {
        let mut connection = self.connection.lock().await;
        if connection.is_none() {
            *connection = Some(
                self.redis_client
                    .get_connection_manager_with_config(ConnectionManagerConfig::default())
                    .await
                    .map_err(|_| GatewayPluginRuntimeError::ConfigStoreUnavailable)?,
            );
        }
        connection.clone().ok_or(GatewayPluginRuntimeError::ConfigStoreUnavailable)
    }
}

#[async_trait]
impl RuntimePluginConfigStore for RedisRuntimePluginConfigStore {
    async fn get_config(&self) -> Result<Option<LoadedRuntimePluginConfig>, GatewayPluginRuntimeError> {
        let mut connection = self.connection().await?;

        let maybe_config: Result<Option<Vec<u8>>, RedisError> =
            cmd("GET").arg(RUNTIME_PLUGIN_CONFIG_KEY).take().query_async(&mut connection).await;
        let Some(config) = maybe_config.map_err(|_| GatewayPluginRuntimeError::ConfigStoreUnavailable)? else {
            return Ok(None);
        };

        let document = decode_config_document(&config)?;
        Ok(Some(LoadedRuntimePluginConfig { document, fingerprint: config }))
    }
}

pub(crate) fn decode_config_document(config: &[u8]) -> Result<RuntimePluginConfigDocument, GatewayPluginRuntimeError> {
    if config.iter().copied().find(|byte| !byte.is_ascii_whitespace()).is_some_and(|byte| matches!(byte, b'{' | b'[')) {
        return serde_json::from_slice::<RuntimePluginConfigDocument>(config)
            .map_err(|_| GatewayPluginRuntimeError::ConfigWrongFormat);
    }

    rmp_serde::decode::from_slice::<RuntimePluginConfigDocument>(config)
        .or_else(|_| serde_json::from_slice::<RuntimePluginConfigDocument>(config))
        .map_err(|_| GatewayPluginRuntimeError::ConfigWrongFormat)
}

#[cfg(test)]
mod tests {
    use cpex_core::config::CpexConfig;

    use super::{RuntimePluginConfigDocument, decode_config_document};

    #[test]
    fn decode_config_document_accepts_json_bytes() {
        let document = br#" { "version": 1, "cpex": { "plugins": [] } }"#;

        let document = decode_config_document(document).expect("JSON document decodes");

        assert!(document.cpex_config().expect("config version is valid").plugins.is_empty());
    }

    #[test]
    fn decode_config_document_accepts_messagepack_bytes() {
        let expected = RuntimePluginConfigDocument { version: 1, cpex: CpexConfig::default() };
        let document = rmp_serde::to_vec_named(&expected).expect("MessagePack document encodes");

        assert!(decode_config_document(&document).expect("MessagePack document decodes").cpex_config().is_ok());
    }

    #[test]
    fn decode_config_document_rejects_missing_cpex_config() {
        let error = decode_config_document(br#"{ "version": 1 }"#).expect_err("missing CPEX config is rejected");

        assert_eq!("runtime plugin config is in wrong format", error.to_string());
    }

    #[test]
    fn decode_config_document_rejects_invalid_json_bytes() {
        let error = decode_config_document(b"{not-json").expect_err("invalid JSON bytes are rejected");

        assert_eq!("runtime plugin config is in wrong format", error.to_string());
    }
}
