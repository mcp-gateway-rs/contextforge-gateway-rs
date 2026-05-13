use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use contextforge_gateway_rs_cpex::{GatewayPluginRuntimeError, RuntimePluginConfigStore};
use serde_json::Value;
use tokio::sync::Mutex as TokioMutex;

#[derive(Clone, Default)]
pub(crate) struct MemoryRuntimePluginConfigStore {
    config: Arc<TokioMutex<Option<Value>>>,
    calls: Arc<AtomicUsize>,
}

impl MemoryRuntimePluginConfigStore {
    pub(crate) fn with_config(config: Value) -> Self {
        Self { config: Arc::new(TokioMutex::new(Some(config))), calls: Arc::new(AtomicUsize::new(0)) }
    }

    pub(crate) async fn set_config(&self, config: Value) {
        *self.config.lock().await = Some(config);
    }

    pub(crate) async fn clear_config(&self) {
        *self.config.lock().await = None;
    }

    pub(crate) fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl RuntimePluginConfigStore for MemoryRuntimePluginConfigStore {
    async fn get_config(&self) -> Result<Option<Value>, GatewayPluginRuntimeError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.config.lock().await.clone())
    }
}
