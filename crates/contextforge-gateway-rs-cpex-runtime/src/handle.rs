use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use contextforge_gateway_rs_lib::{GatewayToolRuntime, RuntimeHookState, ToolPreCallResult};
use cpex_core::factory::{PluginFactory, PluginFactoryRegistry};
use rmcp::{
    ErrorData,
    model::{CallToolRequestParams, CallToolResult},
};

use crate::{
    config::{RedisRuntimePluginConfigStore, RuntimePluginConfigStore, cpex_config_from_document},
    error::GatewayPluginRuntimeError,
    factory::{PAYLOAD_MARKER_KIND, PayloadMarkerFactory},
    runtime::GatewayPluginRuntime,
};

pub struct CpexRuntimeRegistry {
    runtime: Arc<ArcSwap<GatewayPluginRuntime>>,
    config_store: Option<Arc<dyn RuntimePluginConfigStore>>,
    factories: Arc<PluginFactoryRegistry>,
    watcher_started: AtomicBool,
}

struct RegistryToolCallState {
    runtime: Arc<GatewayPluginRuntime>,
    state: Option<RuntimeHookState>,
}

impl Default for CpexRuntimeRegistry {
    fn default() -> Self {
        let mut factories = PluginFactoryRegistry::new();
        factories.register(PAYLOAD_MARKER_KIND, Box::new(PayloadMarkerFactory));
        Self {
            runtime: Arc::new(ArcSwap::from_pointee(GatewayPluginRuntime::default())),
            config_store: None,
            factories: Arc::new(factories),
            watcher_started: AtomicBool::new(false),
        }
    }
}

impl CpexRuntimeRegistry {
    pub fn with_redis_config(redis_client: redis::Client) -> Self {
        Self::with_config_store(Arc::new(RedisRuntimePluginConfigStore::new(redis_client)))
    }

    pub fn with_config_store(config_store: Arc<dyn RuntimePluginConfigStore>) -> Self {
        Self { config_store: Some(config_store), ..Self::default() }
    }

    pub fn register_factory(&mut self, kind: impl Into<String>, factory: Box<dyn PluginFactory>) {
        Arc::get_mut(&mut self.factories)
            .expect("runtime plugin factories must be registered before the registry is shared")
            .register(kind, factory);
    }

    pub async fn reload(&self) -> Result<(), GatewayPluginRuntimeError> {
        reload_runtime(&self.runtime, self.config_store.as_ref(), &self.factories).await
    }

    fn current(&self) -> Arc<GatewayPluginRuntime> {
        self.runtime.load_full()
    }

    fn start_config_watcher(&self) {
        let Some(config_store) = self.config_store.clone() else {
            return;
        };
        if self.watcher_started.swap(true, Ordering::AcqRel) {
            return;
        }

        let runtime = Arc::clone(&self.runtime);
        let factories = Arc::clone(&self.factories);
        tokio::spawn(async move {
            let mut last_config = None;
            loop {
                tokio::time::sleep(Duration::from_secs(2)).await;
                match config_store.get_config().await {
                    Ok(config) if config == last_config => {},
                    Ok(config) => {
                        last_config = config.clone();
                        if let Err(error) = apply_runtime_config(&runtime, &factories, config).await {
                            tracing::warn!(%error, "failed to reload CPEX runtime plugin config");
                        }
                    },
                    Err(error) => tracing::warn!(%error, "failed to load CPEX runtime plugin config"),
                }
            }
        });
    }
}

async fn reload_runtime(
    runtime: &ArcSwap<GatewayPluginRuntime>,
    config_store: Option<&Arc<dyn RuntimePluginConfigStore>>,
    factories: &PluginFactoryRegistry,
) -> Result<(), GatewayPluginRuntimeError> {
    let Some(config_store) = config_store else {
        return Ok(());
    };
    apply_runtime_config(runtime, factories, config_store.get_config().await?).await
}

async fn apply_runtime_config(
    runtime: &ArcSwap<GatewayPluginRuntime>,
    factories: &PluginFactoryRegistry,
    config: Option<serde_json::Value>,
) -> Result<(), GatewayPluginRuntimeError> {
    let Some(config) = config else {
        let old = runtime.swap(Arc::new(GatewayPluginRuntime::default()));
        retire_runtime(old).await;
        return Ok(());
    };
    let config = serde_json::from_value(cpex_config_from_document(&config)?)
        .map_err(|_| GatewayPluginRuntimeError::ConfigWrongFormat)?;
    let old = runtime.swap(Arc::new(GatewayPluginRuntime::from_config(config, factories).await?));
    retire_runtime(old).await;
    Ok(())
}

async fn retire_runtime(runtime: Arc<GatewayPluginRuntime>) {
    if Arc::strong_count(&runtime) == 1 {
        runtime.shutdown().await;
        return;
    }

    tokio::spawn(async move {
        while Arc::strong_count(&runtime) > 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        runtime.shutdown().await;
    });
}

#[async_trait]
impl GatewayToolRuntime for CpexRuntimeRegistry {
    async fn initialize(&self) -> Result<(), contextforge_gateway_rs_lib::RuntimeHookError> {
        self.reload().await?;
        self.start_config_watcher();
        Ok(())
    }

    async fn before_tool_call(
        &self,
        request: &CallToolRequestParams,
        tool_name: &str,
    ) -> Result<ToolPreCallResult, ErrorData> {
        let runtime = self.current();
        let mut result = runtime.before_tool_call(request, tool_name).await?;
        result.state = Some(Box::new(RegistryToolCallState { runtime, state: result.state }));
        Ok(result)
    }

    async fn after_tool_call(
        &self,
        tool_name: &str,
        response: CallToolResult,
        state: Option<RuntimeHookState>,
    ) -> Result<CallToolResult, ErrorData> {
        match state.and_then(|state| state.downcast::<RegistryToolCallState>().ok()) {
            Some(state) => state.runtime.after_tool_call(tool_name, response, state.state).await,
            None => self.current().after_tool_call(tool_name, response, None).await,
        }
    }
}
