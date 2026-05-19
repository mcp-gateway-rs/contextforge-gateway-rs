use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use arc_swap::ArcSwap;
use cpex_core::{
    config::CpexConfig,
    factory::{PluginFactory, PluginFactoryRegistry},
};
use rmcp::{
    ErrorData,
    model::{CallToolRequestParams, CallToolResult},
};
use tokio::task::JoinHandle;

use crate::{
    config::{RedisRuntimePluginConfigStore, RuntimePluginConfigStore, cpex_config_from_document},
    error::GatewayPluginRuntimeError,
    hooks::{RuntimeHookError, RuntimeHookState, ToolPreCallResult},
    runtime::GatewayPluginRuntime,
};

pub struct CpexRuntimeRegistry {
    runtime: Arc<ArcSwap<GatewayPluginRuntime>>,
    config_store: Option<Arc<dyn RuntimePluginConfigStore>>,
    factories: Arc<PluginFactoryRegistry>,
    watcher_started: AtomicBool,
    watcher_interval: Duration,
}

#[derive(Clone)]
pub struct GatewayPluginRuntimeHandle {
    runtime: Arc<ArcSwap<GatewayPluginRuntime>>,
}

struct RegistryToolCallState {
    runtime: Arc<GatewayPluginRuntime>,
    state: Option<RuntimeHookState>,
}

impl Default for CpexRuntimeRegistry {
    fn default() -> Self {
        Self {
            runtime: Arc::new(ArcSwap::from_pointee(GatewayPluginRuntime::default())),
            config_store: None,
            factories: Arc::new(PluginFactoryRegistry::new()),
            watcher_started: AtomicBool::new(false),
            watcher_interval: Duration::from_secs(2),
        }
    }
}

impl CpexRuntimeRegistry {
    pub fn with_redis_config(redis_client: redis::Client) -> Self {
        Self { config_store: Some(Arc::new(RedisRuntimePluginConfigStore::new(redis_client))), ..Self::default() }
    }

    pub fn register_factory(
        &mut self,
        kind: impl Into<String>,
        factory: Box<dyn PluginFactory>,
    ) -> Result<(), GatewayPluginRuntimeError> {
        let factories = Arc::get_mut(&mut self.factories).ok_or(GatewayPluginRuntimeError::FactoryRegistryShared)?;
        factories.register(kind, factory);
        Ok(())
    }

    pub async fn reload(&self) -> Result<(), GatewayPluginRuntimeError> {
        reload_runtime(&self.runtime, self.config_store.as_ref(), &self.factories).await.map(|_| ())
    }

    pub async fn apply_config(&self, config: Option<CpexConfig>) -> Result<(), GatewayPluginRuntimeError> {
        apply_runtime_config(&self.runtime, &self.factories, config).await
    }

    pub fn handle(&self) -> GatewayPluginRuntimeHandle {
        GatewayPluginRuntimeHandle { runtime: Arc::clone(&self.runtime) }
    }

    fn start_config_watcher(&self, initial_config: Option<serde_json::Value>) -> Option<JoinHandle<()>> {
        let config_store = self.config_store.clone()?;
        if self.watcher_started.swap(true, Ordering::AcqRel) {
            return None;
        }

        let runtime = Arc::downgrade(&self.runtime);
        let factories = Arc::clone(&self.factories);
        let watcher_interval = self.watcher_interval;
        Some(tokio::spawn(async move {
            let mut last_applied_config = initial_config;
            loop {
                tokio::time::sleep(watcher_interval).await;
                let Some(runtime) = runtime.upgrade() else {
                    break;
                };
                match config_store.get_config().await {
                    Ok(config) if config == last_applied_config => {},
                    Ok(config) => {
                        let result = match config_from_document(config.as_ref()) {
                            Ok(config) => apply_runtime_config(&runtime, &factories, config).await,
                            Err(error) => Err(error),
                        };
                        match result {
                            Ok(()) => last_applied_config = config,
                            Err(error) => tracing::warn!(%error, "failed to reload CPEX runtime plugin config"),
                        }
                    },
                    Err(error) => tracing::warn!(%error, "failed to load CPEX runtime plugin config"),
                }
            }
        }))
    }
}

#[cfg(test)]
#[allow(
    clippy::items_after_test_module,
    reason = "large inline test module keeps CPEX unit-only helpers out of public API"
)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use async_trait::async_trait;
    use cpex_core::{
        cmf::{CmfHook, ContentPart, MessagePayload, Role},
        context::PluginContext,
        error::{PluginError, PluginViolation},
        factory::{PluginFactory, PluginInstance},
        hooks::{Extensions, HookHandler, PluginResult, TypedHandlerAdapter, types::cmf_hook_names},
        plugin::{Plugin, PluginConfig},
        registry::AnyHookHandler,
    };
    use rmcp::model::{CallToolRequestParams, CallToolResult, Content};
    use serde_json::{Value, json};
    use tokio::sync::Mutex as TokioMutex;

    use crate::{CmfPluginFactory, ToolArgumentsUpdate};

    use super::*;

    #[derive(Clone, Default)]
    struct MemoryConfigStore {
        config: Arc<TokioMutex<Option<Value>>>,
        calls: Arc<AtomicUsize>,
    }

    impl MemoryConfigStore {
        fn with_config(config: Value) -> Self {
            Self { config: Arc::new(TokioMutex::new(Some(config))), calls: Arc::new(AtomicUsize::new(0)) }
        }

        async fn set_config(&self, config: Value) {
            *self.config.lock().await = Some(config);
        }

        async fn clear_config(&self) {
            *self.config.lock().await = None;
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl RuntimePluginConfigStore for MemoryConfigStore {
        async fn get_config(&self) -> Result<Option<Value>, GatewayPluginRuntimeError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.config.lock().await.clone())
        }
    }

    #[derive(Default)]
    struct Observations {
        pre_calls: usize,
        post_calls: usize,
        shutdown_calls: usize,
        pre_tool_call_id: Option<String>,
        post_tool_call_id: Option<String>,
    }

    #[derive(Clone, Copy, Default)]
    enum PreBehavior {
        #[default]
        Allow,
        Rewrite,
        SetContext,
    }

    #[derive(Clone, Copy, Default)]
    enum PostBehavior {
        #[default]
        Allow,
        Rewrite,
        RequireContext,
    }

    struct TestPlugin {
        config: PluginConfig,
        observations: Arc<Mutex<Observations>>,
        pre_behavior: PreBehavior,
        post_behavior: PostBehavior,
    }

    impl TestPlugin {
        fn new(name: &str, hooks: Vec<&'static str>) -> Self {
            Self {
                config: PluginConfig {
                    name: name.to_owned(),
                    kind: "test".to_owned(),
                    hooks: hooks.into_iter().map(str::to_owned).collect(),
                    ..Default::default()
                },
                observations: Arc::new(Mutex::new(Observations::default())),
                pre_behavior: PreBehavior::Allow,
                post_behavior: PostBehavior::Allow,
            }
        }

        fn rewrite_from_config(config: PluginConfig) -> Self {
            Self { config, ..Self::new("generic-pre", vec![cmf_hook_names::TOOL_PRE_INVOKE]).with_pre_rewrite() }
        }

        fn with_pre_rewrite(mut self) -> Self {
            self.pre_behavior = PreBehavior::Rewrite;
            self
        }

        fn with_post_rewrite(mut self) -> Self {
            self.post_behavior = PostBehavior::Rewrite;
            self
        }

        fn with_context_roundtrip(mut self) -> Self {
            self.pre_behavior = PreBehavior::SetContext;
            self.post_behavior = PostBehavior::RequireContext;
            self
        }

        fn observations(&self) -> Arc<Mutex<Observations>> {
            Arc::clone(&self.observations)
        }
    }

    #[async_trait]
    impl Plugin for TestPlugin {
        fn config(&self) -> &PluginConfig {
            &self.config
        }

        async fn shutdown(&self) -> Result<(), Box<PluginError>> {
            self.observations.lock().expect("observations lock poisoned").shutdown_calls += 1;
            Ok(())
        }
    }

    impl HookHandler<CmfHook> for TestPlugin {
        fn handle(
            &self,
            payload: &MessagePayload,
            _extensions: &Extensions,
            ctx: &mut PluginContext,
        ) -> PluginResult<MessagePayload> {
            let is_post = payload.message.role == Role::Tool;
            let mut observations = self.observations.lock().expect("observations lock poisoned");
            if is_post {
                observations.post_calls += 1;
                observations.post_tool_call_id =
                    payload.message.get_tool_results().first().map(|result| result.tool_call_id.clone());
            } else {
                observations.pre_calls += 1;
                observations.pre_tool_call_id =
                    payload.message.get_tool_calls().first().map(|call| call.tool_call_id.clone());
            }
            drop(observations);

            if is_post {
                match self.post_behavior {
                    PostBehavior::Allow => PluginResult::allow(),
                    PostBehavior::Rewrite => PluginResult::modify_payload(payload.clone()),
                    PostBehavior::RequireContext => {
                        if ctx.get_global("pre_seen") == Some(&json!(true)) {
                            PluginResult::allow()
                        } else {
                            PluginResult::deny(
                                PluginViolation::new("missing_context", "pre context missing")
                                    .with_proto_error_code(-32003),
                            )
                        }
                    },
                }
            } else {
                match self.pre_behavior {
                    PreBehavior::Allow => PluginResult::allow(),
                    PreBehavior::Rewrite => {
                        let mut modified = payload.clone();
                        if let Some(ContentPart::ToolCall { content }) = modified
                            .message
                            .content
                            .iter_mut()
                            .find(|part| matches!(part, ContentPart::ToolCall { .. }))
                        {
                            content.arguments =
                                HashMap::from([("a".to_owned(), json!(10)), ("b".to_owned(), json!(20))]);
                        }
                        PluginResult::modify_payload(modified)
                    },
                    PreBehavior::SetContext => {
                        ctx.set_global("pre_seen", json!(true));
                        PluginResult::allow()
                    },
                }
            }
        }
    }

    struct TestPluginFactory {
        observations: Arc<Mutex<Observations>>,
        pre_behavior: PreBehavior,
        post_behavior: PostBehavior,
    }

    impl TestPluginFactory {
        fn from_plugin(plugin: &TestPlugin) -> Self {
            Self {
                observations: Arc::clone(&plugin.observations),
                pre_behavior: plugin.pre_behavior,
                post_behavior: plugin.post_behavior,
            }
        }
    }

    impl PluginFactory for TestPluginFactory {
        fn create(&self, config: &PluginConfig) -> Result<PluginInstance, Box<PluginError>> {
            let plugin = Arc::new(TestPlugin {
                config: config.clone(),
                observations: Arc::clone(&self.observations),
                pre_behavior: self.pre_behavior,
                post_behavior: self.post_behavior,
            });
            let handlers = config
                .hooks
                .iter()
                .filter_map(|hook| {
                    let hook = match hook.as_str() {
                        cmf_hook_names::TOOL_PRE_INVOKE => cmf_hook_names::TOOL_PRE_INVOKE,
                        cmf_hook_names::TOOL_POST_INVOKE => cmf_hook_names::TOOL_POST_INVOKE,
                        _ => return None,
                    };
                    Some((
                        hook,
                        Arc::new(TypedHandlerAdapter::<CmfHook, _>::new(Arc::clone(&plugin)))
                            as Arc<dyn AnyHookHandler>,
                    ))
                })
                .collect();
            let plugin: Arc<dyn Plugin> = plugin;
            Ok(PluginInstance { plugin, handlers })
        }
    }

    fn sum_request(a: i64, b: i64) -> CallToolRequestParams {
        CallToolRequestParams::new("sum")
            .with_arguments(serde_json::Map::from_iter([("a".to_owned(), json!(a)), ("b".to_owned(), json!(b))]))
    }

    fn plugin_config(plugins: &[Arc<TestPlugin>]) -> Value {
        json!({
            "version": 1,
            "cpex": {
                "plugins": plugins.iter().map(|plugin| {
                    json!({
                        "name": plugin.config.name.clone(),
                        "kind": plugin.config.kind.clone(),
                        "hooks": plugin.config.hooks.clone(),
                    })
                }).collect::<Vec<_>>()
            }
        })
    }

    async fn runtime_with_plugin(plugin: &Arc<TestPlugin>, config: Value) -> CpexRuntimeRegistry {
        let mut runtime = CpexRuntimeRegistry::with_config_store(Arc::new(MemoryConfigStore::with_config(config)));
        runtime
            .register_factory("test", Box::new(TestPluginFactory::from_plugin(plugin)))
            .expect("test factory registers");
        runtime.initialize().await.expect("runtime initializes");
        runtime
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn runtime_config_store_is_loaded_on_initialize() {
        let config_store = MemoryConfigStore::with_config(json!({ "version": 1, "cpex": { "plugins": [] } }));
        let runtime = CpexRuntimeRegistry::with_config_store(Arc::new(config_store.clone()));

        let handle = runtime.initialize().await.expect("runtime initializes");

        assert!(handle.is_some());
        assert!(config_store.calls() >= 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn invalid_runtime_plugin_config_documents_are_rejected() {
        for config in [json!({ "version": 2, "cpex": { "plugins": [] } }), json!({ "plugins": [] })] {
            let runtime = CpexRuntimeRegistry::with_config_store(Arc::new(MemoryConfigStore::with_config(config)));
            let error = runtime.initialize().await.expect_err("invalid config is rejected");

            assert_eq!("runtime plugin config is in wrong format", error.to_string());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn unsupported_runtime_plugin_config_is_rejected() {
        for cpex in [
            json!({ "plugin_settings": { "routing_enabled": true }, "plugins": [] }),
            json!({
                "plugins": [{
                    "name": "scoped",
                    "kind": "test",
                    "hooks": [cmf_hook_names::TOOL_PRE_INVOKE],
                    "conditions": [{ "tools": ["sum"] }]
                }]
            }),
            json!({ "plugins": [{ "name": "llm", "kind": "test", "hooks": [cmf_hook_names::LLM_INPUT] }] }),
        ] {
            let runtime = CpexRuntimeRegistry::with_config_store(Arc::new(MemoryConfigStore::with_config(json!({
                "version": 1,
                "cpex": cpex
            }))));
            let error = runtime.initialize().await.expect_err("unsupported config is rejected");

            assert_eq!("runtime plugin config is unsupported", error.to_string());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn runtime_config_loads_registered_factory_plugin() {
        let plugin =
            Arc::new(TestPlugin::new("configured-pre", vec![cmf_hook_names::TOOL_PRE_INVOKE]).with_pre_rewrite());
        let observations = plugin.observations();
        let runtime = runtime_with_plugin(&plugin, plugin_config(&[Arc::clone(&plugin)])).await;

        let result = runtime.before_tool_call(&sum_request(1, 2), "sum", "backend").await.expect("pre hook runs");

        assert!(matches!(result.arguments, ToolArgumentsUpdate::Replace(Some(_))));
        assert_eq!(1, observations.lock().expect("observations lock poisoned").pre_calls);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn runtime_config_loads_generic_cmf_factory_plugin() {
        let mut runtime = CpexRuntimeRegistry::with_config_store(Arc::new(MemoryConfigStore::with_config(json!({
            "version": 1,
            "cpex": {
                "plugins": [{
                    "name": "generic-pre",
                    "kind": "generic",
                    "hooks": [cmf_hook_names::TOOL_PRE_INVOKE]
                }]
            }
        }))));
        runtime
            .register_factory("generic", Box::new(CmfPluginFactory::new(TestPlugin::rewrite_from_config)))
            .expect("test factory registers");
        runtime.initialize().await.expect("runtime initializes");

        let result = runtime.before_tool_call(&sum_request(1, 2), "sum", "backend").await.expect("pre hook runs");

        assert!(matches!(result.arguments, ToolArgumentsUpdate::Replace(Some(_))));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn runtime_reload_replaces_and_clears_current_runtime() {
        let plugin =
            Arc::new(TestPlugin::new("configured-pre", vec![cmf_hook_names::TOOL_PRE_INVOKE]).with_pre_rewrite());
        let observations = plugin.observations();
        let config_store = MemoryConfigStore::with_config(json!({ "version": 1, "cpex": { "plugins": [] } }));
        let mut runtime = CpexRuntimeRegistry::with_config_store(Arc::new(config_store.clone()));
        runtime
            .register_factory("test", Box::new(TestPluginFactory::from_plugin(&plugin)))
            .expect("test factory registers");
        runtime.initialize().await.expect("runtime initializes");

        let result = runtime.before_tool_call(&sum_request(1, 2), "sum", "backend").await.expect("pre hook skips");
        assert!(matches!(result.arguments, ToolArgumentsUpdate::Unchanged));

        config_store.set_config(plugin_config(&[Arc::clone(&plugin)])).await;
        runtime.reload().await.expect("runtime reloads");
        let result = runtime.before_tool_call(&sum_request(1, 2), "sum", "backend").await.expect("pre hook runs");
        assert!(matches!(result.arguments, ToolArgumentsUpdate::Replace(Some(_))));

        config_store.clear_config().await;
        runtime.reload().await.expect("runtime reloads");
        let result = runtime.before_tool_call(&sum_request(1, 2), "sum", "backend").await.expect("pre hook skips");
        assert!(matches!(result.arguments, ToolArgumentsUpdate::Unchanged));
        assert_eq!(1, observations.lock().expect("observations lock poisoned").pre_calls);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn failed_runtime_reload_keeps_current_runtime() {
        let plugin =
            Arc::new(TestPlugin::new("configured-pre", vec![cmf_hook_names::TOOL_PRE_INVOKE]).with_pre_rewrite());
        let observations = plugin.observations();
        let config_store = MemoryConfigStore::with_config(plugin_config(&[Arc::clone(&plugin)]));
        let mut runtime = CpexRuntimeRegistry::with_config_store(Arc::new(config_store.clone()));
        runtime
            .register_factory("test", Box::new(TestPluginFactory::from_plugin(&plugin)))
            .expect("test factory registers");
        runtime.initialize().await.expect("runtime initializes");

        config_store.set_config(json!({ "version": 2, "cpex": { "plugins": [] } })).await;
        runtime.reload().await.expect_err("invalid reload fails");

        let result = runtime.before_tool_call(&sum_request(1, 2), "sum", "backend").await.expect("pre hook runs");
        assert!(matches!(result.arguments, ToolArgumentsUpdate::Replace(Some(_))));
        assert_eq!(1, observations.lock().expect("observations lock poisoned").pre_calls);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn combined_plugin_preserves_context_from_pre_to_post_across_replacement() {
        let plugin = Arc::new(
            TestPlugin::new("context", vec![cmf_hook_names::TOOL_PRE_INVOKE, cmf_hook_names::TOOL_POST_INVOKE])
                .with_context_roundtrip(),
        );
        let observations = plugin.observations();
        let config_store = MemoryConfigStore::with_config(plugin_config(&[Arc::clone(&plugin)]));
        let mut runtime = CpexRuntimeRegistry::with_config_store(Arc::new(config_store.clone()));
        runtime
            .register_factory("test", Box::new(TestPluginFactory::from_plugin(&plugin)))
            .expect("test factory registers");
        runtime.initialize().await.expect("runtime initializes");

        let pre = runtime.before_tool_call(&sum_request(1, 2), "sum", "backend").await.expect("pre hook runs");
        config_store.clear_config().await;
        runtime.reload().await.expect("runtime reloads");
        let response = CallToolResult::success(vec![Content::text("3")]);
        runtime.after_tool_call("sum", response, pre.state).await.expect("post hook runs");

        let observations = observations.lock().expect("observations lock poisoned");
        assert_eq!(1, observations.post_calls);
        assert_eq!(observations.pre_tool_call_id, observations.post_tool_call_id);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn post_only_runtime_does_not_apply_new_post_hook_to_in_flight_call() {
        let plugin = Arc::new(TestPlugin::new("post", vec![cmf_hook_names::TOOL_POST_INVOKE]).with_post_rewrite());
        let observations = plugin.observations();
        let config_store = MemoryConfigStore::with_config(json!({ "version": 1, "cpex": { "plugins": [] } }));
        let mut runtime = CpexRuntimeRegistry::with_config_store(Arc::new(config_store.clone()));
        runtime
            .register_factory("test", Box::new(TestPluginFactory::from_plugin(&plugin)))
            .expect("test factory registers");
        runtime.initialize().await.expect("runtime initializes");

        let pre = runtime.before_tool_call(&sum_request(1, 2), "sum", "backend").await.expect("pre hook skips");
        config_store.set_config(plugin_config(&[Arc::clone(&plugin)])).await;
        runtime.reload().await.expect("runtime reloads");
        let response = CallToolResult::success(vec![Content::text("3")]);
        runtime.after_tool_call("sum", response, pre.state).await.expect("post hook skips");

        assert_eq!(0, observations.lock().expect("observations lock poisoned").post_calls);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn replaced_runtime_shutdowns_on_drop() {
        let plugin = Arc::new(TestPlugin::new("pre", vec![cmf_hook_names::TOOL_PRE_INVOKE]).with_pre_rewrite());
        let observations = plugin.observations();
        let config_store = MemoryConfigStore::with_config(plugin_config(&[Arc::clone(&plugin)]));
        let mut runtime = CpexRuntimeRegistry::with_config_store(Arc::new(config_store.clone()));
        runtime
            .register_factory("test", Box::new(TestPluginFactory::from_plugin(&plugin)))
            .expect("test factory registers");
        runtime.initialize().await.expect("runtime initializes");

        config_store.clear_config().await;
        runtime.reload().await.expect("runtime reloads");

        for _ in 0..20 {
            if observations.lock().expect("observations lock poisoned").shutdown_calls > 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("replaced runtime did not shut down");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn watcher_applies_config_changes() {
        let plugin =
            Arc::new(TestPlugin::new("configured-pre", vec![cmf_hook_names::TOOL_PRE_INVOKE]).with_pre_rewrite());
        let observations = plugin.observations();
        let config_store = MemoryConfigStore::with_config(json!({ "version": 1, "cpex": { "plugins": [] } }));
        let mut runtime =
            CpexRuntimeRegistry::with_config_store_interval(Arc::new(config_store.clone()), Duration::from_millis(10));
        runtime
            .register_factory("test", Box::new(TestPluginFactory::from_plugin(&plugin)))
            .expect("test factory registers");
        let handle = runtime.initialize().await.expect("runtime initializes");
        assert!(handle.is_some());

        config_store.set_config(plugin_config(&[Arc::clone(&plugin)])).await;
        for _ in 0..20 {
            let result = runtime.before_tool_call(&sum_request(1, 2), "sum", "backend").await.expect("pre hook runs");
            if matches!(result.arguments, ToolArgumentsUpdate::Replace(Some(_))) {
                assert_eq!(1, observations.lock().expect("observations lock poisoned").pre_calls);
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("config watcher did not apply plugin config");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn initialize_without_config_store_returns_no_watcher() {
        let runtime = CpexRuntimeRegistry::default();

        let handle = runtime.initialize().await.expect("runtime initializes");

        assert!(handle.is_none());
    }
}

async fn reload_runtime(
    runtime: &ArcSwap<GatewayPluginRuntime>,
    config_store: Option<&Arc<dyn RuntimePluginConfigStore>>,
    factories: &PluginFactoryRegistry,
) -> Result<Option<serde_json::Value>, GatewayPluginRuntimeError> {
    let Some(config_store) = config_store else {
        return Ok(None);
    };
    let config = config_store.get_config().await?;
    apply_runtime_config(runtime, factories, config_from_document(config.as_ref())?).await?;
    Ok(config)
}

fn config_from_document(config: Option<&serde_json::Value>) -> Result<Option<CpexConfig>, GatewayPluginRuntimeError> {
    let Some(config) = config else {
        return Ok(None);
    };
    serde_json::from_value(cpex_config_from_document(config)?)
        .map(Some)
        .map_err(|_| GatewayPluginRuntimeError::ConfigWrongFormat)
}

#[cfg(test)]
impl CpexRuntimeRegistry {
    fn with_config_store(config_store: Arc<dyn RuntimePluginConfigStore>) -> Self {
        Self { config_store: Some(config_store), ..Self::default() }
    }

    fn with_config_store_interval(config_store: Arc<dyn RuntimePluginConfigStore>, watcher_interval: Duration) -> Self {
        Self { config_store: Some(config_store), watcher_interval, ..Self::default() }
    }

    async fn before_tool_call(
        &self,
        request: &CallToolRequestParams,
        tool_name: &str,
        backend_name: &str,
    ) -> Result<ToolPreCallResult, ErrorData> {
        self.handle().before_tool_call(request, tool_name, backend_name).await
    }

    async fn after_tool_call(
        &self,
        tool_name: &str,
        response: CallToolResult,
        state: Option<RuntimeHookState>,
    ) -> Result<CallToolResult, ErrorData> {
        self.handle().after_tool_call(tool_name, response, state).await
    }
}

async fn apply_runtime_config(
    runtime: &ArcSwap<GatewayPluginRuntime>,
    factories: &PluginFactoryRegistry,
    config: Option<CpexConfig>,
) -> Result<(), GatewayPluginRuntimeError> {
    let Some(config) = config else {
        drop(runtime.swap(Arc::new(GatewayPluginRuntime::default())));
        return Ok(());
    };
    drop(runtime.swap(Arc::new(GatewayPluginRuntime::from_config(config, factories).await?)));
    Ok(())
}

impl CpexRuntimeRegistry {
    pub async fn initialize(&self) -> Result<Option<JoinHandle<()>>, RuntimeHookError> {
        let initial_config = reload_runtime(&self.runtime, self.config_store.as_ref(), &self.factories).await?;
        Ok(self.start_config_watcher(initial_config))
    }
}

impl GatewayPluginRuntimeHandle {
    fn current(&self) -> Arc<GatewayPluginRuntime> {
        self.runtime.load_full()
    }

    pub async fn before_tool_call(
        &self,
        request: &CallToolRequestParams,
        tool_name: &str,
        backend_name: &str,
    ) -> Result<ToolPreCallResult, ErrorData> {
        let runtime = self.current();
        let mut result = runtime.before_tool_call(request, tool_name, backend_name).await?;
        if runtime.has_post_hook() {
            let state = result.state.take();
            result.state = Some(Box::new(RegistryToolCallState { runtime, state }));
        } else {
            result.state = None;
        }
        Ok(result)
    }

    pub async fn after_tool_call(
        &self,
        tool_name: &str,
        response: CallToolResult,
        state: Option<RuntimeHookState>,
    ) -> Result<CallToolResult, ErrorData> {
        match state.and_then(|state| state.downcast::<RegistryToolCallState>().ok()) {
            Some(state) => state.runtime.after_tool_call(tool_name, response, state.state).await,
            None => Ok(response),
        }
    }
}
