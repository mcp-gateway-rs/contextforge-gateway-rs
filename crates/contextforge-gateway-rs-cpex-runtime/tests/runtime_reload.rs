mod support;

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use contextforge_gateway_rs_cpex_runtime::CpexRuntimeRegistry;
use contextforge_gateway_rs_lib::{GatewayToolRuntime, ToolArgumentsUpdate};
use cpex_core::hooks::types::cmf_hook_names;
use serde_json::json;

use support::{MemoryRuntimePluginConfigStore, TestPlugin, TestPluginFactory, sum_request};

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn runtime_reload_replaces_current_plugin_runtime() {
    let plugin = Arc::new(TestPlugin::new("configured-pre", vec![cmf_hook_names::TOOL_PRE_INVOKE]).with_pre_rewrite());
    let observations = plugin.observations();
    let config_store = MemoryRuntimePluginConfigStore::with_config(json!({
        "version": 1,
        "cpex": {
            "plugins": []
        }
    }));
    let mut runtime = CpexRuntimeRegistry::with_config_store(Arc::new(config_store.clone()));
    runtime
        .register_factory("test", Box::new(TestPluginFactory::from_plugin(&plugin)))
        .expect("test factory registers");

    GatewayToolRuntime::initialize(&runtime).await.expect("runtime initializes");
    let result = runtime.before_tool_call(&sum_request("sum", 1, 2), "sum").await.expect("pre hook runs");

    assert!(matches!(result.arguments, ToolArgumentsUpdate::Unchanged));
    assert_eq!(0, observations.lock().expect("observations lock poisoned").pre_calls);

    config_store
        .set_config(json!({
            "version": 1,
            "cpex": {
                "plugins": [{
                    "name": "configured-pre",
                    "kind": "test",
                    "hooks": [cmf_hook_names::TOOL_PRE_INVOKE]
                }]
            }
        }))
        .await;

    runtime.reload().await.expect("runtime reloads");
    let result = runtime.before_tool_call(&sum_request("sum", 1, 2), "sum").await.expect("pre hook runs");

    assert!(matches!(result.arguments, ToolArgumentsUpdate::Replace(Some(_))));
    assert_eq!(1, observations.lock().expect("observations lock poisoned").pre_calls);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn failed_runtime_reload_keeps_current_plugin_runtime() {
    let plugin = Arc::new(TestPlugin::new("configured-pre", vec![cmf_hook_names::TOOL_PRE_INVOKE]).with_pre_rewrite());
    let observations = plugin.observations();
    let config_store = MemoryRuntimePluginConfigStore::with_config(json!({
        "version": 1,
        "cpex": {
            "plugins": [{
                "name": "configured-pre",
                "kind": "test",
                "hooks": [cmf_hook_names::TOOL_PRE_INVOKE]
            }]
        }
    }));
    let mut runtime = CpexRuntimeRegistry::with_config_store(Arc::new(config_store.clone()));
    runtime
        .register_factory("test", Box::new(TestPluginFactory::from_plugin(&plugin)))
        .expect("test factory registers");

    GatewayToolRuntime::initialize(&runtime).await.expect("runtime initializes");
    let result = runtime.before_tool_call(&sum_request("sum", 1, 2), "sum").await.expect("pre hook runs");
    assert!(matches!(result.arguments, ToolArgumentsUpdate::Replace(Some(_))));

    config_store.set_config(json!({ "version": 2, "cpex": { "plugins": [] } })).await;
    runtime.reload().await.expect_err("invalid reload fails");

    let result = runtime.before_tool_call(&sum_request("sum", 1, 2), "sum").await.expect("pre hook still runs");
    assert!(matches!(result.arguments, ToolArgumentsUpdate::Replace(Some(_))));
    assert_eq!(2, observations.lock().expect("observations lock poisoned").pre_calls);
    assert_eq!(0, observations.lock().expect("observations lock poisoned").shutdown_calls);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn initialized_runtime_picks_up_config_store_changes() {
    let plugin = Arc::new(TestPlugin::new("configured-pre", vec![cmf_hook_names::TOOL_PRE_INVOKE]).with_pre_rewrite());
    let observations = plugin.observations();
    let config_store = MemoryRuntimePluginConfigStore::with_config(json!({
        "version": 1,
        "cpex": { "plugins": [] }
    }));
    let mut runtime = CpexRuntimeRegistry::with_config_store(Arc::new(config_store.clone()));
    runtime
        .register_factory("test", Box::new(TestPluginFactory::from_plugin(&plugin)))
        .expect("test factory registers");

    GatewayToolRuntime::initialize(&runtime).await.expect("runtime initializes");
    config_store
        .set_config(json!({
            "version": 1,
            "cpex": {
                "plugins": [{
                    "name": "configured-pre",
                    "kind": "test",
                    "hooks": [cmf_hook_names::TOOL_PRE_INVOKE]
                }]
            }
        }))
        .await;

    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let result = runtime.before_tool_call(&sum_request("sum", 1, 2), "sum").await.expect("pre hook runs");
        if matches!(result.arguments, ToolArgumentsUpdate::Replace(Some(_))) {
            break;
        }
        assert!(Instant::now() < deadline, "runtime config change was not applied");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    assert_eq!(1, observations.lock().expect("observations lock poisoned").pre_calls);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn runtime_reload_without_config_clears_current_plugin_runtime() {
    let plugin = Arc::new(TestPlugin::new("configured-pre", vec![cmf_hook_names::TOOL_PRE_INVOKE]).with_pre_rewrite());
    let observations = plugin.observations();
    let config_store = MemoryRuntimePluginConfigStore::with_config(json!({
        "version": 1,
        "cpex": {
            "plugins": [{
                "name": "configured-pre",
                "kind": "test",
                "hooks": [cmf_hook_names::TOOL_PRE_INVOKE]
            }]
        }
    }));
    let mut runtime = CpexRuntimeRegistry::with_config_store(Arc::new(config_store.clone()));
    runtime
        .register_factory("test", Box::new(TestPluginFactory::from_plugin(&plugin)))
        .expect("test factory registers");

    GatewayToolRuntime::initialize(&runtime).await.expect("runtime initializes");
    let result = runtime.before_tool_call(&sum_request("sum", 1, 2), "sum").await.expect("pre hook runs");
    assert!(matches!(result.arguments, ToolArgumentsUpdate::Replace(Some(_))));

    config_store.clear_config().await;
    runtime.reload().await.expect("runtime reloads");
    let result = runtime.before_tool_call(&sum_request("sum", 1, 2), "sum").await.expect("pre hook skips");

    assert!(matches!(result.arguments, ToolArgumentsUpdate::Unchanged));
    assert_eq!(1, observations.lock().expect("observations lock poisoned").pre_calls);
}
