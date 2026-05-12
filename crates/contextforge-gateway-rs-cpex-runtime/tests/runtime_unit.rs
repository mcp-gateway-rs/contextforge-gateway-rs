mod support;

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use contextforge_gateway_rs_cpex_runtime::CpexRuntimeRegistry;
use contextforge_gateway_rs_lib::{GatewayToolRuntime, ToolArgumentsUpdate};
use cpex_core::hooks::types::cmf_hook_names;
use serde_json::json;

use support::{MemoryRuntimePluginConfigStore, TestPlugin, TestPluginFactory, plugin_config, sum_request, text};

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn runtime_config_store_is_loaded_on_initialize() {
    let config_store = MemoryRuntimePluginConfigStore::with_config(json!({
        "version": 1,
        "cpex": { "plugins": [] }
    }));
    let runtime = CpexRuntimeRegistry::with_config_store(Arc::new(config_store.clone()));

    GatewayToolRuntime::initialize(&runtime).await.expect("runtime initializes");

    assert!(config_store.calls() >= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn invalid_runtime_plugin_config_document_is_rejected() {
    let config_store = MemoryRuntimePluginConfigStore::with_config(json!({
        "version": 2,
        "cpex": { "plugins": [] }
    }));
    let runtime = CpexRuntimeRegistry::with_config_store(Arc::new(config_store));
    let error = GatewayToolRuntime::initialize(&runtime).await.expect_err("invalid config is rejected");

    assert_eq!("runtime plugin config is in wrong format", error.to_string());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn raw_runtime_plugin_config_document_is_rejected() {
    let config_store = MemoryRuntimePluginConfigStore::with_config(json!({ "plugins": [] }));
    let runtime = CpexRuntimeRegistry::with_config_store(Arc::new(config_store));
    let error = GatewayToolRuntime::initialize(&runtime).await.expect_err("raw config is rejected");

    assert_eq!("runtime plugin config is in wrong format", error.to_string());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn routed_runtime_plugin_config_is_rejected() {
    let config_store = MemoryRuntimePluginConfigStore::with_config(json!({
        "version": 1,
        "cpex": {
            "plugin_settings": { "routing_enabled": true },
            "plugins": [],
            "routes": [{ "tool": "sum", "plugins": ["configured-pre"] }]
        }
    }));
    let runtime = CpexRuntimeRegistry::with_config_store(Arc::new(config_store));
    let error = GatewayToolRuntime::initialize(&runtime).await.expect_err("routed config is rejected");

    assert_eq!("runtime plugin config is unsupported", error.to_string());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn runtime_config_loads_registered_factory_plugin() {
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
    let mut runtime = CpexRuntimeRegistry::with_config_store(Arc::new(config_store));
    runtime.register_factory("test", Box::new(TestPluginFactory::from_plugin(&plugin)));

    GatewayToolRuntime::initialize(&runtime).await.expect("runtime initializes");
    let result = runtime.before_tool_call(&sum_request("sum", 1, 2), "sum").await.expect("pre hook runs");

    match result.arguments {
        ToolArgumentsUpdate::Replace(Some(args)) => {
            assert_eq!(Some(&json!(10)), args.get("a"));
            assert_eq!(Some(&json!(20)), args.get("b"));
        },
        other => panic!("expected rewritten args, got {other:?}"),
    }
    assert_eq!(1, observations.lock().expect("observations lock poisoned").pre_calls);
}

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
    runtime.register_factory("test", Box::new(TestPluginFactory::from_plugin(&plugin)));

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
async fn initialized_runtime_picks_up_config_store_changes() {
    let plugin = Arc::new(TestPlugin::new("configured-pre", vec![cmf_hook_names::TOOL_PRE_INVOKE]).with_pre_rewrite());
    let observations = plugin.observations();
    let config_store = MemoryRuntimePluginConfigStore::with_config(json!({
        "version": 1,
        "cpex": { "plugins": [] }
    }));
    let mut runtime = CpexRuntimeRegistry::with_config_store(Arc::new(config_store.clone()));
    runtime.register_factory("test", Box::new(TestPluginFactory::from_plugin(&plugin)));

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
    runtime.register_factory("test", Box::new(TestPluginFactory::from_plugin(&plugin)));

    GatewayToolRuntime::initialize(&runtime).await.expect("runtime initializes");
    let result = runtime.before_tool_call(&sum_request("sum", 1, 2), "sum").await.expect("pre hook runs");
    assert!(matches!(result.arguments, ToolArgumentsUpdate::Replace(Some(_))));

    config_store.clear_config().await;
    runtime.reload().await.expect("runtime reloads");
    let result = runtime.before_tool_call(&sum_request("sum", 1, 2), "sum").await.expect("pre hook skips");

    assert!(matches!(result.arguments, ToolArgumentsUpdate::Unchanged));
    assert_eq!(1, observations.lock().expect("observations lock poisoned").pre_calls);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn combined_plugin_preserves_context_from_pre_to_post() {
    let plugin = Arc::new(
        TestPlugin::new("context", vec![cmf_hook_names::TOOL_PRE_INVOKE, cmf_hook_names::TOOL_POST_INVOKE])
            .with_context_roundtrip(),
    );
    let observations = plugin.observations();
    let config_store = MemoryRuntimePluginConfigStore::with_config(plugin_config(&[Arc::clone(&plugin)]));
    let mut runtime = CpexRuntimeRegistry::with_config_store(Arc::new(config_store));
    runtime.register_factory("test", Box::new(TestPluginFactory::from_plugin(&plugin)));
    GatewayToolRuntime::initialize(&runtime).await.expect("runtime initializes");

    let pre = runtime.before_tool_call(&sum_request("sum", 1, 2), "sum").await.expect("pre hook runs");
    let response = rmcp::model::CallToolResult::success(vec![rmcp::model::Content::text("3")]);
    let result = runtime.after_tool_call("sum", response, pre.state).await.expect("post hook runs");

    assert_eq!("3", text(&result));
    assert_eq!(1, observations.lock().expect("observations lock poisoned").post_calls);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn registry_pins_runtime_from_pre_to_post_across_replacement() {
    let plugin = Arc::new(
        TestPlugin::new("context", vec![cmf_hook_names::TOOL_PRE_INVOKE, cmf_hook_names::TOOL_POST_INVOKE])
            .with_context_roundtrip(),
    );
    let observations = plugin.observations();
    let config_store = MemoryRuntimePluginConfigStore::with_config(plugin_config(&[Arc::clone(&plugin)]));
    let mut runtime = CpexRuntimeRegistry::with_config_store(Arc::new(config_store.clone()));
    runtime.register_factory("test", Box::new(TestPluginFactory::from_plugin(&plugin)));
    GatewayToolRuntime::initialize(&runtime).await.expect("runtime initializes");

    let pre = runtime.before_tool_call(&sum_request("sum", 1, 2), "sum").await.expect("pre hook runs");
    config_store.clear_config().await;
    runtime.reload().await.expect("runtime reloads");
    let response = rmcp::model::CallToolResult::success(vec![rmcp::model::Content::text("3")]);
    let result = runtime.after_tool_call("sum", response, pre.state).await.expect("post hook runs");

    assert_eq!("3", text(&result));
    assert_eq!(1, observations.lock().expect("observations lock poisoned").post_calls);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn registry_shutdowns_replaced_runtime_when_unused() {
    let plugin = Arc::new(TestPlugin::new("pre", vec![cmf_hook_names::TOOL_PRE_INVOKE]).with_pre_rewrite());
    let observations = plugin.observations();
    let config_store = MemoryRuntimePluginConfigStore::with_config(plugin_config(&[Arc::clone(&plugin)]));
    let mut runtime = CpexRuntimeRegistry::with_config_store(Arc::new(config_store.clone()));
    runtime.register_factory("test", Box::new(TestPluginFactory::from_plugin(&plugin)));
    GatewayToolRuntime::initialize(&runtime).await.expect("runtime initializes");

    config_store.clear_config().await;
    runtime.reload().await.expect("runtime reloads");

    assert_eq!(1, observations.lock().expect("observations lock poisoned").shutdown_calls);
}
