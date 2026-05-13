mod support;

use std::sync::Arc;

use contextforge_gateway_rs_cpex_runtime::{CmfPluginFactory, CpexRuntimeRegistry};
use contextforge_gateway_rs_lib::{GatewayToolRuntime, ToolArgumentsUpdate};
use cpex_core::hooks::types::cmf_hook_names;
use serde_json::json;

use support::{MemoryRuntimePluginConfigStore, TestPlugin, TestPluginFactory, sum_request};

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
async fn runtime_config_loads_generic_cmf_factory_plugin() {
    let config_store = MemoryRuntimePluginConfigStore::with_config(json!({
        "version": 1,
        "cpex": {
            "plugins": [{
                "name": "generic-pre",
                "kind": "generic",
                "hooks": [cmf_hook_names::TOOL_PRE_INVOKE]
            }]
        }
    }));
    let mut runtime = CpexRuntimeRegistry::with_config_store(Arc::new(config_store));
    runtime.register_factory("generic", Box::new(CmfPluginFactory::new(TestPlugin::rewrite_from_config)));

    GatewayToolRuntime::initialize(&runtime).await.expect("runtime initializes");
    let result = runtime.before_tool_call(&sum_request("sum", 1, 2), "sum").await.expect("pre hook runs");

    match result.arguments {
        ToolArgumentsUpdate::Replace(Some(args)) => {
            assert_eq!(Some(&json!(10)), args.get("a"));
            assert_eq!(Some(&json!(20)), args.get("b"));
        },
        other => panic!("expected rewritten args, got {other:?}"),
    }
}
