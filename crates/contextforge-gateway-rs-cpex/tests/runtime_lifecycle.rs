mod support;

use std::sync::Arc;

use contextforge_gateway_rs_cpex::CpexRuntimeRegistry;
use contextforge_gateway_rs_lib::GatewayToolRuntime;
use cpex_core::hooks::types::cmf_hook_names;

use support::{MemoryRuntimePluginConfigStore, TestPlugin, TestPluginFactory, plugin_config, sum_request, text};

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn combined_plugin_preserves_context_from_pre_to_post() {
    let plugin = Arc::new(
        TestPlugin::new("context", vec![cmf_hook_names::TOOL_PRE_INVOKE, cmf_hook_names::TOOL_POST_INVOKE])
            .with_context_roundtrip(),
    );
    let observations = plugin.observations();
    let config_store = MemoryRuntimePluginConfigStore::with_config(plugin_config(&[Arc::clone(&plugin)]));
    let mut runtime = CpexRuntimeRegistry::with_config_store(Arc::new(config_store));
    runtime
        .register_factory("test", Box::new(TestPluginFactory::from_plugin(&plugin)))
        .expect("test factory registers");
    GatewayToolRuntime::initialize(&runtime).await.expect("runtime initializes");

    let pre = runtime.before_tool_call(&sum_request("sum", 1, 2), "sum", "backend").await.expect("pre hook runs");
    let response = rmcp::model::CallToolResult::success(vec![rmcp::model::Content::text("3")]);
    let result = runtime.after_tool_call("sum", response, pre.state).await.expect("post hook runs");

    assert_eq!("3", text(&result));
    let observations = observations.lock().expect("observations lock poisoned");
    assert_eq!(1, observations.post_calls);
    assert_eq!(observations.pre_tool_call_id, observations.post_tool_call_id);
    assert_ne!(Some("gateway-tool-call".to_owned()), observations.post_tool_call_id);
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
    runtime
        .register_factory("test", Box::new(TestPluginFactory::from_plugin(&plugin)))
        .expect("test factory registers");
    GatewayToolRuntime::initialize(&runtime).await.expect("runtime initializes");

    let pre = runtime.before_tool_call(&sum_request("sum", 1, 2), "sum", "backend").await.expect("pre hook runs");
    config_store
        .set_config(serde_json::json!({
            "version": 1,
            "cpex": { "plugins": [] }
        }))
        .await;
    runtime.reload().await.expect("runtime reloads");
    let response = rmcp::model::CallToolResult::success(vec![rmcp::model::Content::text("3")]);
    let result = runtime.after_tool_call("sum", response, pre.state).await.expect("post hook runs");

    assert_eq!("3", text(&result));
    assert_eq!(1, observations.lock().expect("observations lock poisoned").post_calls);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn registry_does_not_apply_new_post_hook_to_in_flight_call() {
    let plugin = Arc::new(TestPlugin::new("post", vec![cmf_hook_names::TOOL_POST_INVOKE]).with_post_rewrite());
    let observations = plugin.observations();
    let config_store = MemoryRuntimePluginConfigStore::with_config(serde_json::json!({
        "version": 1,
        "cpex": { "plugins": [] }
    }));
    let mut runtime = CpexRuntimeRegistry::with_config_store(Arc::new(config_store.clone()));
    runtime
        .register_factory("test", Box::new(TestPluginFactory::from_plugin(&plugin)))
        .expect("test factory registers");
    GatewayToolRuntime::initialize(&runtime).await.expect("runtime initializes");

    let pre = runtime.before_tool_call(&sum_request("sum", 1, 2), "sum", "backend").await.expect("pre hook runs");
    config_store.set_config(plugin_config(&[Arc::clone(&plugin)])).await;
    runtime.reload().await.expect("runtime reloads");
    let response = rmcp::model::CallToolResult::success(vec![rmcp::model::Content::text("3")]);
    let result = runtime.after_tool_call("sum", response, pre.state).await.expect("post hook skips");

    assert_eq!("3", text(&result));
    assert_eq!(0, observations.lock().expect("observations lock poisoned").post_calls);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn registry_shutdowns_replaced_runtime_when_unused() {
    let plugin = Arc::new(TestPlugin::new("pre", vec![cmf_hook_names::TOOL_PRE_INVOKE]).with_pre_rewrite());
    let observations = plugin.observations();
    let config_store = MemoryRuntimePluginConfigStore::with_config(plugin_config(&[Arc::clone(&plugin)]));
    let mut runtime = CpexRuntimeRegistry::with_config_store(Arc::new(config_store.clone()));
    runtime
        .register_factory("test", Box::new(TestPluginFactory::from_plugin(&plugin)))
        .expect("test factory registers");
    GatewayToolRuntime::initialize(&runtime).await.expect("runtime initializes");

    config_store
        .set_config(serde_json::json!({
            "version": 1,
            "cpex": { "plugins": [] }
        }))
        .await;
    runtime.reload().await.expect("runtime reloads");

    assert_eq!(1, observations.lock().expect("observations lock poisoned").shutdown_calls);
}
