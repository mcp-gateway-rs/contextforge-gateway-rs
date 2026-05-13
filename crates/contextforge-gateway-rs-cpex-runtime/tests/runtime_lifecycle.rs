mod support;

use std::sync::Arc;

use contextforge_gateway_rs_cpex_runtime::CpexRuntimeRegistry;
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
