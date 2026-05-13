mod support;

use std::sync::Arc;

use contextforge_gateway_rs_cpex_runtime::CpexRuntimeRegistry;
use cpex_core::hooks::types::cmf_hook_names;
use rmcp::model::ErrorCode;
use serde_json::{Value, json};

use support::{
    MemoryRuntimePluginConfigStore, TestPlugin, TestPluginFactory, error_code, runtime_with_post, runtime_with_pre,
    runtime_with_pre_and_post, start_gateway, sum_request, text,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn disabled_runtime_does_not_invoke_registered_plugin() {
    let pre_plugin =
        Arc::new(TestPlugin::new("disabled-pre", vec![cmf_hook_names::TOOL_PRE_INVOKE]).with_pre_rewrite());
    let post_plugin =
        Arc::new(TestPlugin::new("disabled-post", vec![cmf_hook_names::TOOL_POST_INVOKE]).with_post_rewrite());
    let pre_observations = pre_plugin.observations();
    let post_observations = post_plugin.observations();
    let runtime = runtime_with_pre_and_post(pre_plugin, post_plugin);

    let gateway = start_gateway("admin@example.com", false, runtime).await;
    let service = gateway.connect("admin@example.com").await;
    let result = service.call_tool(sum_request(format!("{}-sum", gateway.backend_name), 1, 2)).await.unwrap();

    assert_eq!("3", text(&result));
    assert_eq!(0, pre_observations.lock().expect("observations lock poisoned").pre_calls);
    assert_eq!(0, post_observations.lock().expect("observations lock poisoned").post_calls);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn disabled_runtime_does_not_load_config() {
    let config_store = MemoryRuntimePluginConfigStore::with_config(json!({
        "version": 1,
        "cpex": { "plugins": [] }
    }));
    let runtime = Arc::new(CpexRuntimeRegistry::with_config_store(Arc::new(config_store.clone())));

    let gateway = start_gateway("admin@example.com", false, runtime).await;
    let service = gateway.connect("admin@example.com").await;
    let result = service.call_tool(sum_request(format!("{}-sum", gateway.backend_name), 1, 2)).await.unwrap();

    assert_eq!("3", text(&result));
    assert_eq!(0, config_store.calls());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn pre_hook_modifies_backend_arguments_without_rerouting_tool() {
    let plugin = Arc::new(TestPlugin::new("pre", vec![cmf_hook_names::TOOL_PRE_INVOKE]).with_pre_rewrite());
    let observations = plugin.observations();
    let runtime = runtime_with_pre(plugin);

    let gateway = start_gateway("admin@example.com", true, runtime).await;
    let service = gateway.connect("admin@example.com").await;
    let result = service.call_tool(sum_request(format!("{}-sum", gateway.backend_name), 1, 2)).await.unwrap();

    assert_eq!("30", text(&result));
    let backend_calls = gateway.backend_state.calls.lock().expect("backend calls lock poisoned");
    assert_eq!("sum", backend_calls[0].tool_name);
    assert_eq!(Some(&Value::from(10)), backend_calls[0].args.as_ref().and_then(|args| args.get("a")));

    let observations = observations.lock().expect("observations lock poisoned");
    assert_eq!(1, observations.pre_calls);
    assert_eq!(Some("sum".to_owned()), observations.pre_payload_name);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn post_hook_receives_backend_result_and_modifies_client_result() {
    let plugin = Arc::new(TestPlugin::new("post", vec![cmf_hook_names::TOOL_POST_INVOKE]).with_post_rewrite());
    let observations = plugin.observations();
    let runtime = runtime_with_post(plugin);

    let gateway = start_gateway("admin@example.com", true, runtime).await;
    let service = gateway.connect("admin@example.com").await;
    let result = service.call_tool(sum_request(format!("{}-sum", gateway.backend_name), 1, 2)).await.unwrap();

    assert_eq!("post:3", text(&result));
    let observations = observations.lock().expect("observations lock poisoned");
    assert_eq!(1, observations.post_calls);
    assert_eq!(Some("sum".to_owned()), observations.post_payload_name);
    assert_eq!(Some("3".to_owned()), observations.post_result_text);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn pre_and_post_hooks_share_gateway_call_context() {
    let pre_plugin =
        Arc::new(TestPlugin::new("context-pre", vec![cmf_hook_names::TOOL_PRE_INVOKE]).with_context_roundtrip());
    let post_plugin =
        Arc::new(TestPlugin::new("context-post", vec![cmf_hook_names::TOOL_POST_INVOKE]).with_context_roundtrip());
    let post_observations = post_plugin.observations();
    let runtime = runtime_with_pre_and_post(pre_plugin, post_plugin);

    let gateway = start_gateway("admin@example.com", true, runtime).await;
    let service = gateway.connect("admin@example.com").await;
    let result = service.call_tool(sum_request(format!("{}-sum", gateway.backend_name), 1, 2)).await.unwrap();

    assert_eq!("3", text(&result));
    assert_eq!(1, post_observations.lock().expect("observations lock poisoned").post_calls);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn pre_and_post_denials_return_plugin_error_codes() {
    let pre_plugin = Arc::new(TestPlugin::new("pre-deny", vec![cmf_hook_names::TOOL_PRE_INVOKE]).with_pre_deny());
    let runtime = runtime_with_pre(pre_plugin);
    let gateway = start_gateway("admin@example.com", true, runtime).await;
    let service = gateway.connect("admin@example.com").await;
    let error = service.call_tool(sum_request(format!("{}-sum", gateway.backend_name), 1, 2)).await.unwrap_err();
    assert_eq!(ErrorCode(-32001), error_code(error));
    assert!(gateway.backend_state.calls.lock().expect("backend calls lock poisoned").is_empty());

    let post_plugin = Arc::new(TestPlugin::new("post-deny", vec![cmf_hook_names::TOOL_POST_INVOKE]).with_post_deny());
    let runtime = runtime_with_post(post_plugin);
    let gateway = start_gateway("admin@example.com", true, runtime).await;
    let service = gateway.connect("admin@example.com").await;
    let error = service.call_tool(sum_request(format!("{}-sum", gateway.backend_name), 1, 2)).await.unwrap_err();
    assert_eq!(ErrorCode(-32002), error_code(error));
    assert_eq!(1, gateway.backend_state.calls.lock().expect("backend calls lock poisoned").len());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn pre_hook_invalid_arguments_return_invalid_params() {
    let plugin =
        Arc::new(TestPlugin::new("invalid-args", vec![cmf_hook_names::TOOL_PRE_INVOKE]).with_invalid_pre_args());
    let runtime = runtime_with_pre(plugin);
    let gateway = start_gateway("admin@example.com", true, runtime).await;
    let service = gateway.connect("admin@example.com").await;
    let error = service.call_tool(sum_request(format!("{}-sum", gateway.backend_name), 1, 2)).await.unwrap_err();

    assert_eq!(ErrorCode::INVALID_PARAMS, error_code(error));
    assert!(gateway.backend_state.calls.lock().expect("backend calls lock poisoned").is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn enabled_runtime_applies_config_change_after_gateway_start() {
    let plugin = Arc::new(TestPlugin::new("runtime-pre", vec![cmf_hook_names::TOOL_PRE_INVOKE]).with_pre_rewrite());
    let observations = plugin.observations();
    let config_store = MemoryRuntimePluginConfigStore::with_config(json!({
        "version": 1,
        "cpex": { "plugins": [] }
    }));
    let mut runtime = CpexRuntimeRegistry::with_config_store(Arc::new(config_store.clone()));
    runtime.register_factory("test", Box::new(TestPluginFactory::from_plugin(&plugin)));
    let runtime = Arc::new(runtime);
    let gateway = start_gateway("admin@example.com", true, Arc::clone(&runtime)).await;
    let service = gateway.connect("admin@example.com").await;

    let result = service.call_tool(sum_request(format!("{}-sum", gateway.backend_name), 1, 2)).await.unwrap();
    assert_eq!("3", text(&result));

    config_store
        .set_config(json!({
            "version": 1,
            "cpex": {
                "plugins": [{
                    "name": "runtime-pre",
                    "kind": "test",
                    "hooks": [cmf_hook_names::TOOL_PRE_INVOKE]
                }]
            }
        }))
        .await;
    runtime.reload().await.expect("runtime reloads");

    let result = service.call_tool(sum_request(format!("{}-sum", gateway.backend_name), 1, 2)).await.unwrap();
    assert_eq!("30", text(&result));
    assert_eq!(1, observations.lock().expect("observations lock poisoned").pre_calls);
}
