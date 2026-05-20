mod support;

use std::sync::Arc;

use cpex_core::cmf::Role;
use cpex_core::hooks::types::cmf_hook_names;
use rmcp::model::ErrorCode;
use serde_json::Value;

use support::{
    POST_DENY_ERROR_CODE, PRE_DENY_ERROR_CODE, REWRITTEN_SUM_A, REWRITTEN_SUM_B, TestPlugin, error_code,
    runtime_with_post, runtime_with_pre, runtime_with_pre_and_post, start_gateway, sum_request, text,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn disabled_runtime_does_not_invoke_registered_plugin() {
    let pre_plugin =
        Arc::new(TestPlugin::new("disabled-pre", vec![cmf_hook_names::TOOL_PRE_INVOKE]).with_pre_rewrite());
    let post_plugin =
        Arc::new(TestPlugin::new("disabled-post", vec![cmf_hook_names::TOOL_POST_INVOKE]).with_post_rewrite());
    let pre_observations = pre_plugin.observations();
    let post_observations = post_plugin.observations();
    let runtime = runtime_with_pre_and_post(pre_plugin, post_plugin).await;

    let gateway = start_gateway("admin@example.com", false, runtime).await;
    let service = gateway.connect("admin@example.com").await;
    let result = service.call_tool(sum_request(format!("{}-sum", gateway.backend_name), 1, 2)).await.unwrap();

    assert_eq!("3", text(&result));
    assert_eq!(0, pre_observations.lock().expect("observations lock poisoned").pre_calls);
    assert_eq!(0, post_observations.lock().expect("observations lock poisoned").post_calls);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn pre_hook_modifies_backend_arguments_without_rerouting_tool() {
    let plugin = Arc::new(TestPlugin::new("pre", vec![cmf_hook_names::TOOL_PRE_INVOKE]).with_pre_rewrite());
    let observations = plugin.observations();
    let runtime = runtime_with_pre(plugin).await;

    let gateway = start_gateway("admin@example.com", true, runtime).await;
    let service = gateway.connect("admin@example.com").await;
    let result = service.call_tool(sum_request(format!("{}-sum", gateway.backend_name), 1, 2)).await.unwrap();

    assert_eq!((REWRITTEN_SUM_A + REWRITTEN_SUM_B).to_string(), text(&result));
    let backend_calls = gateway.backend_state.calls.lock().expect("backend calls lock poisoned");
    assert_eq!("sum", backend_calls[0].tool_name);
    assert_eq!(Some(&Value::from(REWRITTEN_SUM_A)), backend_calls[0].args.as_ref().and_then(|args| args.get("a")));

    let observations = observations.lock().expect("observations lock poisoned");
    assert_eq!(1, observations.pre_calls);
    assert_eq!(Some("sum".to_owned()), observations.pre_payload_name);
    assert_eq!(Some(gateway.backend_name.clone()), observations.pre_payload_namespace);
    assert_eq!(Some(Role::Assistant), observations.pre_payload_role);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn post_hook_receives_backend_result_and_modifies_client_result() {
    let plugin = Arc::new(TestPlugin::new("post", vec![cmf_hook_names::TOOL_POST_INVOKE]).with_post_rewrite());
    let observations = plugin.observations();
    let runtime = runtime_with_post(plugin).await;

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
async fn post_hook_can_return_raw_cmf_result_content() {
    let plugin = Arc::new(TestPlugin::new("post", vec![cmf_hook_names::TOOL_POST_INVOKE]).with_raw_post_rewrite());
    let runtime = runtime_with_post(plugin).await;

    let gateway = start_gateway("admin@example.com", true, runtime).await;
    let service = gateway.connect("admin@example.com").await;
    let result = service.call_tool(sum_request(format!("{}-sum", gateway.backend_name), 1, 2)).await.unwrap();

    assert_eq!("raw-post", text(&result));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn pre_and_post_hooks_share_gateway_call_context() {
    let pre_plugin =
        Arc::new(TestPlugin::new("context-pre", vec![cmf_hook_names::TOOL_PRE_INVOKE]).with_context_roundtrip());
    let post_plugin =
        Arc::new(TestPlugin::new("context-post", vec![cmf_hook_names::TOOL_POST_INVOKE]).with_context_roundtrip());
    let post_observations = post_plugin.observations();
    let runtime = runtime_with_pre_and_post(pre_plugin, post_plugin).await;

    let gateway = start_gateway("admin@example.com", true, runtime).await;
    let service = gateway.connect("admin@example.com").await;
    let result = service.call_tool(sum_request(format!("{}-sum", gateway.backend_name), 1, 2)).await.unwrap();

    assert_eq!("3", text(&result));
    assert_eq!(1, post_observations.lock().expect("observations lock poisoned").post_calls);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn pre_and_post_denials_return_plugin_error_codes() {
    let pre_plugin = Arc::new(TestPlugin::new("pre-deny", vec![cmf_hook_names::TOOL_PRE_INVOKE]).with_pre_deny());
    let runtime = runtime_with_pre(pre_plugin).await;
    let gateway = start_gateway("admin@example.com", true, runtime).await;
    let service = gateway.connect("admin@example.com").await;
    let error = service.call_tool(sum_request(format!("{}-sum", gateway.backend_name), 1, 2)).await.unwrap_err();
    assert_eq!(ErrorCode(PRE_DENY_ERROR_CODE), error_code(error));
    assert!(gateway.backend_state.calls.lock().expect("backend calls lock poisoned").is_empty());

    let post_plugin = Arc::new(TestPlugin::new("post-deny", vec![cmf_hook_names::TOOL_POST_INVOKE]).with_post_deny());
    let runtime = runtime_with_post(post_plugin).await;
    let gateway = start_gateway("admin@example.com", true, runtime).await;
    let service = gateway.connect("admin@example.com").await;
    let error = service.call_tool(sum_request(format!("{}-sum", gateway.backend_name), 1, 2)).await.unwrap_err();
    assert_eq!(ErrorCode(POST_DENY_ERROR_CODE), error_code(error));
    assert_eq!(1, gateway.backend_state.calls.lock().expect("backend calls lock poisoned").len());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn pre_hook_invalid_arguments_return_invalid_params() {
    let plugin =
        Arc::new(TestPlugin::new("invalid-args", vec![cmf_hook_names::TOOL_PRE_INVOKE]).with_invalid_pre_args());
    let runtime = runtime_with_pre(plugin).await;
    let gateway = start_gateway("admin@example.com", true, runtime).await;
    let service = gateway.connect("admin@example.com").await;
    let error = service.call_tool(sum_request(format!("{}-sum", gateway.backend_name), 1, 2)).await.unwrap_err();

    assert_eq!(ErrorCode::INVALID_PARAMS, error_code(error));
    assert!(gateway.backend_state.calls.lock().expect("backend calls lock poisoned").is_empty());
}
