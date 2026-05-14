use contextforge_gateway_rs_lib::ToolArgumentsUpdate;
use cpex_core::cmf::MessagePayload;
use cpex_core::executor::PipelineResult;
use rmcp::{
    ErrorData,
    model::{CallToolResult, ErrorCode},
};
use tracing::warn;

use crate::cmf::{tool_call_arguments, tool_result_response};

pub(crate) fn modified_message_payload(result: &PipelineResult) -> Option<&MessagePayload> {
    result.modified_payload.as_ref().and_then(|payload| payload.as_any().downcast_ref::<MessagePayload>())
}

pub(crate) fn effective_pre_args(
    original_args: Option<&serde_json::Map<String, serde_json::Value>>,
    pre_result: &PipelineResult,
) -> Result<ToolArgumentsUpdate, ErrorData> {
    let Some(modified_payload) = modified_message_payload(pre_result) else {
        return Ok(ToolArgumentsUpdate::Unchanged);
    };

    let Some(arguments) = tool_call_arguments(modified_payload) else {
        return Err(ErrorData {
            code: ErrorCode::INVALID_PARAMS,
            message: "Plugin modified tool payload without a tool call".into(),
            data: None,
        });
    };

    if original_args == Some(&arguments) || (original_args.is_none() && arguments.is_empty()) {
        Ok(ToolArgumentsUpdate::Unchanged)
    } else {
        Ok(ToolArgumentsUpdate::Replace(Some(arguments.clone())))
    }
}

pub(crate) fn effective_post_result(original: CallToolResult, result: &PipelineResult) -> CallToolResult {
    match modified_message_payload(result) {
        Some(payload) => tool_result_response(original, payload),
        None => original,
    }
}

pub(crate) fn plugin_denied_error(result: PipelineResult) -> ErrorData {
    let code = result
        .violation
        .and_then(|violation| {
            warn!("Plugin denied tool call: code={} plugin={:?}", violation.code, violation.plugin_name);
            violation.proto_error_code.and_then(|code| i32::try_from(code).ok()).map(ErrorCode)
        })
        .unwrap_or(ErrorCode::INVALID_REQUEST);

    ErrorData { code, message: "Plugin denied tool call".into(), data: None }
}

pub(crate) fn log_pipeline_errors(hook: &'static str, result: &PipelineResult) {
    for error in &result.errors {
        warn!(
            hook,
            plugin = error.plugin_name,
            code = error.code.as_deref().unwrap_or(""),
            proto_error_code = error.proto_error_code,
            "CPEX plugin soft error"
        );
    }
}
