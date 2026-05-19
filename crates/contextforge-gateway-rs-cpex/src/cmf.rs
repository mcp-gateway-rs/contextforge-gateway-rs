use cpex_core::cmf::{ContentPart, Message, MessagePayload, Role, ToolCall, ToolResult};
use rmcp::model::{CallToolRequestParams, CallToolResult, Content};
use serde_json::{Map, Value};

pub(crate) fn tool_call_payload(
    request: &CallToolRequestParams,
    tool_name: &str,
    backend_name: &str,
    tool_call_id: &str,
) -> MessagePayload {
    MessagePayload {
        message: Message {
            schema_version: "2.0".to_owned(),
            role: Role::Assistant,
            content: vec![ContentPart::ToolCall {
                content: ToolCall {
                    tool_call_id: tool_call_id.to_owned(),
                    name: tool_name.to_owned(),
                    arguments: request.arguments.clone().unwrap_or_default().into_iter().collect(),
                    namespace: Some(backend_name.to_owned()),
                },
            }],
            channel: None,
        },
    }
}

pub(crate) fn tool_result_payload(tool_name: &str, response: &CallToolResult, tool_call_id: &str) -> MessagePayload {
    MessagePayload {
        message: Message {
            schema_version: "2.0".to_owned(),
            role: Role::Tool,
            content: vec![ContentPart::ToolResult {
                content: ToolResult {
                    tool_call_id: tool_call_id.to_owned(),
                    tool_name: tool_name.to_owned(),
                    content: serde_json::to_value(response).unwrap_or(Value::Null),
                    is_error: response.is_error.unwrap_or(false),
                },
            }],
            channel: None,
        },
    }
}

pub(crate) fn tool_call_arguments(payload: &MessagePayload) -> Option<Map<String, Value>> {
    payload
        .message
        .get_tool_calls()
        .first()
        .map(|tool_call| tool_call.arguments.clone().into_iter().collect::<Map<String, Value>>())
}

pub(crate) fn tool_result_response(original: CallToolResult, payload: &MessagePayload) -> CallToolResult {
    let mut result = payload.message.get_tool_results().first().map_or(original, |tool_result| {
        serde_json::from_value::<CallToolResult>(tool_result.content.clone()).map_or_else(
            |_| {
                if tool_result.is_error {
                    raw_error_tool_result(tool_result.content.clone())
                } else {
                    raw_success_tool_result(tool_result.content.clone())
                }
            },
            |mut result| {
                result.is_error = Some(tool_result.is_error);
                result
            },
        )
    });

    let text = payload.message.get_text_content();
    if !text.is_empty() {
        result.content.push(Content::text(text));
    }

    result
}

fn raw_success_tool_result(value: Value) -> CallToolResult {
    if let Value::String(text) = value {
        CallToolResult::success(vec![Content::text(text)])
    } else {
        CallToolResult::structured(value)
    }
}

fn raw_error_tool_result(value: Value) -> CallToolResult {
    if let Value::String(text) = value {
        CallToolResult::error(vec![Content::text(text)])
    } else {
        CallToolResult::structured_error(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_result_response_uses_cmf_error_flag_for_nested_mcp_result() {
        let original = CallToolResult::success(vec![Content::text("original")]);
        let nested = CallToolResult::success(vec![Content::text("changed")]);
        let mut payload = tool_result_payload("sum", &nested, "call-1");
        let ContentPart::ToolResult { content } = &mut payload.message.content[0] else {
            panic!("expected tool result");
        };
        content.is_error = true;

        let result = tool_result_response(original, &payload);

        assert_eq!(Some(true), result.is_error);
    }
}
