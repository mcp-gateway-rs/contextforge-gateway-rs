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
    let mut result = payload
        .message
        .get_tool_results()
        .first()
        .and_then(|tool_result| serde_json::from_value::<CallToolResult>(tool_result.content.clone()).ok())
        .unwrap_or(original);

    let text = payload.message.get_text_content();
    if !text.is_empty() {
        result.content.push(Content::text(text));
    }

    result
}
