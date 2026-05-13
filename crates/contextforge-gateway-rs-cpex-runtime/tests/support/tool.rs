use rmcp::{
    ServiceError,
    model::{CallToolRequestParams, CallToolResult, ErrorCode},
};
use serde_json::{Map, Value};

pub(crate) fn sum_request(tool_name: impl Into<String>, a: i64, b: i64) -> CallToolRequestParams {
    CallToolRequestParams::new(tool_name.into())
        .with_arguments(Map::from_iter([("a".to_owned(), Value::from(a)), ("b".to_owned(), Value::from(b))]))
}

pub(crate) fn text(result: &CallToolResult) -> String {
    let text = result
        .content
        .iter()
        .filter_map(|content| content.as_text())
        .map(|text| text.text.as_str())
        .collect::<String>();
    assert!(!text.is_empty(), "text result");
    text
}

pub(crate) fn error_code(error: ServiceError) -> ErrorCode {
    let ServiceError::McpError(error) = error else {
        panic!("expected MCP error, got {error:?}");
    };
    error.code
}
