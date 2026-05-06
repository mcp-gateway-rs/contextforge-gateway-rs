#![allow(dead_code)]
use std::{any::Any, sync::Arc};

use rmcp::{
    ErrorData as McpError, RoleServer, ServerHandler,
    handler::server::{
        router::{prompt::PromptRouter, tool::ToolRouter},
        wrapper::Parameters,
    },
    model::*,
    prompt, prompt_handler, prompt_router, schemars,
    service::RequestContext,
    task_handler,
    task_manager::{OperationProcessor, OperationResultTransport},
    tool, tool_handler, tool_router,
};
use serde_json::json;
use tokio::sync::Mutex;

struct ToolCallOperationResult {
    id: String,
    result: Result<CallToolResult, McpError>,
}

impl OperationResultTransport for ToolCallOperationResult {
    fn operation_id(&self) -> &String {
        &self.id
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct StructRequest {
    pub a: i32,
    pub b: i32,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct ExamplePromptArgs {
    /// A message to put in the prompt
    pub message: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct CounterAnalysisArgs {
    /// The target value you're trying to reach
    pub goal: i32,
    /// Preferred strategy: 'fast' or 'careful'
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strategy: Option<String>,
}

#[derive(Clone)]
pub struct Counter {
    counter: Arc<Mutex<i32>>,
    tool_router: ToolRouter<Counter>,
    prompt_router: PromptRouter<Counter>,
    processor: Arc<Mutex<OperationProcessor>>,
}

#[tool_router]
impl Counter {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            counter: Arc::new(Mutex::new(0)),
            tool_router: Self::tool_router(),
            prompt_router: Self::prompt_router(),
            processor: Arc::new(Mutex::new(OperationProcessor::new())),
        }
    }

    fn _create_resource_text(&self, uri: &str, name: &str) -> Resource {
        RawResource::new(uri, name.to_string()).no_annotation()
    }

    #[tool(description = "Increment the counter by 1")]
    async fn increment(&self) -> Result<CallToolResult, McpError> {
        let mut counter = self.counter.lock().await;
        *counter += 1;
        Ok(CallToolResult::success(vec![Content::text(counter.to_string())]))
    }

    #[tool(description = "Decrement the counter by 1")]
    async fn decrement(&self) -> Result<CallToolResult, McpError> {
        let mut counter = self.counter.lock().await;
        *counter -= 1;
        Ok(CallToolResult::success(vec![Content::text(counter.to_string())]))
    }

    #[tool(description = "Get the current counter value")]
    async fn get_value(&self) -> Result<CallToolResult, McpError> {
        let counter = self.counter.lock().await;
        Ok(CallToolResult::success(vec![Content::text(counter.to_string())]))
    }

    #[tool(description = "Long running task example")]
    async fn long_task(&self) -> Result<CallToolResult, McpError> {
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        Ok(CallToolResult::success(vec![Content::text("Long task completed")]))
    }

    #[tool(description = "Say hello to the client")]
    fn say_hello(&self) -> Result<CallToolResult, McpError> {
        Ok(CallToolResult::success(vec![Content::text("hello")]))
    }

    #[tool(description = "Repeat what you say")]
    fn echo(&self, Parameters(object): Parameters<JsonObject>) -> Result<CallToolResult, McpError> {
        Ok(CallToolResult::success(vec![Content::text(serde_json::Value::Object(object).to_string())]))
    }

    #[tool(description = "Calculate the sum of two numbers")]
    fn sum(&self, Parameters(StructRequest { a, b }): Parameters<StructRequest>) -> Result<CallToolResult, McpError> {
        Ok(CallToolResult::success(vec![Content::text((a + b).to_string())]))
    }

    /// Returns the `Mcp-Session-Id` of the current session (streamable HTTP only).
    #[tool(description = "Get the session ID for this connection")]
    fn get_session_id(&self, ctx: RequestContext<RoleServer>) -> Result<CallToolResult, McpError> {
        let session_id = ctx
            .extensions
            .get::<axum::http::request::Parts>()
            .and_then(|parts| parts.headers.get("mcp-session-id"))
            .map(|v| v.to_str().unwrap_or("(non-ascii)").to_owned());

        match session_id {
            Some(id) => Ok(CallToolResult::success(vec![Content::text(id)])),
            None => Ok(CallToolResult::success(vec![Content::text("no session (not running over streamable HTTP?)")])),
        }
    }
}

#[prompt_router]
impl Counter {
    /// This is an example prompt that takes one required argument, message
    #[prompt(
        name = "example_prompt",
        meta = Meta(rmcp::object!({"meta_key": "meta_value"}))
    )]
    async fn example_prompt(
        &self,
        Parameters(args): Parameters<ExamplePromptArgs>,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<Vec<PromptMessage>, McpError> {
        let prompt = format!("This is an example prompt with your message here: '{}'", args.message);
        Ok(vec![PromptMessage::new_text(PromptMessageRole::User, prompt)])
    }

    /// Analyze the current counter value and suggest next steps
    #[prompt(name = "counter_analysis")]
    async fn counter_analysis(
        &self,
        Parameters(args): Parameters<CounterAnalysisArgs>,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<GetPromptResult, McpError> {
        let strategy = args.strategy.unwrap_or_else(|| "careful".to_string());
        let current_value = *self.counter.lock().await;
        let difference = args.goal - current_value;

        let messages = vec![
            PromptMessage::new_text(
                PromptMessageRole::Assistant,
                "I'll analyze the counter situation and suggest the best approach.",
            ),
            PromptMessage::new_text(
                PromptMessageRole::User,
                format!(
                    "Current counter value: {}\nGoal value: {}\nDifference: {}\nStrategy preference: {}\n\nPlease analyze the situation and suggest the best approach to reach the goal.",
                    current_value, args.goal, difference, strategy
                ),
            ),
        ];

        Ok(GetPromptResult::new(messages)
            .with_description(format!("Counter analysis for reaching {} from {}", args.goal, current_value)))
    }
}

#[tool_handler(meta = Meta(rmcp::object!({"tool_meta_key": "tool_meta_value"})))]
#[prompt_handler(meta = Meta(rmcp::object!({"router_meta_key": "router_meta_value"})))]
#[task_handler]
impl ServerHandler for Counter {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_prompts()
                .enable_resources()
                .enable_tools()
                .build(),
        )
        .with_server_info(Implementation::from_build_env())
        .with_protocol_version(ProtocolVersion::V_2024_11_05)
        .with_instructions("This server provides counter tools and prompts. Tools: increment, decrement, get_value, say_hello, echo, sum. Prompts: example_prompt (takes a message), counter_analysis (analyzes counter state with a goal).".to_string())
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, McpError> {
        Ok(ListResourcesResult {
            resources: vec![
                self._create_resource_text("str:////Users/to/some/path/", "cwd"),
                self._create_resource_text("memo://insights", "memo-name"),
            ],
            next_cursor: None,
            meta: None,
        })
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, McpError> {
        let uri = &request.uri;
        match uri.as_str() {
            "str:////Users/to/some/path/" => {
                let cwd = "/Users/to/some/path/";
                Ok(ReadResourceResult::new(vec![ResourceContents::text(cwd, uri.clone())]))
            },
            "memo://insights" => {
                let memo = "Business Intelligence Memo\n\nAnalysis has revealed 5 key insights ...";
                Ok(ReadResourceResult::new(vec![ResourceContents::text(memo, uri.clone())]))
            },
            _ => Err(McpError::resource_not_found(
                "resource_not_found",
                Some(json!({
                    "uri": uri
                })),
            )),
        }
    }

    async fn list_resource_templates(
        &self,
        _request: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, McpError> {
        Ok(ListResourceTemplatesResult { next_cursor: None, resource_templates: Vec::new(), meta: None })
    }

    async fn initialize(
        &self,
        _request: InitializeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<InitializeResult, McpError> {
        if let Some(http_request_part) = context.extensions.get::<axum::http::request::Parts>() {
            let initialize_headers = &http_request_part.headers;
            let initialize_uri = &http_request_part.uri;
            tracing::info!(?initialize_headers, %initialize_uri, "initialize from http server");
        }
        Ok(self.get_info())
    }
}
