use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Default)]
pub enum Transport {
    #[default]
    #[serde(rename = "STREAMABLEHTTP")]
    StreamableHttp,
    #[serde(rename = "SSE")]
    Sse,
    #[serde(rename = "STDIO")]
    Stdio,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Default)]
pub enum IntegrationType {
    #[serde(rename = "REST")]
    Rest,
    #[default]
    #[serde(rename = "MCP")]
    Mcp,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct BackendMCPGateway {
    pub name: String,
    pub url: url::Url,
    pub transport: Transport,
    pub passthrough_headers: Vec<String>,
    pub allowed_tool_names: Vec<String>,
    pub allowed_resource_names: Vec<String>,
    pub allowed_prompt_names: Vec<String>,
}   

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct VirtualHost {
    pub backends: HashMap<String, BackendMCPGateway>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct UserConfig {
    pub virtual_hosts: HashMap<String, VirtualHost>,
}
