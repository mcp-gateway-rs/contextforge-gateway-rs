use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Default)]
pub enum RequestType {
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
#[serde(rename_all = "camelCase")]
pub struct BackendMCPGateway { 
    pub name: String,
    #[serde(default)]
    pub original_name: String,
    #[serde(default)]
    pub _computed_name: String,
    #[serde(default)]
    pub description: String,
    pub url: url::Url,
    #[serde(default)]
    pub plugin_chain_post: String,
    #[serde(default)]
    pub plugin_chain_pre: String,
    #[serde(default)]
    pub request_type: RequestType,
    #[serde(default)]
    pub integration_type: IntegrationType,
    #[serde(default)]
    pub expose_passthrough: bool,
    #[serde(default)]
    pub header_mapping: String,
    #[serde(default)]
    pub headers: String,
    #[serde(default)]
    pub reachable: String,
    #[serde(default)]
    pub enabled: String,
    #[serde(default)]
    pub visibility: String,
    #[serde(default)]
    pub team_id: String,
    #[serde(default)]
    pub gateway_id: String
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct VirtualHost {
    pub backends: HashMap<String, BackendMCPGateway>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UserConfig {
    pub virtual_hosts: HashMap<String, VirtualHost>,
}
