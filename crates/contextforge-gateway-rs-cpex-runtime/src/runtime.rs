use std::sync::Arc;

use async_trait::async_trait;
use contextforge_gateway_rs_lib::{GatewayToolRuntime, RuntimeHookState, ToolPreCallResult};
use cpex_core::{
    cmf::{CmfHook, MessagePayload},
    config::CpexConfig,
    context::PluginContextTable,
    executor::PipelineResult,
    factory::PluginFactoryRegistry,
    hooks::{payload::Extensions, types::cmf_hook_names},
    manager::PluginManager,
};
use rmcp::{
    ErrorData,
    model::{CallToolRequestParams, CallToolResult},
};

use crate::{
    cmf::{tool_call_payload, tool_result_payload},
    error::GatewayPluginRuntimeError,
    pipeline::{effective_post_result, effective_pre_args, log_pipeline_errors, plugin_denied_error},
};

#[derive(Clone)]
pub(crate) struct GatewayPluginRuntime {
    inner: Arc<PluginRuntimeInner>,
}

struct PluginRuntimeInner {
    manager: Arc<PluginManager>,
    has_pre_hook: bool,
    has_post_hook: bool,
}

struct ToolCallState {
    context_table: PluginContextTable,
}

impl Default for GatewayPluginRuntime {
    fn default() -> Self {
        Self {
            inner: Arc::new(PluginRuntimeInner {
                manager: Arc::new(PluginManager::default()),
                has_pre_hook: false,
                has_post_hook: false,
            }),
        }
    }
}

impl GatewayPluginRuntime {
    pub(crate) async fn shutdown(&self) {
        self.inner.manager.shutdown().await;
    }

    pub(crate) async fn from_config(
        config: CpexConfig,
        factories: &PluginFactoryRegistry,
    ) -> Result<Self, GatewayPluginRuntimeError> {
        if config.routing_enabled() || !config.routes.is_empty() {
            return Err(GatewayPluginRuntimeError::ConfigUnsupported);
        }

        let has_pre_hook =
            config.plugins.iter().any(|plugin| plugin.hooks.iter().any(|hook| hook == cmf_hook_names::TOOL_PRE_INVOKE));
        let has_post_hook = config
            .plugins
            .iter()
            .any(|plugin| plugin.hooks.iter().any(|hook| hook == cmf_hook_names::TOOL_POST_INVOKE));
        let manager = PluginManager::from_config(config, factories)
            .map_err(|source| GatewayPluginRuntimeError::Configuration { hook: "config", source })?;
        let manager = Arc::new(manager);
        manager.initialize().await.map_err(|source| GatewayPluginRuntimeError::Initialization { source })?;
        Ok(Self { inner: Arc::new(PluginRuntimeInner { manager, has_pre_hook, has_post_hook }) })
    }

    async fn invoke_tool_pre(&self, payload: MessagePayload, extensions: Extensions) -> PipelineResult {
        let (result, background_tasks) = self
            .inner
            .manager
            .invoke_named::<CmfHook>(cmf_hook_names::TOOL_PRE_INVOKE, payload, extensions, None)
            .await;
        log_pipeline_errors(cmf_hook_names::TOOL_PRE_INVOKE, &result);
        drop(background_tasks);
        result
    }

    async fn invoke_tool_post(
        &self,
        payload: MessagePayload,
        extensions: Extensions,
        context_table: Option<PluginContextTable>,
    ) -> PipelineResult {
        let (result, background_tasks) = self
            .inner
            .manager
            .invoke_named::<CmfHook>(cmf_hook_names::TOOL_POST_INVOKE, payload, extensions, context_table)
            .await;
        log_pipeline_errors(cmf_hook_names::TOOL_POST_INVOKE, &result);
        drop(background_tasks);
        result
    }

    async fn before_tool_call_inner(
        &self,
        request: &CallToolRequestParams,
        tool_name: &str,
    ) -> Result<ToolPreCallResult, ErrorData> {
        if !self.inner.has_pre_hook {
            return Ok(ToolPreCallResult::unchanged());
        }

        let original_payload = tool_call_payload(request, tool_name);
        let pre_result = self.invoke_tool_pre(original_payload, Extensions::default()).await;
        if pre_result.is_denied() {
            return Err(plugin_denied_error(pre_result));
        }

        let arguments = effective_pre_args(request.arguments.as_ref(), &pre_result)?;
        let state = ToolCallState { context_table: pre_result.context_table };
        Ok(ToolPreCallResult { arguments, state: Some(Box::new(state)) })
    }

    async fn after_tool_call_inner(
        &self,
        tool_name: &str,
        response: CallToolResult,
        state: Option<RuntimeHookState>,
    ) -> Result<CallToolResult, ErrorData> {
        if !self.inner.has_post_hook {
            return Ok(response);
        }

        let state = state.and_then(|state| state.downcast::<ToolCallState>().ok());
        let context_table = state.map(|state| state.context_table);
        let post_result = self
            .invoke_tool_post(tool_result_payload(tool_name, &response), Extensions::default(), context_table)
            .await;
        if post_result.is_denied() {
            return Err(plugin_denied_error(post_result));
        }

        Ok(effective_post_result(response, &post_result))
    }
}

#[async_trait]
impl GatewayToolRuntime for GatewayPluginRuntime {
    async fn before_tool_call(
        &self,
        request: &CallToolRequestParams,
        tool_name: &str,
    ) -> Result<ToolPreCallResult, ErrorData> {
        self.before_tool_call_inner(request, tool_name).await
    }

    async fn after_tool_call(
        &self,
        tool_name: &str,
        response: CallToolResult,
        state: Option<RuntimeHookState>,
    ) -> Result<CallToolResult, ErrorData> {
        self.after_tool_call_inner(tool_name, response, state).await
    }
}
