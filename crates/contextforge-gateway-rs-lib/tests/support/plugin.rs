use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use cpex_core::{
    cmf::{CmfHook, ContentPart, Message, MessagePayload, Role},
    context::PluginContext,
    error::{PluginError, PluginViolation},
    factory::{PluginFactory, PluginInstance},
    hooks::{Extensions, HookHandler, PluginResult, TypedHandlerAdapter, types::cmf_hook_names},
    plugin::{Plugin, PluginConfig},
};
use rmcp::model::{CallToolResult, Content};
use serde_json::json;

use super::tool::text;

pub(crate) const PRE_DENY_ERROR_CODE: i32 = -32001;
pub(crate) const POST_DENY_ERROR_CODE: i32 = -32002;
const MISSING_CONTEXT_ERROR_CODE: i32 = -32003;
pub(crate) const REWRITTEN_SUM_A: i64 = 10;
pub(crate) const REWRITTEN_SUM_B: i64 = 20;

#[derive(Default)]
pub(crate) struct Observations {
    pub(crate) pre_calls: usize,
    pub(crate) post_calls: usize,
    pub(crate) shutdown_calls: usize,
    pub(crate) pre_payload_name: Option<String>,
    pub(crate) pre_payload_namespace: Option<String>,
    pub(crate) pre_payload_role: Option<Role>,
    pub(crate) pre_tool_call_id: Option<String>,
    pub(crate) post_payload_name: Option<String>,
    pub(crate) post_tool_call_id: Option<String>,
    pub(crate) post_result_text: Option<String>,
}

#[derive(Clone, Copy, Default)]
pub(crate) enum PreBehavior {
    #[default]
    Allow,
    Rewrite,
    Deny,
    InvalidArgs,
    SetContext,
}

#[derive(Clone, Copy, Default)]
pub(crate) enum PostBehavior {
    #[default]
    Allow,
    Rewrite,
    RewriteRaw,
    Deny,
    RequireContext,
}

pub(crate) struct TestPlugin {
    pub(crate) config: PluginConfig,
    pub(crate) observations: Arc<Mutex<Observations>>,
    pre_behavior: PreBehavior,
    post_behavior: PostBehavior,
}

impl TestPlugin {
    pub(crate) fn new(name: &str, hooks: Vec<&'static str>) -> Self {
        Self {
            config: PluginConfig {
                name: name.to_owned(),
                kind: "test".to_owned(),
                hooks: hooks.into_iter().map(str::to_owned).collect(),
                ..Default::default()
            },
            observations: Arc::new(Mutex::new(Observations::default())),
            pre_behavior: PreBehavior::Allow,
            post_behavior: PostBehavior::Allow,
        }
    }

    pub(crate) fn rewrite_from_config(config: PluginConfig) -> Self {
        Self {
            config,
            observations: Arc::new(Mutex::new(Observations::default())),
            pre_behavior: PreBehavior::Rewrite,
            post_behavior: PostBehavior::Allow,
        }
    }

    pub(crate) fn with_pre_rewrite(mut self) -> Self {
        self.pre_behavior = PreBehavior::Rewrite;
        self
    }

    pub(crate) fn with_post_rewrite(mut self) -> Self {
        self.post_behavior = PostBehavior::Rewrite;
        self
    }

    pub(crate) fn with_raw_post_rewrite(mut self) -> Self {
        self.post_behavior = PostBehavior::RewriteRaw;
        self
    }

    pub(crate) fn with_pre_deny(mut self) -> Self {
        self.pre_behavior = PreBehavior::Deny;
        self
    }

    pub(crate) fn with_post_deny(mut self) -> Self {
        self.post_behavior = PostBehavior::Deny;
        self
    }

    pub(crate) fn with_invalid_pre_args(mut self) -> Self {
        self.pre_behavior = PreBehavior::InvalidArgs;
        self
    }

    pub(crate) fn with_context_roundtrip(mut self) -> Self {
        self.pre_behavior = PreBehavior::SetContext;
        self.post_behavior = PostBehavior::RequireContext;
        self
    }

    pub(crate) fn observations(&self) -> Arc<Mutex<Observations>> {
        Arc::clone(&self.observations)
    }
}

#[async_trait]
impl Plugin for TestPlugin {
    fn config(&self) -> &PluginConfig {
        &self.config
    }

    async fn shutdown(&self) -> Result<(), Box<PluginError>> {
        self.observations.lock().expect("observations lock poisoned").shutdown_calls += 1;
        Ok(())
    }
}

impl HookHandler<CmfHook> for TestPlugin {
    fn handle(
        &self,
        payload: &MessagePayload,
        _extensions: &Extensions,
        ctx: &mut PluginContext,
    ) -> PluginResult<MessagePayload> {
        let is_post = payload.message.role == Role::Tool;
        let mut observations = self.observations.lock().expect("observations lock poisoned");
        if is_post {
            observations.post_calls += 1;
            if let Some(result) = payload.message.get_tool_results().first() {
                observations.post_payload_name = Some(result.tool_name.clone());
                observations.post_tool_call_id = Some(result.tool_call_id.clone());
            }
            observations.post_result_text = Some(cmf_result_text(payload));
        } else {
            observations.pre_calls += 1;
            if let Some(call) = payload.message.get_tool_calls().first() {
                observations.pre_payload_name = Some(call.name.clone());
                observations.pre_payload_namespace.clone_from(&call.namespace);
                observations.pre_payload_role = Some(payload.message.role);
                observations.pre_tool_call_id = Some(call.tool_call_id.clone());
            }
        }
        drop(observations);

        if is_post {
            match self.post_behavior {
                PostBehavior::Allow => PluginResult::allow(),
                PostBehavior::Rewrite => {
                    let mut modified = payload.clone();
                    let result_text = cmf_result_text(payload);
                    if let Some(ContentPart::ToolResult { content }) =
                        modified.message.content.iter_mut().find(|part| matches!(part, ContentPart::ToolResult { .. }))
                    {
                        content.content = serde_json::to_value(CallToolResult::success(vec![Content::text(format!(
                            "post:{result_text}"
                        ))]))
                        .expect("tool result serializes");
                    }
                    PluginResult::modify_payload(modified)
                },
                PostBehavior::RewriteRaw => {
                    let mut modified = payload.clone();
                    if let Some(ContentPart::ToolResult { content }) =
                        modified.message.content.iter_mut().find(|part| matches!(part, ContentPart::ToolResult { .. }))
                    {
                        content.content = json!("raw-post");
                    }
                    PluginResult::modify_payload(modified)
                },
                PostBehavior::Deny => PluginResult::deny(
                    PluginViolation::new("post_denied", "post denied")
                        .with_proto_error_code(i64::from(POST_DENY_ERROR_CODE)),
                ),
                PostBehavior::RequireContext => {
                    if ctx.get_global("pre_seen") == Some(&json!(true)) {
                        PluginResult::allow()
                    } else {
                        PluginResult::deny(
                            PluginViolation::new("missing_context", "pre context missing")
                                .with_proto_error_code(i64::from(MISSING_CONTEXT_ERROR_CODE)),
                        )
                    }
                },
            }
        } else {
            match self.pre_behavior {
                PreBehavior::Allow => PluginResult::allow(),
                PreBehavior::Rewrite => {
                    let mut modified = payload.clone();
                    if let Some(ContentPart::ToolCall { content }) =
                        modified.message.content.iter_mut().find(|part| matches!(part, ContentPart::ToolCall { .. }))
                    {
                        "echo".clone_into(&mut content.name);
                        content.arguments = HashMap::from([
                            ("a".to_owned(), json!(REWRITTEN_SUM_A)),
                            ("b".to_owned(), json!(REWRITTEN_SUM_B)),
                        ]);
                    }
                    PluginResult::modify_payload(modified)
                },
                PreBehavior::Deny => PluginResult::deny(
                    PluginViolation::new("pre_denied", "pre denied")
                        .with_proto_error_code(i64::from(PRE_DENY_ERROR_CODE)),
                ),
                PreBehavior::InvalidArgs => {
                    PluginResult::modify_payload(MessagePayload { message: Message::text(Role::User, "invalid") })
                },
                PreBehavior::SetContext => {
                    ctx.set_global("pre_seen", json!(true));
                    PluginResult::allow()
                },
            }
        }
    }
}

fn cmf_result_text(payload: &MessagePayload) -> String {
    payload
        .message
        .get_tool_results()
        .first()
        .and_then(|result| serde_json::from_value::<CallToolResult>(result.content.clone()).ok())
        .map_or_else(|| payload.message.get_text_content(), |result| text(&result))
}

pub(crate) struct TestPluginFactory {
    pub(crate) observations: Arc<Mutex<Observations>>,
    pub(crate) pre_behavior: PreBehavior,
    pub(crate) post_behavior: PostBehavior,
}

impl TestPluginFactory {
    pub(crate) fn from_plugin(plugin: &TestPlugin) -> Self {
        Self {
            observations: Arc::clone(&plugin.observations),
            pre_behavior: plugin.pre_behavior,
            post_behavior: plugin.post_behavior,
        }
    }
}

impl PluginFactory for TestPluginFactory {
    fn create(&self, config: &PluginConfig) -> Result<PluginInstance, Box<PluginError>> {
        let plugin = Arc::new(TestPlugin {
            config: config.clone(),
            observations: Arc::clone(&self.observations),
            pre_behavior: self.pre_behavior,
            post_behavior: self.post_behavior,
        });
        let mut handlers = Vec::new();
        if config.hooks.iter().any(|hook| hook == cmf_hook_names::TOOL_PRE_INVOKE) {
            handlers.push((
                cmf_hook_names::TOOL_PRE_INVOKE,
                Arc::new(TypedHandlerAdapter::<CmfHook, _>::new(Arc::clone(&plugin)))
                    as Arc<dyn cpex_core::registry::AnyHookHandler>,
            ));
        }
        if config.hooks.iter().any(|hook| hook == cmf_hook_names::TOOL_POST_INVOKE) {
            handlers.push((
                cmf_hook_names::TOOL_POST_INVOKE,
                Arc::new(TypedHandlerAdapter::<CmfHook, _>::new(Arc::clone(&plugin)))
                    as Arc<dyn cpex_core::registry::AnyHookHandler>,
            ));
        }
        Ok(PluginInstance { plugin: Arc::<TestPlugin>::clone(&plugin), handlers })
    }
}
