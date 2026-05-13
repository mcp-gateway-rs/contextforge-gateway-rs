#![allow(dead_code, reason = "shared test fixture is used by separate integration test targets")]

use std::{
    collections::HashMap,
    fs,
    sync::{
        Arc, Mutex, Mutex as StdMutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use contextforge_gateway_rs_apis::{
    User,
    user_store::{BackendMCPGateway, UserConfig, VirtualHost},
};
use contextforge_gateway_rs_cpex_runtime::{CpexRuntimeRegistry, GatewayPluginRuntimeError, RuntimePluginConfigStore};
use contextforge_gateway_rs_lib::{Config, ConfigStoreError, Gateway, UpstreamConnectionMode, UserConfigStore};
use cpex_core::{
    cmf::{CmfHook, ContentPart, Message, MessagePayload, Role},
    context::PluginContext,
    error::{PluginError, PluginViolation},
    factory::{PluginFactory, PluginInstance},
    hooks::{Extensions, HookHandler, PluginResult, TypedHandlerAdapter, types::cmf_hook_names},
    plugin::{Plugin, PluginConfig},
};
use futures::FutureExt;
use http::{HeaderMap, HeaderValue};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use rmcp::{
    ErrorData, RoleServer, ServerHandler, ServiceError, ServiceExt,
    model::{
        CallToolRequestParams, CallToolResult, Content, ErrorCode, Implementation, InitializeRequestParams,
        InitializeResult, ServerCapabilities,
    },
    service::RequestContext,
    transport::{
        StreamableHttpClientTransport, StreamableHttpServerConfig, StreamableHttpService,
        streamable_http_client::StreamableHttpClientTransportConfig,
        streamable_http_server::session::local::LocalSessionManager,
    },
};
use serde_json::{Map, Value, json};
use tokio::sync::Mutex as TokioMutex;

#[derive(Default)]
pub(crate) struct Observations {
    pub(crate) pre_calls: usize,
    pub(crate) post_calls: usize,
    pub(crate) shutdown_calls: usize,
    pub(crate) pre_payload_name: Option<String>,
    pub(crate) post_payload_name: Option<String>,
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

    pub(crate) fn with_pre_rewrite(mut self) -> Self {
        self.pre_behavior = PreBehavior::Rewrite;
        self
    }

    pub(crate) fn with_post_rewrite(mut self) -> Self {
        self.post_behavior = PostBehavior::Rewrite;
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
            observations.post_payload_name =
                payload.message.get_tool_results().first().map(|result| result.tool_name.clone());
            observations.post_result_text = Some(cmf_result_text(payload));
        } else {
            observations.pre_calls += 1;
            observations.pre_payload_name = payload.message.get_tool_calls().first().map(|call| call.name.clone());
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
                PostBehavior::Deny => {
                    PluginResult::deny(PluginViolation::new("post_denied", "post denied").with_proto_error_code(-32002))
                },
                PostBehavior::RequireContext => {
                    if ctx.get_global("pre_seen") == Some(&json!(true)) {
                        PluginResult::allow()
                    } else {
                        PluginResult::deny(
                            PluginViolation::new("missing_context", "pre context missing")
                                .with_proto_error_code(-32003),
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
                        content.arguments = HashMap::from([("a".to_owned(), json!(10)), ("b".to_owned(), json!(20))]);
                    }
                    PluginResult::modify_payload(modified)
                },
                PreBehavior::Deny => {
                    PluginResult::deny(PluginViolation::new("pre_denied", "pre denied").with_proto_error_code(-32001))
                },
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

#[derive(Clone, Default)]
struct MemoryUserConfigStore {
    configs: Arc<TokioMutex<HashMap<String, UserConfig>>>,
}

#[async_trait]
impl UserConfigStore for MemoryUserConfigStore {
    async fn get_config<'a>(&self, key: &'a User<'a>) -> Result<UserConfig, ConfigStoreError> {
        self.configs.lock().await.get(key.key()).cloned().ok_or(ConfigStoreError::NoDataForKey)
    }

    async fn set_config<'a>(&self, key: &'a User<'a>, config: &'a UserConfig) -> Result<(), ConfigStoreError> {
        self.configs.lock().await.insert(key.key().to_owned(), config.clone());
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct BackendObservation {
    pub(crate) tool_name: String,
    pub(crate) args: Option<Map<String, Value>>,
}

#[derive(Clone, Default)]
pub(crate) struct BackendState {
    pub(crate) calls: Arc<StdMutex<Vec<BackendObservation>>>,
}

#[derive(Clone)]
struct TestBackend {
    state: BackendState,
}

impl ServerHandler for TestBackend {
    async fn initialize(
        &self,
        _request: InitializeRequestParams,
        _cx: RequestContext<RoleServer>,
    ) -> Result<InitializeResult, ErrorData> {
        Ok(InitializeResult::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("test-backend", "0.1.0")))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        self.state
            .calls
            .lock()
            .expect("backend calls lock poisoned")
            .push(BackendObservation { tool_name: request.name.to_string(), args: request.arguments.clone() });

        match request.name.as_ref() {
            "sum" => {
                let args = request
                    .arguments
                    .as_ref()
                    .ok_or_else(|| ErrorData::invalid_params("sum requires arguments", None))?;
                let a = args
                    .get("a")
                    .and_then(Value::as_i64)
                    .ok_or_else(|| ErrorData::invalid_params("sum requires numeric a", None))?;
                let b = args
                    .get("b")
                    .and_then(Value::as_i64)
                    .ok_or_else(|| ErrorData::invalid_params("sum requires numeric b", None))?;
                Ok(CallToolResult::success(vec![Content::text((a + b).to_string())]))
            },
            _ => Err(ErrorData {
                code: ErrorCode::METHOD_NOT_FOUND,
                message: format!("unknown tool {}", request.name).into(),
                data: None,
            }),
        }
    }
}

pub(crate) struct RunningGateway {
    pub(crate) backend_state: BackendState,
    pub(crate) backend_name: String,
    gateway_url: String,
    handle: Option<tokio::task::JoinHandle<Vec<contextforge_gateway_rs_lib::Result<()>>>>,
}

impl RunningGateway {
    pub(crate) async fn connect(
        &self,
        user: &str,
    ) -> rmcp::service::RunningService<rmcp::RoleClient, InitializeRequestParams> {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let mut headers = HeaderMap::new();
            headers.insert(
                http::header::AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {}", token(user))).expect("valid auth header"),
            );
            let client = reqwest::Client::builder().default_headers(headers).build().expect("client builds");
            let transport = StreamableHttpClientTransport::with_client(
                client,
                StreamableHttpClientTransportConfig::with_uri(self.gateway_url.clone()),
            );
            match InitializeRequestParams::default().serve(transport).await {
                Ok(service) => return service,
                Err(error) if Instant::now() < deadline => {
                    let _ = error;
                    tokio::time::sleep(Duration::from_millis(20)).await;
                },
                Err(error) => panic!("gateway service starts: {error:?}"),
            }
        }
    }
}

impl Drop for RunningGateway {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

pub(crate) async fn start_gateway(
    user: &str,
    runtime_plugins_enabled: bool,
    plugin_runtime: Arc<CpexRuntimeRegistry>,
) -> RunningGateway {
    start_gateway_with_runtime(user, runtime_plugins_enabled, plugin_runtime).await
}

async fn start_gateway_with_runtime(
    user: &str,
    runtime_plugins_enabled: bool,
    plugin_runtime: Arc<CpexRuntimeRegistry>,
) -> RunningGateway {
    let gateway_port = openport::pick_random_unused_port().expect("gateway port");
    let backend_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("backend binds");
    let backend_port = backend_listener.local_addr().expect("backend address").port();
    let backend_name = format!("backend-{backend_port}");
    let virtual_host_id = "vh-cpex-test";
    let backend_state = BackendState::default();

    let backend_service = StreamableHttpService::new(
        {
            let backend_state = backend_state.clone();
            move || Ok(TestBackend { state: backend_state.clone() })
        },
        LocalSessionManager::default().into(),
        StreamableHttpServerConfig::default(),
    );
    let backend_router = axum::Router::new().route_service("/mcp", backend_service);

    let user_store = MemoryUserConfigStore::default();
    user_store
        .set_config(
            &User::new(user),
            &UserConfig {
                virtual_hosts: HashMap::from([(
                    virtual_host_id.to_owned(),
                    VirtualHost {
                        backends: HashMap::from([(
                            backend_name.clone(),
                            BackendMCPGateway {
                                url: format!("http://127.0.0.1:{backend_port}/mcp").parse().expect("backend URL"),
                            },
                        )]),
                    },
                )]),
            },
        )
        .await
        .expect("user config is stored");

    let gateway_plugin_runtime: Arc<dyn contextforge_gateway_rs_lib::GatewayToolRuntime> =
        Arc::<CpexRuntimeRegistry>::clone(&plugin_runtime);
    let gateway = Gateway::builder()
        .with_config(Config {
            address: Some(format!("127.0.0.1:{gateway_port}").parse().expect("gateway address")),
            token_verification_public_key: "../../assets/jwt.key.pub".into(),
            upstream_connection_mode: Some(UpstreamConnectionMode::PlainTextOrTls),
            runtime_plugins_enabled: Some(runtime_plugins_enabled),
            ..Default::default()
        })
        .with_user_config_store(Arc::new(user_store))
        .with_session_manager(Arc::new(LocalSessionManager::default()))
        .with_plugin_runtime(gateway_plugin_runtime)
        .build();

    let gateway = async move { gateway.run_gateway().await }.boxed();
    let backend = async move {
        axum::serve(backend_listener, backend_router).await.expect("backend serves");
        Ok(())
    }
    .boxed();

    RunningGateway {
        backend_state,
        backend_name,
        gateway_url: format!("http://127.0.0.1:{gateway_port}/contextforge-rs/servers/{virtual_host_id}/mcp"),
        handle: Some(tokio::spawn(futures::future::join_all(vec![gateway, backend]))),
    }
}

#[derive(Clone, Default)]
pub(crate) struct MemoryRuntimePluginConfigStore {
    config: Arc<TokioMutex<Option<Value>>>,
    calls: Arc<AtomicUsize>,
}

impl MemoryRuntimePluginConfigStore {
    pub(crate) fn with_config(config: Value) -> Self {
        Self { config: Arc::new(TokioMutex::new(Some(config))), calls: Arc::new(AtomicUsize::new(0)) }
    }

    pub(crate) async fn set_config(&self, config: Value) {
        *self.config.lock().await = Some(config);
    }

    pub(crate) async fn clear_config(&self) {
        *self.config.lock().await = None;
    }

    pub(crate) fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl RuntimePluginConfigStore for MemoryRuntimePluginConfigStore {
    async fn get_config(&self) -> Result<Option<Value>, GatewayPluginRuntimeError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.config.lock().await.clone())
    }
}

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

fn token(user_id: &str) -> String {
    let key = EncodingKey::from_rsa_pem(&fs::read("../../assets/jwt.key").expect("jwt key")).expect("encoding key");
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some("test".to_owned());
    let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("system clock").as_secs();
    let claims = json!({
        "iss": "http://contextforge-gateway-rs",
        "sub": user_id,
        "aud": "mcp-audience",
        "exp": now + 3600,
        "iat": now,
        "userinfo": { "sub": user_id },
    });
    encode(&header, &claims, &key).expect("jwt token")
}

pub(crate) fn runtime_with_pre(plugin: Arc<TestPlugin>) -> Arc<CpexRuntimeRegistry> {
    runtime_with_plugins(&[plugin])
}

pub(crate) fn runtime_with_post(plugin: Arc<TestPlugin>) -> Arc<CpexRuntimeRegistry> {
    runtime_with_plugins(&[plugin])
}

pub(crate) fn runtime_with_pre_and_post(
    pre_plugin: Arc<TestPlugin>,
    post_plugin: Arc<TestPlugin>,
) -> Arc<CpexRuntimeRegistry> {
    runtime_with_plugins(&[pre_plugin, post_plugin])
}

pub(crate) fn runtime_with_plugins(plugins: &[Arc<TestPlugin>]) -> Arc<CpexRuntimeRegistry> {
    let config_store = MemoryRuntimePluginConfigStore::with_config(json!({
        "version": 1,
        "cpex": {
            "plugins": plugins.iter().enumerate().map(|(index, plugin)| {
                json!({
                    "name": plugin.config.name.clone(),
                    "kind": format!("test-{index}"),
                    "hooks": plugin.config.hooks.clone(),
                })
            }).collect::<Vec<_>>()
        }
    }));
    let mut runtime = CpexRuntimeRegistry::with_config_store(Arc::new(config_store));
    for (index, plugin) in plugins.iter().enumerate() {
        runtime.register_factory(format!("test-{index}"), Box::new(TestPluginFactory::from_plugin(plugin)));
    }
    Arc::new(runtime)
}

pub(crate) fn plugin_config(plugins: &[Arc<TestPlugin>]) -> Value {
    json!({
        "version": 1,
        "cpex": {
            "plugins": plugins.iter().map(|plugin| {
                json!({
                    "name": plugin.config.name.clone(),
                    "kind": plugin.config.kind.clone(),
                    "hooks": plugin.config.hooks.clone(),
                })
            }).collect::<Vec<_>>()
        }
    })
}
