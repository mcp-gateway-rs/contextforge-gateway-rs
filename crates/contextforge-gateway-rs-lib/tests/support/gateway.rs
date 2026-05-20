use std::{
    collections::HashMap,
    sync::{Arc, Mutex as StdMutex, OnceLock},
    time::{Duration, Instant},
};

use contextforge_gateway_rs_apis::{
    User,
    user_store::{BackendMCPGateway, UserConfig, VirtualHost},
};
use contextforge_gateway_rs_cpex::CpexRuntimeRegistry;
use contextforge_gateway_rs_lib::{Config, Gateway, UpstreamConnectionMode, UserConfigStore, UserConfigStoreType};
use futures::FutureExt;
use http::{HeaderMap, HeaderValue};
use rmcp::{
    ErrorData, RoleServer, ServerHandler, ServiceExt,
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
use serde_json::{Map, Value};
use tokio::sync::Mutex as TokioMutex;

use super::{MemoryUserConfigStore, token};

static GATEWAY_PORT_LOCK: OnceLock<Arc<TokioMutex<()>>> = OnceLock::new();
const CLIENT_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const GATEWAY_PORT_READY_TIMEOUT: Duration = Duration::from_secs(10);
const TEST_POLL_INTERVAL: Duration = Duration::from_millis(20);

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
        let deadline = Instant::now() + CLIENT_CONNECT_TIMEOUT;
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
                    tokio::time::sleep(TEST_POLL_INTERVAL).await;
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
    let port_lock = Arc::clone(GATEWAY_PORT_LOCK.get_or_init(|| Arc::new(TokioMutex::new(()))));
    let port_guard = port_lock.lock().await;
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

    let gateway = Gateway::builder()
        .with_config(Config {
            address: Some(format!("127.0.0.1:{gateway_port}").parse().expect("gateway address")),
            token_verification_public_key: Some("../../assets/jwt.key.pub".into()),
            upstream_connection_mode: Some(UpstreamConnectionMode::PlainTextOrTls),
            runtime_plugins_enabled: Some(runtime_plugins_enabled),
            ..Default::default()
        })
        .with_session_manager(Arc::new(LocalSessionManager::default()))
        .with_user_config_store_type(UserConfigStoreType::Test(Arc::new(user_store)))
        .with_plugin_runtime(runtime_plugins_enabled.then(|| plugin_runtime.handle()))
        .build();

    let gateway = async move { gateway.run_gateway().await }.boxed();
    let backend = async move {
        axum::serve(backend_listener, backend_router).await.expect("backend serves");
        Ok(())
    }
    .boxed();

    let handle = tokio::spawn(futures::future::join_all(vec![gateway, backend]));
    wait_for_gateway_port(gateway_port).await;
    drop(port_guard);

    RunningGateway {
        backend_state,
        backend_name,
        gateway_url: format!("http://127.0.0.1:{gateway_port}/contextforge-rs/servers/{virtual_host_id}/mcp"),
        handle: Some(handle),
    }
}

async fn wait_for_gateway_port(port: u16) {
    let deadline = Instant::now() + GATEWAY_PORT_READY_TIMEOUT;
    loop {
        match tokio::net::TcpStream::connect(("127.0.0.1", port)).await {
            Ok(_) => return,
            Err(error) if Instant::now() < deadline => {
                let _ = error;
                tokio::time::sleep(TEST_POLL_INTERVAL).await;
            },
            Err(error) => panic!("gateway TCP listener starts: {error:?}"),
        }
    }
}
