mod support;

use std::{
    collections::HashMap,
    fs::File,
    io::Read,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use contextforge_gateway_rs_apis::{
    User,
    user_store::{BackendMCPGateway, UserConfig, VirtualHost},
};
use futures::{FutureExt, future::BoxFuture};
use http::{HeaderMap, HeaderValue, StatusCode};
use rmcp::{
    ClientHandler, ServiceExt,
    model::{
        CallToolRequest, CallToolRequestParams, CallToolResult, CancelledNotificationParam, ClientInfo, ClientRequest,
        InitializeRequestParams, LoggingLevel, LoggingMessageNotificationParam, ProgressNotificationParam,
        ResourceUpdatedNotificationParam, ServerResult, SetLevelRequestParams, SubscribeRequestParams,
        UnsubscribeRequestParams,
    },
    service::{NotificationContext, PeerRequestOptions, RoleClient, RunningService, ServiceError},
    transport::{
        StreamableHttpClientTransport, StreamableHttpServerConfig, StreamableHttpService,
        streamable_http_client::StreamableHttpClientTransportConfig,
        streamable_http_server::session::local::LocalSessionManager,
    },
};
use rustls::crypto::{self};
use serde_json::{Value, json};
use tracing::{info, warn};

use contextforge_gateway_rs_lib::{
    Config, Gateway, Result, UpstreamConnectionMode, UserConfigStore, UserConfigStoreType,
};
use support::{MemoryUserConfigStore, mock_counter, token};

const MOCK_COUNTER_TOOL_NAMES: &[&str] = &[
    "cancellable_task",
    "decrement",
    "echo",
    "get_session_id",
    "get_value",
    "increment",
    "logging_task",
    "long_task",
    "notification_task",
    "progress_task",
    "say_hello",
    "sum",
];
const UPSTREAM_CONTROL_SETTLE_DURATION: Duration = Duration::from_millis(600);

#[derive(Clone, Debug)]
struct NotificationCapturingClient {
    notifications: CapturedNotifications,
}

type CapturedNotifications = Arc<Mutex<Vec<CapturedNotification>>>;

#[derive(Clone, Debug)]
enum CapturedNotification {
    Logging(Value),
    Progress(ProgressNotificationParam),
    ResourceUpdated(String),
    ResourceListChanged,
    ToolListChanged,
}

impl ClientHandler for NotificationCapturingClient {
    fn get_info(&self) -> ClientInfo {
        ClientInfo::default()
    }

    async fn on_progress(&self, params: ProgressNotificationParam, _context: NotificationContext<RoleClient>) {
        self.notifications
            .lock()
            .expect("notification lock should not be poisoned")
            .push(CapturedNotification::Progress(params));
    }

    async fn on_logging_message(
        &self,
        params: LoggingMessageNotificationParam,
        _context: NotificationContext<RoleClient>,
    ) {
        self.notifications
            .lock()
            .expect("notification lock should not be poisoned")
            .push(CapturedNotification::Logging(params.data));
    }

    async fn on_resource_updated(
        &self,
        params: ResourceUpdatedNotificationParam,
        _context: NotificationContext<RoleClient>,
    ) {
        self.notifications
            .lock()
            .expect("notification lock should not be poisoned")
            .push(CapturedNotification::ResourceUpdated(params.uri));
    }

    async fn on_resource_list_changed(&self, _context: NotificationContext<RoleClient>) {
        self.notifications
            .lock()
            .expect("notification lock should not be poisoned")
            .push(CapturedNotification::ResourceListChanged);
    }

    async fn on_tool_list_changed(&self, _context: NotificationContext<RoleClient>) {
        self.notifications
            .lock()
            .expect("notification lock should not be poisoned")
            .push(CapturedNotification::ToolListChanged);
    }
}

async fn notification_client(
    gateway_url: String,
    client: reqwest::Client,
) -> Result<(RunningService<RoleClient, NotificationCapturingClient>, CapturedNotifications)> {
    let notifications = Arc::new(Mutex::new(Vec::new()));
    let service = NotificationCapturingClient { notifications: Arc::clone(&notifications) }
        .serve(StreamableHttpClientTransport::with_client(
            client,
            StreamableHttpClientTransportConfig::with_uri(gateway_url),
        ))
        .await?;
    Ok((service, notifications))
}

fn clear_notifications(notifications: &CapturedNotifications) {
    notifications.lock().expect("notification lock should not be poisoned").clear();
}

fn notifications_contain<F>(notifications: &CapturedNotifications, predicate: F) -> bool
where
    F: FnMut(&CapturedNotification) -> bool,
{
    notifications.lock().expect("notification lock should not be poisoned").iter().any(predicate)
}

fn notifications_count<F>(notifications: &CapturedNotifications, mut predicate: F) -> usize
where
    F: FnMut(&CapturedNotification) -> bool,
{
    notifications
        .lock()
        .expect("notification lock should not be poisoned")
        .iter()
        .filter(|event| predicate(event))
        .count()
}

async fn eventually<F>(timeout: Duration, mut condition: F) -> bool
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + timeout;
    loop {
        if condition() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn notifications_absent_for<F>(timeout: Duration, notifications: &CapturedNotifications, mut predicate: F) -> bool
where
    F: FnMut(&CapturedNotification) -> bool,
{
    let deadline = Instant::now() + timeout;
    loop {
        if notifications_contain(notifications, &mut predicate) {
            return false;
        }
        if Instant::now() >= deadline {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn find_tool(expected_tool_names: &[String], suffix: &str) -> Result<String> {
    expected_tool_names
        .iter()
        .find(|name| name.ends_with(suffix))
        .cloned()
        .ok_or_else(|| format!("Missing {suffix} tool").into())
}

async fn call_progress_tool(
    service: &RunningService<RoleClient, NotificationCapturingClient>,
    notifications: &CapturedNotifications,
    progress_tool: &str,
) -> Result<()> {
    let request =
        ClientRequest::CallToolRequest(CallToolRequest::new(CallToolRequestParams::new(progress_tool.to_owned())));
    let request_handle = service.send_cancellable_request(request, PeerRequestOptions::no_options()).await?;
    let expected_progress_token = request_handle.progress_token.clone();
    let saw_progress = eventually(Duration::from_secs(1), || {
        notifications_contain(notifications, |event| {
            matches!(
                event,
                CapturedNotification::Progress(params)
                    if params.message.as_deref() == Some("progress from backend")
                        && params.progress_token == expected_progress_token
            )
        })
    })
    .await;
    let result = request_handle.await_response().await?;
    if saw_progress && server_result_contains_text(&result, "Progress task completed") {
        Ok(())
    } else {
        Err(format!("Expected progress notification, got {result:?} {:?}", notifications.lock()).into())
    }
}

fn find_first_tools(expected_tool_names: &[String], suffix: &str, count: usize) -> Result<Vec<String>> {
    let tools =
        expected_tool_names.iter().filter(|name| name.ends_with(suffix)).take(count).cloned().collect::<Vec<_>>();
    if tools.len() == count {
        Ok(tools)
    } else {
        Err(format!("Expected {count} {suffix} tools, got {}", tools.len()).into())
    }
}

fn call_tool_result_contains_text(result: &CallToolResult, expected: &str) -> bool {
    result.content.iter().any(|content| content.as_text().is_some_and(|text| text.text == expected))
}

fn server_result_contains_text(result: &ServerResult, expected: &str) -> bool {
    match result {
        ServerResult::CallToolResult(result) => call_tool_result_contains_text(result, expected),
        _ => false,
    }
}

fn client_for_user(user: &str) -> Result<reqwest::Client> {
    let mut default_headers = HeaderMap::new();
    let token = token(user);
    default_headers.insert(http::header::AUTHORIZATION, HeaderValue::from_str(&format!("Bearer {token}"))?);
    Ok(reqwest::Client::builder().default_headers(default_headers).build()?)
}

async fn raw_initialize_session(gateway_url: &str, user: &str) -> Result<String> {
    let response = client_for_user(user)?
        .post(gateway_url)
        .header(http::header::ACCEPT, "application/json, text/event-stream")
        .header(http::header::CONTENT_TYPE, "application/json")
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {
                    "name": "raw-test-client",
                    "version": "0.1.0"
                }
            }
        }))
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(format!("initialize failed with {}", response.status()).into());
    }
    Ok(response.headers().get("Mcp-Session-Id").ok_or("initialize response missing session id")?.to_str()?.to_owned())
}

async fn raw_ping_with_session(gateway_url: String, user: &str, session_id: String) -> Result<StatusCode> {
    Ok(client_for_user(user)?
        .post(gateway_url)
        .header(http::header::ACCEPT, "application/json, text/event-stream")
        .header(http::header::CONTENT_TYPE, "application/json")
        .header("Mcp-Session-Id", session_id)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "ping"
        }))
        .send()
        .await?
        .status())
}

async fn raw_delete_session(gateway_url: String, user: &str, session_id: String) -> Result<StatusCode> {
    Ok(client_for_user(user)?.delete(gateway_url).header("Mcp-Session-Id", session_id).send().await?.status())
}

async fn raw_sse_get(gateway_url: String, user: &str, session_id: String) -> Result<StatusCode> {
    Ok(client_for_user(user)?
        .get(gateway_url)
        .header(http::header::ACCEPT, "text/event-stream")
        .header("Mcp-Session-Id", session_id)
        .send()
        .await?
        .status())
}

fn create_ports(ports: usize) -> Vec<u16> {
    (0..ports).map(|_| openport::pick_random_unused_port().expect("Expecting to find port")).collect()
}

fn create_backends(ports: &[u16], with_tls: bool) -> HashMap<String, BackendMCPGateway> {
    ports
        .iter()
        .map(|port| {
            let url = if with_tls {
                format!("https://127.0.0.1:{port}/mcp").parse().expect("This should work")
            } else {
                format!("http://127.0.0.1:{port}/mcp").parse().expect("This should work")
            };

            (format!("backend-{port}"), BackendMCPGateway { url })
        })
        .collect::<HashMap<_, _>>()
}

fn create_tool_names(ports: &[u16]) -> Vec<String> {
    ports
        .iter()
        .flat_map(|port| MOCK_COUNTER_TOOL_NAMES.iter().map(move |name| format!("backend-{port}-{name}")))
        .collect::<Vec<_>>()
}

fn create_axum_servers(ports: &[u16], router: &axum::Router) -> Vec<BoxFuture<'static, Result<()>>> {
    ports
        .iter()
        .map(|port| {
            let addr = format!("127.0.0.1:{port}");
            let router = router.clone();
            async {
                let listener = tokio::net::TcpListener::bind(addr).await.expect("Expect this to work");
                axum::serve(listener, router).await.expect("server runs");
                Ok(())
            }
            .boxed()
        })
        .collect()
}

async fn create_axum_tls_servers(ports: &[u16], router: axum::Router) -> Vec<BoxFuture<'static, Result<()>>> {
    let config = axum_server::tls_rustls::RustlsConfig::from_pem_file(
        "../../assets/contextforgeCA/contextforge-server.cert.pem",
        "../../assets/contextforgeCA/contextforge-server.key.pem",
    )
    .await
    .expect("Expect this to work");

    ports
        .iter()
        .map(|port| {
            let router = router.clone();
            let addr: SocketAddr = format!("127.0.0.1:{port}").parse().expect("Expect this to work");
            let config = config.clone();
            async move {
                //let listener = tokio::net::TcpListener::bind(addr).await.expect("Expect this to work");
                _ = axum_server::bind_rustls(addr, config).serve(router.into_make_service()).await;
                Ok(())
            }
            .boxed()
        })
        .collect()
}

struct TestSettings {
    handle: tokio::task::JoinHandle<Vec<Result<()>>>,
    gateway_url: String,
    second_gateway_url: String,
    expected_tool_names: Vec<String>,
}

async fn create_gateway_with_four_counters(user: &str, config: Config) -> Result<TestSettings> {
    let mocked_user_config_store = MemoryUserConfigStore::default();

    let gateway_one_ports = create_ports(2);
    let gateway_two_ports = create_ports(2);

    let service = StreamableHttpService::new(
        || Ok(mock_counter::Counter::new()),
        LocalSessionManager::default().into(),
        StreamableHttpServerConfig::default(),
    );

    let router = axum::Router::new().route_service("/mcp", service);

    assert_ne!(gateway_one_ports, gateway_two_ports);

    let gateway_one_backends = create_backends(&gateway_one_ports, false);
    let gateway_two_backends = create_backends(&gateway_two_ports, false);

    let mut virtual_host_one_tool_names = create_tool_names(&gateway_one_ports);
    let mut virtual_host_two_tool_names = create_tool_names(&gateway_two_ports);
    virtual_host_one_tool_names.sort();
    virtual_host_two_tool_names.sort();

    let user_key = User::new(user);

    let virtual_host_one_id = uuid::Uuid::new_v4().to_string();
    let virtual_host_two_id = uuid::Uuid::new_v4().to_string();

    let virtual_hosts = HashMap::from([
        (virtual_host_one_id.clone(), VirtualHost { backends: gateway_one_backends }),
        (virtual_host_two_id.clone(), VirtualHost { backends: gateway_two_backends }),
    ]);

    let user_config = UserConfig { virtual_hosts };

    mocked_user_config_store.set_config(&user_key, &user_config).await.expect("This should work");

    let gateway = Gateway::builder()
        .with_config(config.clone())
        //.with_user_config_store(Arc::new(mocked_user_config_store))
        .with_session_manager(Arc::new(LocalSessionManager::default()))
        .with_user_config_store_type(UserConfigStoreType::Test(Arc::new(mocked_user_config_store)))
        .with_upstream_notification_control_timeout(Some(Duration::from_millis(100)))
        .build();

    let gateway = async move {
        let res = gateway.run_gateway().await;
        warn!("Gateway exited with result {res:?}");
        Ok(())
    }
    .boxed();

    if let Some(address) = config.address.as_ref() {
        let gateway_url = format!("http://{address}/contextforge-rs/servers/{virtual_host_one_id}/mcp");
        let second_gateway_url = format!("http://{address}/contextforge-rs/servers/{virtual_host_two_id}/mcp");

        let servers_one = create_axum_servers(&gateway_one_ports, &router);
        let servers_two = create_axum_servers(&gateway_two_ports, &router);
        let handle: tokio::task::JoinHandle<Vec<Result<()>>> = tokio::spawn(futures::future::join_all(
            vec![gateway].into_iter().chain(servers_one).chain(servers_two), //.chain(vec![test_future].into_iter()),
        ));

        Ok(TestSettings { handle, gateway_url, second_gateway_url, expected_tool_names: virtual_host_one_tool_names })
    } else {
        Err("Invalid configuration".into())
    }
}

async fn create_tls_gateway_with_four_tls_counters(user: &str, config: Config) -> Result<TestSettings> {
    let mocked_user_config_store = MemoryUserConfigStore::default();

    let gateway_one_ports = create_ports(2);
    let gateway_two_ports = create_ports(2);

    let service = StreamableHttpService::new(
        || Ok(mock_counter::Counter::new()),
        LocalSessionManager::default().into(),
        StreamableHttpServerConfig::default().disable_allowed_hosts().disable_allowed_origins(),
    );

    let router = axum::Router::new().route_service("/mcp", service);

    assert_ne!(gateway_one_ports, gateway_two_ports);

    let gateway_one_backends = create_backends(&gateway_one_ports, true);
    let gateway_two_backends = create_backends(&gateway_two_ports, true);

    let mut virtual_host_one_tool_names = create_tool_names(&gateway_one_ports);
    let mut virtual_host_two_tool_names = create_tool_names(&gateway_two_ports);
    virtual_host_one_tool_names.sort();
    virtual_host_two_tool_names.sort();

    let user_key = User::new(user);

    let virtual_host_one_id = uuid::Uuid::new_v4().to_string();
    let virtual_host_two_id = uuid::Uuid::new_v4().to_string();

    let virtual_hosts = HashMap::from([
        (virtual_host_one_id.clone(), VirtualHost { backends: gateway_one_backends }),
        (virtual_host_two_id.clone(), VirtualHost { backends: gateway_two_backends }),
    ]);

    let user_config = UserConfig { virtual_hosts };

    mocked_user_config_store.set_config(&user_key, &user_config).await.expect("This should work");

    let gateway = Gateway::builder()
        .with_config(config.clone())
        //.with_user_config_store(Arc::new(mocked_user_config_store))
        .with_session_manager(Arc::new(LocalSessionManager::default()))
        .with_user_config_store_type(UserConfigStoreType::Test(Arc::new(mocked_user_config_store)))
        .build();

    let gateway = async move {
        let res = gateway.run_gateway().await;
        warn!("Gateway exited with result {res:?}");
        Ok(())
    }
    .boxed();

    if let Some(address) = config.tls_address.as_ref() {
        let gateway_url = format!("https://{address}/contextforge-rs/servers/{virtual_host_one_id}/mcp");
        let second_gateway_url = format!("https://{address}/contextforge-rs/servers/{virtual_host_two_id}/mcp");

        let servers_one = create_axum_tls_servers(&gateway_one_ports, router.clone()).await;
        let servers_two = create_axum_tls_servers(&gateway_two_ports, router.clone()).await;
        let handle: tokio::task::JoinHandle<Vec<Result<()>>> =
            tokio::spawn(futures::future::join_all(vec![gateway].into_iter().chain(servers_one).chain(servers_two)));

        Ok(TestSettings { handle, gateway_url, second_gateway_url, expected_tool_names: virtual_host_one_tool_names })
    } else {
        Err("Invalid configuration".into())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[test_log::test]
async fn plaintext_list_tools_end_to_end_test() -> Result<()> {
    let gateway_port = create_ports(1)[0];

    let config = Config {
        address: Some(format!("127.0.0.1:{gateway_port}").parse().expect("This should work")),
        token_verification_public_key: Some("../../assets/jwt.key.pub".into()),
        upstream_connection_mode: Some(UpstreamConnectionMode::PlainTextOrTls),
        ..Default::default()
    };

    let user = "admin@example.com";

    let Ok(TestSettings { handle, gateway_url, expected_tool_names, .. }) =
        create_gateway_with_four_counters(user, config).await
    else {
        panic!("Invalid configuration ");
    };

    let test_future: BoxFuture<'_, Result<()>> = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let client = client_for_user(user)?;

        info!("Seding request to {gateway_url}");

        let config = StreamableHttpClientTransportConfig::with_uri(gateway_url);
        let transport = StreamableHttpClientTransport::with_client(client, config);
        let request = InitializeRequestParams::default();

        let maybe_service = request.serve(transport).await;
        let Ok(running_service) = maybe_service else {
            warn!("No Service {maybe_service:?}");
            return Err("Couldn't get a service".into());
        };

        let list_tools = running_service.list_tools(None).await;
        let Ok(list_tools) = list_tools else {
            let msg = format!("List tools returned error  {list_tools:?}");
            warn!(msg);
            return Err(msg.into());
        };

        let mut names: Vec<String> = list_tools.tools.iter().map(|t| t.name.to_string()).collect();
        names.sort();

        info!("Tool names {names:#?}");
        if expected_tool_names != names {
            warn!("Actual {names:#?} Expected {expected_tool_names:#?}");
            return Err("Expected tool names don't match actual".into());
        }

        Ok(())
    }
    .boxed();

    let maybe_passed = test_future.await;

    handle.abort();
    if maybe_passed.is_ok() {
        info!("Test passed");
    } else {
        info!("Test NOT passed {maybe_passed:?}");
        panic!()
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[test_log::test]
async fn plaintext_sse_get_requires_session_owner_scope() -> Result<()> {
    let gateway_port = create_ports(1)[0];

    let config = Config {
        address: Some(format!("127.0.0.1:{gateway_port}").parse().expect("This should work")),
        token_verification_public_key: Some("../../assets/jwt.key.pub".into()),
        upstream_connection_mode: Some(UpstreamConnectionMode::PlainTextOrTls),
        ..Default::default()
    };

    let user = "admin@example.com";
    let TestSettings { handle, gateway_url, second_gateway_url, .. } =
        create_gateway_with_four_counters(user, config).await.expect("valid plaintext gateway configuration");

    let test_future: BoxFuture<'_, Result<()>> = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let session_id = raw_initialize_session(&gateway_url, user).await?;
        let cross_virtual_host_get = client_for_user(user)?
            .get(second_gateway_url.clone())
            .header(http::header::ACCEPT, "text/event-stream")
            .header("Mcp-Session-Id", session_id.clone())
            .send()
            .await?;
        if cross_virtual_host_get.status() != StatusCode::NOT_FOUND {
            return Err(format!("cross-virtual-host SSE attach returned {}", cross_virtual_host_get.status()).into());
        }

        let cross_principal_get = client_for_user("other-admin@example.com")?
            .get(gateway_url.clone())
            .header(http::header::ACCEPT, "text/event-stream")
            .header("Mcp-Session-Id", session_id.clone())
            .send()
            .await?;
        if cross_principal_get.status() != StatusCode::NOT_FOUND {
            return Err(format!("cross-principal SSE attach returned {}", cross_principal_get.status()).into());
        }

        let cross_virtual_host_post =
            raw_ping_with_session(second_gateway_url.clone(), user, session_id.clone()).await?;
        if cross_virtual_host_post != StatusCode::NOT_FOUND {
            return Err(format!("cross-virtual-host POST returned {cross_virtual_host_post}").into());
        }

        let cross_principal_post =
            raw_ping_with_session(gateway_url.clone(), "other-admin@example.com", session_id.clone()).await?;
        if cross_principal_post != StatusCode::NOT_FOUND {
            return Err(format!("cross-principal POST returned {cross_principal_post}").into());
        }

        let cross_virtual_host_delete = raw_delete_session(second_gateway_url, user, session_id.clone()).await?;
        if cross_virtual_host_delete != StatusCode::NOT_FOUND {
            return Err(format!("cross-virtual-host DELETE returned {cross_virtual_host_delete}").into());
        }

        let cross_principal_delete =
            raw_delete_session(gateway_url.clone(), "other-admin@example.com", session_id.clone()).await?;
        if cross_principal_delete != StatusCode::NOT_FOUND {
            return Err(format!("cross-principal DELETE returned {cross_principal_delete}").into());
        }

        let owner_get = raw_sse_get(gateway_url.clone(), user, session_id.clone()).await?;
        if !owner_get.is_success() {
            return Err(format!("owner SSE attach returned {owner_get}").into());
        }

        let owner_ping = raw_ping_with_session(gateway_url.clone(), user, session_id.clone()).await?;
        if !owner_ping.is_success() {
            return Err(format!("owner POST returned {owner_ping}").into());
        }

        let owner_delete = raw_delete_session(gateway_url.clone(), user, session_id.clone()).await?;
        if !owner_delete.is_success() {
            return Err(format!("owner DELETE returned {owner_delete}").into());
        }

        let deleted_ping = raw_ping_with_session(gateway_url.clone(), user, session_id.clone()).await?;
        if deleted_ping != StatusCode::NOT_FOUND {
            return Err(format!("deleted-session POST returned {deleted_ping}").into());
        }

        let deleted_get = raw_sse_get(gateway_url, user, session_id).await?;
        if deleted_get != StatusCode::NOT_FOUND {
            return Err(format!("deleted-session SSE attach returned {deleted_get}").into());
        }

        Ok(())
    }
    .boxed();

    let maybe_passed = test_future.await;
    handle.abort();
    maybe_passed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[test_log::test]
async fn plaintext_session_notifications_forward_subscribed_resources_and_enabled_logs() -> Result<()> {
    let gateway_port = create_ports(1)[0];

    let config = Config {
        address: Some(format!("127.0.0.1:{gateway_port}").parse().expect("This should work")),
        token_verification_public_key: Some("../../assets/jwt.key.pub".into()),
        upstream_connection_mode: Some(UpstreamConnectionMode::PlainTextOrTls),
        ..Default::default()
    };

    let user = "admin@example.com";
    let TestSettings { handle, gateway_url, expected_tool_names, .. } =
        create_gateway_with_four_counters(user, config).await.expect("valid plaintext gateway configuration");

    let test_future: BoxFuture<'_, Result<()>> = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let client = client_for_user(user)?;
        let (service, notifications) = notification_client(gateway_url.clone(), client.clone()).await?;

        let notification_tools = find_first_tools(&expected_tool_names, "-notification_task", 2)?;
        let notification_tool = notification_tools[0].clone();
        let second_notification_tool = notification_tools[1].clone();
        let backend_name = notification_tool.strip_suffix("-notification_task").expect("tool suffix should match");
        let second_backend_name =
            second_notification_tool.strip_suffix("-notification_task").expect("tool suffix should match");
        let logging_tool = format!("{backend_name}-logging_task");
        let progress_tool = format!("{backend_name}-progress_task");
        let resource_uri = format!("{backend_name}-memo://insights");
        let second_resource_uri = format!("{second_backend_name}-memo://insights");

        let saw_initialized_tool_list = eventually(Duration::from_secs(1), || {
            notifications_contain(&notifications, |event| matches!(event, CapturedNotification::ToolListChanged))
        })
        .await;
        if !saw_initialized_tool_list {
            return Err("Expected backend tool list notification after initialize".into());
        }
        clear_notifications(&notifications);

        service.set_level(SetLevelRequestParams::new(LoggingLevel::Info)).await?;
        let cancellable_tool = find_tool(&expected_tool_names, "-cancellable_task")?;
        let long_request =
            ClientRequest::CallToolRequest(CallToolRequest::new(CallToolRequestParams::new(cancellable_tool)));
        let long_handle = service.send_cancellable_request(long_request, PeerRequestOptions::no_options()).await?;
        let cancelled_progress_token = long_handle.progress_token.clone();
        let cancel_peer = long_handle.peer.clone();
        let cancel_request_id = long_handle.id.clone();
        let list_tools_while_call_is_active = service.list_tools(None).await?;
        let mut names =
            list_tools_while_call_is_active.tools.iter().map(|tool| tool.name.to_string()).collect::<Vec<_>>();
        names.sort();
        cancel_peer
            .notify_cancelled(CancelledNotificationParam {
                request_id: cancel_request_id,
                reason: Some("concurrency check complete".to_owned()),
            })
            .await?;
        if names != expected_tool_names {
            return Err("Concurrent same-session list_tools returned partial backend data".into());
        }
        match long_handle.await_response().await {
            Err(ServiceError::Cancelled { reason }) if reason.as_deref() == Some("concurrency check complete") => {},
            other => return Err(format!("Cancelled downstream request returned {other:?}").into()),
        }
        let saw_backend_cancellation = eventually(Duration::from_secs(1), || {
            notifications_contain(
                &notifications,
                |event| matches!(event, CapturedNotification::Logging(data) if data == &json!("Cancellable task cancelled")),
            )
        })
        .await;
        if !saw_backend_cancellation {
            return Err(format!("Expected backend cancellation notification, got {:?}", notifications.lock()).into());
        }
        if !notifications_absent_for(Duration::from_millis(100), &notifications, |event| {
            matches!(
                event,
                CapturedNotification::Progress(params)
                    if params.message.as_deref() == Some("cancelled progress from backend")
                        && params.progress_token == cancelled_progress_token
            )
        })
        .await
        {
            return Err("Cancelled request progress should not be forwarded".into());
        }
        clear_notifications(&notifications);

        let (default_log_service, default_log_notifications) =
            notification_client(gateway_url.clone(), client.clone()).await?;
        default_log_service.call_tool(CallToolRequestParams::new(logging_tool.clone())).await?;
        call_progress_tool(&default_log_service, &default_log_notifications, &progress_tool).await?;
        if !notifications_absent_for(Duration::from_millis(100), &default_log_notifications, |event| {
            matches!(event, CapturedNotification::Logging(_))
        })
        .await
        {
            return Err("Logging should be opt-in before set_level".into());
        }

        service.subscribe(SubscribeRequestParams::new(resource_uri.clone())).await?;

        let saw_resource_update = eventually(Duration::from_secs(1), || {
            notifications_contain(
                &notifications,
                |event| matches!(event, CapturedNotification::ResourceUpdated(uri) if uri == &resource_uri),
            ) && notifications_contain(&notifications, |event| {
                matches!(event, CapturedNotification::ResourceListChanged)
            }) && notifications_contain(&notifications, |event| matches!(event, CapturedNotification::ToolListChanged))
        })
        .await;
        if !saw_resource_update {
            return Err(format!("Expected resource update, got {:?}", notifications.lock()).into());
        }

        clear_notifications(&notifications);
        let result = service.call_tool(CallToolRequestParams::new(notification_tool.clone())).await?;
        let saw_tool_resource_update = eventually(Duration::from_secs(1), || {
            notifications_contain(
                &notifications,
                |event| matches!(event, CapturedNotification::ResourceUpdated(uri) if uri == &resource_uri),
            )
        })
        .await;
        if !saw_tool_resource_update || !call_tool_result_contains_text(&result, "Notification task completed") {
            return Err(format!("Expected tool resource update, got {:?}", notifications.lock()).into());
        }

        clear_notifications(&notifications);
        service.call_tool(CallToolRequestParams::new(logging_tool.clone())).await?;
        let saw_log = eventually(Duration::from_secs(1), || {
            notifications_count(&notifications, |event| matches!(event, CapturedNotification::Logging(_))) == 3
                && notifications_contain(
                    &notifications,
                    |event| matches!(event, CapturedNotification::Logging(message) if message == "Log start"),
                )
        })
        .await;
        if !saw_log {
            return Err(format!("Expected logging notifications, got {:?}", notifications.lock()).into());
        }

        let (timeout_log_service, timeout_log_notifications) =
            notification_client(gateway_url.clone(), client.clone()).await?;
        timeout_log_service.set_level(SetLevelRequestParams::new(LoggingLevel::Debug)).await?;
        timeout_log_service.call_tool(CallToolRequestParams::new(logging_tool.clone())).await?;
        call_progress_tool(&timeout_log_service, &timeout_log_notifications, &progress_tool).await?;
        if !notifications_absent_for(Duration::from_millis(100), &timeout_log_notifications, |event| {
            matches!(event, CapturedNotification::Logging(_))
        })
        .await
        {
            return Err("Timed out set_level should not enable logging locally".into());
        }

        service.subscribe(SubscribeRequestParams::new(second_resource_uri.clone())).await?;
        let failing_uri = format!("{backend_name}-fail://subscribe");
        clear_notifications(&notifications);
        if service.subscribe(SubscribeRequestParams::new(failing_uri.clone())).await.is_ok() {
            return Err("Expected backend subscribe failure to propagate".into());
        }
        service.call_tool(CallToolRequestParams::new(logging_tool.clone())).await?;
        service.call_tool(CallToolRequestParams::new(second_notification_tool.clone())).await?;
        let saw_failed_subscribe_sentinel = eventually(Duration::from_secs(1), || {
            notifications_contain(
                &notifications,
                |event| matches!(event, CapturedNotification::Logging(message) if message == "Log start"),
            ) && notifications_contain(
                &notifications,
                |event| matches!(event, CapturedNotification::ResourceUpdated(uri) if uri == &second_resource_uri),
            )
        })
        .await;
        call_progress_tool(&service, &notifications, &progress_tool).await?;
        let failed_subscribe_absent = notifications_absent_for(
            Duration::from_millis(100),
            &notifications,
            |event| matches!(event, CapturedNotification::ResourceUpdated(uri) if uri.starts_with(&failing_uri)),
        )
        .await;
        if !saw_failed_subscribe_sentinel || !failed_subscribe_absent {
            return Err("Failed subscribe should not forward private resource updates".into());
        }

        clear_notifications(&notifications);
        service.call_tool(CallToolRequestParams::new(notification_tool.clone())).await?;
        let original_subscription_still_forwards = eventually(Duration::from_secs(1), || {
            notifications_contain(
                &notifications,
                |event| matches!(event, CapturedNotification::ResourceUpdated(uri) if uri == &resource_uri),
            )
        })
        .await;
        if !original_subscription_still_forwards {
            return Err("Failed subscribe should preserve the original subscription".into());
        }
        call_progress_tool(&service, &notifications, &progress_tool).await?;
        if !notifications_absent_for(
            Duration::from_millis(100),
            &notifications,
            |event| matches!(event, CapturedNotification::ResourceUpdated(uri) if uri.starts_with(&failing_uri)),
        )
        .await
        {
            return Err("Failed subscribe should not forward private resource updates".into());
        }

        let slow_subscribe_uri = format!("{second_backend_name}-slow://subscribe");
        clear_notifications(&notifications);
        if service.subscribe(SubscribeRequestParams::new(slow_subscribe_uri)).await.is_ok() {
            return Err("Expected backend subscribe timeout to propagate".into());
        }
        tokio::time::sleep(UPSTREAM_CONTROL_SETTLE_DURATION).await;
        service.call_tool(CallToolRequestParams::new(second_notification_tool.clone())).await?;
        let original_subscription_survived_subscribe_timeout = eventually(Duration::from_secs(1), || {
            notifications_contain(
                &notifications,
                |event| matches!(event, CapturedNotification::ResourceUpdated(uri) if uri == &second_resource_uri),
            )
        })
        .await;
        call_progress_tool(&service, &notifications, &progress_tool).await?;
        if !original_subscription_survived_subscribe_timeout {
            return Err("Subscribe timeout should restore relay and preserve the original subscription".into());
        }

        let failing_unsubscribe_uri = format!("{backend_name}-fail://unsubscribe");
        clear_notifications(&notifications);
        if service.unsubscribe(UnsubscribeRequestParams::new(failing_unsubscribe_uri.clone())).await.is_ok() {
            return Err("Expected backend unsubscribe failure to propagate".into());
        }
        service.call_tool(CallToolRequestParams::new(logging_tool.clone())).await?;
        service.call_tool(CallToolRequestParams::new(notification_tool.clone())).await?;
        let original_subscription_survived_failed_unsubscribe = eventually(Duration::from_secs(1), || {
            notifications_contain(
                &notifications,
                |event| matches!(event, CapturedNotification::Logging(message) if message == "Log start"),
            ) && notifications_contain(
                &notifications,
                |event| matches!(event, CapturedNotification::ResourceUpdated(uri) if uri == &resource_uri),
            )
        })
        .await;
        call_progress_tool(&service, &notifications, &progress_tool).await?;
        let failed_unsubscribe_absent =
            notifications_absent_for(Duration::from_millis(100), &notifications, |event| {
                matches!(event, CapturedNotification::ResourceUpdated(uri) if uri.starts_with(&failing_unsubscribe_uri))
            })
            .await;
        if !original_subscription_survived_failed_unsubscribe || !failed_unsubscribe_absent {
            return Err("Failed unsubscribe should preserve the original subscription".into());
        }

        let slow_unsubscribe_uri = format!("{second_backend_name}-slow://unsubscribe");
        clear_notifications(&notifications);
        if service.unsubscribe(UnsubscribeRequestParams::new(slow_unsubscribe_uri)).await.is_ok() {
            return Err("Expected backend unsubscribe timeout to propagate".into());
        }
        tokio::time::sleep(UPSTREAM_CONTROL_SETTLE_DURATION).await;
        service.call_tool(CallToolRequestParams::new(second_notification_tool.clone())).await?;
        let original_subscription_survived_unsubscribe_timeout = eventually(Duration::from_secs(1), || {
            notifications_contain(
                &notifications,
                |event| matches!(event, CapturedNotification::ResourceUpdated(uri) if uri == &second_resource_uri),
            )
        })
        .await;
        call_progress_tool(&service, &notifications, &progress_tool).await?;
        if !original_subscription_survived_unsubscribe_timeout {
            return Err("Unsubscribe timeout should restore relay and preserve the original subscription".into());
        }

        clear_notifications(&notifications);
        let result = service.call_tool(CallToolRequestParams::new(logging_tool.clone())).await?;
        let saw_log = eventually(Duration::from_secs(1), || {
            notifications_contain(
                &notifications,
                |event| matches!(event, CapturedNotification::Logging(message) if message == "Log start"),
            )
        })
        .await;
        if !saw_log || !call_tool_result_contains_text(&result, "Logging task completed") {
            return Err(format!("Expected log notification, got {:?}", notifications.lock()).into());
        }

        clear_notifications(&notifications);
        service.unsubscribe(UnsubscribeRequestParams::new(resource_uri.clone())).await?;
        service.call_tool(CallToolRequestParams::new(notification_tool.clone())).await?;
        service.call_tool(CallToolRequestParams::new(logging_tool.clone())).await?;
        service.call_tool(CallToolRequestParams::new(second_notification_tool)).await?;
        let saw_successful_unsubscribe_sentinel = eventually(Duration::from_secs(1), || {
            notifications_contain(
                &notifications,
                |event| matches!(event, CapturedNotification::Logging(message) if message == "Log start"),
            ) && notifications_contain(
                &notifications,
                |event| matches!(event, CapturedNotification::ResourceUpdated(uri) if uri == &second_resource_uri),
            )
        })
        .await;
        call_progress_tool(&service, &notifications, &progress_tool).await?;
        let unsubscribed_resource_absent = notifications_absent_for(
            Duration::from_millis(100),
            &notifications,
            |event| matches!(event, CapturedNotification::ResourceUpdated(uri) if uri == &resource_uri),
        )
        .await;
        if !saw_successful_unsubscribe_sentinel || !unsubscribed_resource_absent {
            return Err("Successful unsubscribe should stop resource updates".into());
        }

        let (unsubscribed_service, unsubscribed_notifications) =
            notification_client(gateway_url.clone(), client.clone()).await?;
        unsubscribed_service.call_tool(CallToolRequestParams::new(notification_tool)).await?;
        call_progress_tool(&unsubscribed_service, &unsubscribed_notifications, &progress_tool).await?;
        if !notifications_absent_for(
            Duration::from_millis(100),
            &unsubscribed_notifications,
            |event| matches!(event, CapturedNotification::ResourceUpdated(uri) if uri == &resource_uri),
        )
        .await
        {
            return Err("Resource updates should not leak to an unsubscribed downstream session".into());
        }

        let (muted_service, muted_notifications) = notification_client(gateway_url, client).await?;
        muted_service.set_level(SetLevelRequestParams::new(LoggingLevel::Error)).await?;
        muted_service.call_tool(CallToolRequestParams::new(logging_tool)).await?;
        call_progress_tool(&muted_service, &muted_notifications, &progress_tool).await?;
        if !notifications_absent_for(Duration::from_millis(100), &muted_notifications, |event| {
            matches!(event, CapturedNotification::Logging(_))
        })
        .await
        {
            return Err("Logging level should be scoped to the downstream session".into());
        }

        Ok(())
    }
    .boxed();

    let maybe_passed = test_future.await;
    handle.abort();
    maybe_passed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[test_log::test]
async fn tls_list_tools_end_to_end_test() -> Result<()> {
    let provider = crypto::ring::default_provider();
    _ = provider.install_default();
    let gateway_port = create_ports(1)[0];
    let server_socket_addr: std::net::SocketAddr =
        format!("127.0.0.1:{gateway_port}").parse().expect("This should work");

    let config = Config {
        token_verification_public_key: Some("../../assets/jwt.key.pub".into()),
        upstream_connection_mode: Some(UpstreamConnectionMode::PlainTextOrTls),
        tls_address: Some(server_socket_addr),
        server_private_key: Some("../../assets/contextforgeCA/contextforge-server.key.pem".into()),
        server_certificate: Some("../../assets/contextforgeCA/contextforge-server.cert.pem".into()),
        upstream_trust_bundle: Some("../../assets/contextforgeCA/contextforge.intermediate.ca-chain.cert.pem".into()),
        ..Default::default()
    };

    let user = "admin@example.com";

    let Ok(TestSettings { handle, gateway_url, expected_tool_names, .. }) =
        create_tls_gateway_with_four_tls_counters(user, config).await
    else {
        panic!("Invalid configuration ");
    };

    let test_future: BoxFuture<Result<()>> = async {
        let mut buf = Vec::new();
        File::open("../../assets/contextforgeCA/contextforge.intermediate.ca-chain.cert.pem")?.read_to_end(&mut buf)?;
        let certificates = reqwest::Certificate::from_pem_bundle(&buf)?;

        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut default_headers = HeaderMap::new();
        let token = token(user);
        default_headers.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).expect("This should work"),
        );
        let client = reqwest::Client::builder()
            .https_only(true)
            .tls_certs_only(certificates)
            .default_headers(default_headers)
            .build()
            .expect("This should work");

        info!("Seding request to {gateway_url}");

        let config = StreamableHttpClientTransportConfig::with_uri(gateway_url);
        let transport = StreamableHttpClientTransport::with_client(client, config);
        let request = InitializeRequestParams::default();

        let maybe_service = request.serve(transport).await;
        let Ok(running_service) = maybe_service else {
            warn!("No Service {maybe_service:?}");
            return Err("Couldn't get a service".into());
        };

        let list_tools = running_service.list_tools(None).await;
        let Ok(list_tools) = list_tools else {
            let msg = format!("List tools returned error  {list_tools:?}");
            warn!(msg);
            return Err(msg.into());
        };

        let mut names: Vec<String> = list_tools.tools.iter().map(|t| t.name.to_string()).collect();
        names.sort();

        info!("Tool names {names:#?}");
        if expected_tool_names != names {
            warn!("Actual {names:#?} Expected {expected_tool_names:#?}");
            return Err("Expected tool names don't match actual".into());
        }

        Ok(())
    }
    .boxed();

    let maybe_passed = test_future.await;

    handle.abort();
    if maybe_passed.is_ok() {
        info!("Test passed");
    } else {
        info!("Test NOT passed {maybe_passed:?}");
        panic!()
    }

    Ok(())
}
