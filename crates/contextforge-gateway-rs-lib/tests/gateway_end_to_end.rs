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
use http::{HeaderMap, HeaderValue};
use rmcp::{
    ClientHandler, ServiceExt,
    model::InitializeRequestParams,
    model::{
        CallToolRequest, CallToolRequestParams, CallToolResult, ClientInfo, ClientRequest, LoggingLevel,
        LoggingMessageNotificationParam, ProgressNotificationParam, ResourceUpdatedNotificationParam, ServerResult,
        SetLevelRequestParams, SubscribeRequestParams, UnsubscribeRequestParams,
    },
    service::{NotificationContext, PeerRequestOptions, RoleClient, RunningService},
    transport::{
        StreamableHttpClientTransport, StreamableHttpServerConfig, StreamableHttpService,
        streamable_http_client::StreamableHttpClientTransportConfig,
        streamable_http_server::session::local::LocalSessionManager,
    },
};
use rustls::crypto::{self};
use tracing::{info, warn};

use contextforge_gateway_rs_lib::{
    Config, Gateway, Result, UpstreamConnectionMode, UserConfigStore, UserConfigStoreType,
};
use support::{MemoryUserConfigStore, mock_counter, token};

const MOCK_COUNTER_TOOL_NAMES: &[&str] = &[
    "decrement",
    "echo",
    "get_session_id",
    "get_value",
    "increment",
    "instant_logging_task",
    "logging_task",
    "long_task",
    "notification_task",
    "progress_task",
    "say_hello",
    "sum",
];

#[derive(Clone, Debug)]
struct NotificationCapturingClient {
    notifications: CapturedNotifications,
}

type CapturedNotifications = Arc<Mutex<Vec<CapturedNotification>>>;

#[derive(Clone, Debug)]
enum CapturedNotification {
    Logging(String),
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
            .push(CapturedNotification::Logging(params.data.as_str().unwrap_or_default().to_owned()));
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

async fn notifications_absent_for<F>(
    duration: Duration,
    notifications: &CapturedNotifications,
    mut predicate: F,
) -> bool
where
    F: FnMut(&CapturedNotification) -> bool,
{
    let deadline = Instant::now() + duration;
    loop {
        if notifications_contain(notifications, |event| predicate(event)) {
            return false;
        }
        if Instant::now() >= deadline {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
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

fn find_tool(expected_tool_names: &[String], suffix: &str) -> Result<String> {
    expected_tool_names
        .iter()
        .find(|name| name.ends_with(suffix))
        .cloned()
        .ok_or_else(|| format!("Missing {suffix} tool").into())
}

fn find_tools(expected_tool_names: &[String], suffix: &str, count: usize) -> Result<Vec<String>> {
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

    let virtual_host_one_id = uuid::Uuid::new_v4().to_string();
    let virtual_host_two_id = uuid::Uuid::new_v4().to_string();

    let virtual_hosts = HashMap::from([
        (virtual_host_one_id.clone(), VirtualHost { backends: gateway_one_backends }),
        (virtual_host_two_id.clone(), VirtualHost { backends: gateway_two_backends }),
    ]);

    let user_config = UserConfig { virtual_hosts };

    mocked_user_config_store.set_config(&User::new(user), &user_config).await.expect("This should work");

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

    if let Some(address) = config.address.as_ref() {
        let gateway_url = format!("http://{address}/contextforge-rs/servers/{virtual_host_one_id}/mcp");

        let servers_one = create_axum_servers(&gateway_one_ports, &router);
        let servers_two = create_axum_servers(&gateway_two_ports, &router);
        let handle: tokio::task::JoinHandle<Vec<Result<()>>> = tokio::spawn(futures::future::join_all(
            vec![gateway].into_iter().chain(servers_one).chain(servers_two), //.chain(vec![test_future].into_iter()),
        ));

        Ok(TestSettings { handle, gateway_url, expected_tool_names: virtual_host_one_tool_names })
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

        let servers_one = create_axum_tls_servers(&gateway_one_ports, router.clone()).await;
        let servers_two = create_axum_tls_servers(&gateway_two_ports, router.clone()).await;
        let handle: tokio::task::JoinHandle<Vec<Result<()>>> =
            tokio::spawn(futures::future::join_all(vec![gateway].into_iter().chain(servers_one).chain(servers_two)));

        Ok(TestSettings { handle, gateway_url, expected_tool_names: virtual_host_one_tool_names })
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

    let Ok(TestSettings { handle, gateway_url, expected_tool_names }) =
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
async fn plaintext_call_tool_forwards_backend_notifications() -> Result<()> {
    let gateway_port = create_ports(1)[0];

    let config = Config {
        address: Some(format!("127.0.0.1:{gateway_port}").parse().expect("This should work")),
        token_verification_public_key: Some("../../assets/jwt.key.pub".into()),
        upstream_connection_mode: Some(UpstreamConnectionMode::PlainTextOrTls),
        ..Default::default()
    };

    let user = "admin@example.com";
    let TestSettings { handle, gateway_url, expected_tool_names } =
        create_gateway_with_four_counters(user, config).await.expect("valid plaintext gateway configuration");

    let test_future: BoxFuture<'_, Result<()>> = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let client = client_for_user(user)?;

        let (service, notifications) = notification_client(gateway_url.clone(), client.clone()).await?;
        let saw_initialized_tool_list = eventually(Duration::from_secs(1), || {
            notifications_contain(&notifications, |event| matches!(event, CapturedNotification::ToolListChanged))
        })
        .await;
        if !saw_initialized_tool_list {
            return Err("Expected backend tool list notification after initialize".into());
        }
        clear_notifications(&notifications);

        let progress_tool = find_tool(&expected_tool_names, "-progress_task")?;
        let notification_tools = find_tools(&expected_tool_names, "-notification_task", 2)?;
        let notification_tool = notification_tools[0].clone();
        let second_notification_tool = notification_tools[1].clone();
        let backend_name = notification_tool.strip_suffix("-notification_task").expect("tool suffix should match");
        let second_backend_name =
            second_notification_tool.strip_suffix("-notification_task").expect("tool suffix should match");
        let logging_tool = format!("{backend_name}-logging_task");
        let resource_uri = format!("{backend_name}-memo://insights");
        let second_resource_uri = format!("{second_backend_name}-memo://insights");

        let long_tool = find_tool(&expected_tool_names, "-long_task")?;
        let long_request = ClientRequest::CallToolRequest(CallToolRequest::new(CallToolRequestParams::new(long_tool)));
        let long_handle = service.send_cancellable_request(long_request, PeerRequestOptions::no_options()).await?;
        let list_tools_while_call_is_active = service.list_tools(None).await?;
        let mut names =
            list_tools_while_call_is_active.tools.iter().map(|tool| tool.name.to_string()).collect::<Vec<_>>();
        names.sort();
        long_handle.cancel(Some("concurrency check complete".to_owned())).await?;
        if names != expected_tool_names {
            return Err("Concurrent same-session list_tools returned partial backend data".into());
        }

        let request =
            ClientRequest::CallToolRequest(CallToolRequest::new(CallToolRequestParams::new(progress_tool.clone())));
        let request_handle = service.send_cancellable_request(request, PeerRequestOptions::no_options()).await?;
        let expected_progress_token = request_handle.progress_token.clone();
        let saw_progress = eventually(Duration::from_secs(1), || {
            notifications_contain(&notifications, |event| {
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
        if !saw_progress || !server_result_contains_text(&result, "Progress task completed") {
            return Err(format!("Unexpected progress forwarding result {result:?} {:?}", notifications.lock()).into());
        }

        service.subscribe(SubscribeRequestParams::new(resource_uri.clone())).await?;
        let saw_async_resource_update = eventually(Duration::from_secs(1), || {
            notifications_contain(
                &notifications,
                |event| matches!(event, CapturedNotification::ResourceUpdated(uri) if uri == &resource_uri),
            ) && notifications_contain(&notifications, |event| {
                matches!(event, CapturedNotification::ResourceListChanged)
            }) && notifications_contain(&notifications, |event| matches!(event, CapturedNotification::ToolListChanged))
        })
        .await;
        if !saw_async_resource_update {
            return Err(format!("Expected async subscribed notifications, got {:?}", notifications.lock()).into());
        }
        if notifications_count(&notifications, |event| matches!(event, CapturedNotification::ResourceListChanged)) != 1
            || notifications_count(&notifications, |event| matches!(event, CapturedNotification::ToolListChanged)) != 1
        {
            return Err(
                format!("Subscribe notifications should be forwarded once, got {:?}", notifications.lock()).into()
            );
        }

        clear_notifications(&notifications);
        let result = service.call_tool(CallToolRequestParams::new(notification_tool.clone())).await?;
        let saw_resource_update = eventually(Duration::from_secs(1), || {
            notifications_contain(
                &notifications,
                |event| matches!(event, CapturedNotification::ResourceUpdated(uri) if uri == &resource_uri),
            )
        })
        .await;
        if !saw_resource_update || !call_tool_result_contains_text(&result, "Notification task completed") {
            return Err(format!("Unexpected resource notification result {result:?} {:?}", notifications.lock()).into());
        }

        clear_notifications(&notifications);
        service.subscribe(SubscribeRequestParams::new(second_resource_uri.clone())).await?;
        let saw_second_backend_resource_update = eventually(Duration::from_secs(1), || {
            notifications_contain(
                &notifications,
                |event| matches!(event, CapturedNotification::ResourceUpdated(uri) if uri == &second_resource_uri),
            )
        })
        .await;
        if !saw_second_backend_resource_update {
            return Err("Expected notification from second backend subscription".into());
        }

        clear_notifications(&notifications);
        service.unsubscribe(UnsubscribeRequestParams::new(resource_uri.clone())).await?;
        service.call_tool(CallToolRequestParams::new(notification_tool.clone())).await?;
        service.call_tool(CallToolRequestParams::new(second_notification_tool.clone())).await?;
        let saw_unsubscribe_sentinel = eventually(Duration::from_secs(1), || {
            notifications_contain(
                &notifications,
                |event| matches!(event, CapturedNotification::ResourceUpdated(uri) if uri == &second_resource_uri),
            )
        })
        .await;
        let saw_unsubscribed_resource = notifications_contain(
            &notifications,
            |event| matches!(event, CapturedNotification::ResourceUpdated(uri) if uri == &resource_uri),
        );
        let unsubscribed_resource_stayed_absent = notifications_absent_for(
            Duration::from_millis(100),
            &notifications,
            |event| matches!(event, CapturedNotification::ResourceUpdated(uri) if uri == &resource_uri),
        )
        .await;
        if !saw_unsubscribe_sentinel || saw_unsubscribed_resource || !unsubscribed_resource_stayed_absent {
            return Err("Resource update should not be forwarded after unsubscribe".into());
        }

        let failing_uri = format!("{backend_name}-fail://subscribe");
        clear_notifications(&notifications);
        if service.subscribe(SubscribeRequestParams::new(failing_uri.clone())).await.is_ok() {
            return Err("Expected backend subscribe failure to propagate".into());
        }
        service.call_tool(CallToolRequestParams::new(second_notification_tool.clone())).await?;
        let saw_failed_subscribe_sentinel = eventually(Duration::from_secs(1), || {
            notifications_contain(
                &notifications,
                |event| matches!(event, CapturedNotification::ResourceUpdated(uri) if uri == &second_resource_uri),
            )
        })
        .await;
        let saw_failed_subscribe_resource = notifications_contain(
            &notifications,
            |event| matches!(event, CapturedNotification::ResourceUpdated(uri) if uri.starts_with(&failing_uri)),
        );
        let failed_subscribe_resource_stayed_absent = notifications_absent_for(
            Duration::from_millis(100),
            &notifications,
            |event| matches!(event, CapturedNotification::ResourceUpdated(uri) if uri.starts_with(&failing_uri)),
        )
        .await;
        if !saw_failed_subscribe_sentinel || saw_failed_subscribe_resource || !failed_subscribe_resource_stayed_absent {
            return Err("Failed subscribe should not forward buffered resource updates".into());
        }

        let instant_tool = format!("{backend_name}-instant_logging_task");
        let result = service.call_tool(CallToolRequestParams::new(instant_tool)).await?;
        let saw_instant_log = eventually(Duration::from_secs(1), || {
            notifications_contain(
                &notifications,
                |event| matches!(event, CapturedNotification::Logging(message) if message == "Instant log"),
            )
        })
        .await;
        if !saw_instant_log || !call_tool_result_contains_text(&result, "Instant logging task completed") {
            return Err(format!("Expected drained log notification, got {:?}", notifications.lock()).into());
        }

        let (muted_service, muted_notifications) = notification_client(gateway_url.clone(), client.clone()).await?;
        muted_service.set_level(SetLevelRequestParams::new(LoggingLevel::Error)).await?;
        let (default_service, default_notifications) = notification_client(gateway_url.clone(), client.clone()).await?;
        let (unsubscribed_service, unsubscribed_notifications) =
            notification_client(gateway_url.clone(), client).await?;
        muted_service.call_tool(CallToolRequestParams::new(logging_tool.clone())).await?;
        muted_service.call_tool(CallToolRequestParams::new(progress_tool.clone())).await?;
        default_service.call_tool(CallToolRequestParams::new(logging_tool.clone())).await?;
        unsubscribed_service.call_tool(CallToolRequestParams::new(notification_tool)).await?;
        unsubscribed_service.call_tool(CallToolRequestParams::new(progress_tool)).await?;
        let muted_received_progress = eventually(Duration::from_secs(1), || {
            notifications_contain(&muted_notifications, |event| matches!(event, CapturedNotification::Progress(_)))
        })
        .await;
        let muted_received_logs =
            notifications_contain(&muted_notifications, |event| matches!(event, CapturedNotification::Logging(_)));
        let muted_logs_stayed_absent =
            notifications_absent_for(Duration::from_millis(100), &muted_notifications, |event| {
                matches!(event, CapturedNotification::Logging(_))
            })
            .await;
        let default_received_logs = eventually(Duration::from_secs(1), || {
            notifications_count(&default_notifications, |event| matches!(event, CapturedNotification::Logging(_))) == 3
        })
        .await;
        if !muted_received_progress || muted_received_logs || !muted_logs_stayed_absent || !default_received_logs {
            return Err("Logging level should be scoped to the muted downstream session".into());
        }
        let unsubscribed_received_progress = eventually(Duration::from_secs(1), || {
            notifications_contain(&unsubscribed_notifications, |event| {
                matches!(event, CapturedNotification::Progress(_))
            })
        })
        .await;
        let unsubscribed_received_resource_update = notifications_contain(
            &unsubscribed_notifications,
            |event| matches!(event, CapturedNotification::ResourceUpdated(uri) if uri == &resource_uri),
        );
        let unsubscribed_resource_update_stayed_absent = notifications_absent_for(
            Duration::from_millis(100),
            &unsubscribed_notifications,
            |event| matches!(event, CapturedNotification::ResourceUpdated(uri) if uri == &resource_uri),
        )
        .await;
        if !unsubscribed_received_progress
            || unsubscribed_received_resource_update
            || !unsubscribed_resource_update_stayed_absent
        {
            return Err("Resource subscriptions should be scoped to one downstream session".into());
        }

        service.cancel().await?;
        muted_service.cancel().await?;
        default_service.cancel().await?;
        unsubscribed_service.cancel().await?;
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

    let Ok(TestSettings { handle, gateway_url, expected_tool_names }) =
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
            HeaderValue::from_str(format!("Bearer {token}").as_str()).expect("This should work"),
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
