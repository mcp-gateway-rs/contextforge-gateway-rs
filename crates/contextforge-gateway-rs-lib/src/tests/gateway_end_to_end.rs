use futures::{FutureExt, future::BoxFuture};
use http::{HeaderMap, HeaderValue};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use openport;
use rmcp::{
    ServiceExt,
    model::InitializeRequestParams,
    transport::{
        StreamableHttpClientTransport, StreamableHttpServerConfig, StreamableHttpService,
        streamable_http_client::StreamableHttpClientTransportConfig,
        streamable_http_server::session::local::LocalSessionManager,
    },
};
use std::{collections::HashMap, fs, sync::Arc};
use tokio::sync::Semaphore;
use tracing::{info, warn};

use crate::{
    Config, Gateway,
    common::DefaultClaims,
    tests::{mock_counter, mocked_user_config_store::MockedUserConfigStore},
    user_config_store::{BackendMCPGateway, User, UserConfig, UserConfigStore, VirtualHost},
};

const MOCK_COUNTER_TOOL_NAMES: &[&str] =
    &["decrement", "echo", "get_session_id", "get_value", "increment", "long_task", "say_hello", "sum"];

fn create_ports(ports: usize) -> Vec<u16> {
    (0..ports).into_iter().map(|_| openport::pick_random_unused_port().expect("Expecting to find port")).collect()
}

fn create_backends(ports: &[u16]) -> HashMap<String, BackendMCPGateway> {
    ports
        .iter()
        .filter_map(|port| {
            let url = format!("http://127.0.0.1:{port}/mcp").parse().expect("This should work");
            Some((format!("backend-{port}"), BackendMCPGateway { url }))
        })
        .collect::<HashMap<_, _>>()
}

fn create_tool_names(ports: &[u16]) -> Vec<String> {
    ports
        .iter()
        .flat_map(|port| {
            MOCK_COUNTER_TOOL_NAMES.iter().map(|name| format!("backend-{port}-{name}")).collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
}

fn create_axum_servers(
    ports: &[u16],
    router: axum::Router,
) -> Vec<BoxFuture<'static, Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>>> {
    ports
        .iter()
        .map(|port| {
            let addr = format!("127.0.0.1:{port}");
            let router = router.clone();
            async {
                let listener = tokio::net::TcpListener::bind(addr).await.expect("Expect this to work");
                axum::serve(listener, router).await.unwrap();
                Ok(())
            }
            .boxed()
        })
        .collect()
}

pub fn get_token(user_id: String) -> String {
    let key = EncodingKey::from_rsa_pem(&fs::read("../../assets/jwt.key").expect("Expecting this to work"))
        .expect("Expecting this to work");
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some("test".to_owned());

    let claims = DefaultClaims::new(user_id);

    encode::<DefaultClaims>(&header, &claims, &key).expect("Expecting this to work")
}

struct TestSettings {
    handle: tokio::task::JoinHandle<Vec<Result<(), Box<dyn std::error::Error + Send + Sync>>>>,
    gateway_url: String,
    expected_tool_names: Vec<String>,
}

async fn create_gateway_with_four_counters(user: &str, test_semaphore: Arc<Semaphore>) -> TestSettings {
    let gateway_port = create_ports(1)[0];

    let mut config = Config::default();
    config.address = Some(format!("127.0.0.1:{gateway_port}").parse().expect("This should work"));
    config.token_verification_public_key = "../../assets/jwt.key.pub".into();

    let mocked_user_config_store = MockedUserConfigStore::default();

    let gateway_one_ports = create_ports(2);
    let gateway_two_ports = create_ports(2);

    let service = StreamableHttpService::new(
        || Ok(mock_counter::Counter::new()),
        LocalSessionManager::default().into(),
        StreamableHttpServerConfig::default(),
    );

    let router = axum::Router::new().route_service("/mcp", service);

    let servers_one = create_axum_servers(&gateway_one_ports, router.clone());
    let servers_two = create_axum_servers(&gateway_two_ports, router.clone());

    assert_ne!(gateway_one_ports, gateway_two_ports);

    let gateway_one_backends = create_backends(&gateway_one_ports);
    let gateway_two_backends = create_backends(&gateway_two_ports);

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
        .with_config(config)
        .with_user_config_store(Arc::new(mocked_user_config_store))
        .with_session_manager(Arc::new(LocalSessionManager::default()))
        .build();

    let gateway: std::pin::Pin<
        Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>> + Send>,
    > = async move {
        let res = gateway.run_gateway().await;
        warn!("Gateway exited with result {res:?}");
        test_semaphore.forget_permits(1);
        Ok(())
    }
    .boxed();

    let handle: tokio::task::JoinHandle<Vec<Result<(), Box<dyn std::error::Error + Send + Sync>>>> =
        tokio::spawn(futures::future::join_all(
            vec![gateway].into_iter().chain(servers_one.into_iter()).chain(servers_two.into_iter()), //.chain(vec![test_future].into_iter()),
        ));

    let gateway_url = format!("http://127.0.0.1:{gateway_port}/contextforge-rs/servers/{}/mcp", virtual_host_one_id);
    TestSettings { handle, gateway_url, expected_tool_names: virtual_host_one_tool_names }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[test_log::test]
async fn list_tools_end_to_end_test() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let semaphore = Arc::new(Semaphore::new(1));
    let _ = semaphore.acquire().await.unwrap();
    let user = "admin@example.com";
    let test_semaphore = Arc::new(Semaphore::new(1));
    let _ = semaphore.acquire().await.unwrap();

    let TestSettings { handle, gateway_url, expected_tool_names } =
        create_gateway_with_four_counters(user, test_semaphore.clone()).await;

    let test_future: std::pin::Pin<
        Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>> + Send>,
    > = async {
        let _ = test_semaphore.acquire().await;
        //tokio::time::sleep(Duration::from_millis(100)).await;
        let mut default_headers = HeaderMap::new();
        let token = get_token(user.to_owned());
        default_headers.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_str(format!("Bearer {token}").as_str()).expect("This should work"),
        );
        let client = reqwest::Client::builder().default_headers(default_headers).build().expect("This should work");

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
    if let Ok(_) = maybe_passed {
        info!("Test passed");
    } else {
        info!("Test NOT passed {maybe_passed:?}");
        panic!()
    }

    Ok(())
}
