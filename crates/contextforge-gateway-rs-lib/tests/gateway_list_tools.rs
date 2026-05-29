mod support;

use std::{
    fs::File,
    io::Read,
    time::{Duration, Instant},
};

use contextforge_gateway_rs_lib::{Config, Result, UpstreamConnectionMode};
use futures::{FutureExt, future::BoxFuture};
use http::{HeaderMap, HeaderValue};
use rmcp::{
    ServiceExt,
    model::InitializeRequestParams,
    transport::{StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig},
};
use rustls::crypto;
use tracing::{info, warn};

use support::{
    ListToolsGatewaySettings, create_gateway_with_four_counters, create_ports,
    create_tls_gateway_with_four_tls_counters, token,
};

const CLIENT_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const TEST_POLL_INTERVAL: Duration = Duration::from_millis(20);

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[test_log::test]
async fn plaintext_lists_prefixed_backend_tools() -> Result<()> {
    let gateway_port = create_ports(1)[0];

    let config = Config {
        address: Some(format!("127.0.0.1:{gateway_port}").parse().expect("This should work")),
        token_verification_public_key: Some("../../assets/jwt.key.pub".into()),
        upstream_connection_mode: Some(UpstreamConnectionMode::PlainTextOrTls),
        ..Default::default()
    };

    let user = "admin@example.com";

    let Ok(ListToolsGatewaySettings { handle, gateway_url, expected_tool_names }) =
        create_gateway_with_four_counters(user, config).await
    else {
        panic!("Invalid configuration ");
    };

    let client = create_client(user);
    let maybe_passed = assert_list_tools(gateway_url, client, expected_tool_names).await;

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
async fn tls_lists_prefixed_backend_tools() -> Result<()> {
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

    let Ok(ListToolsGatewaySettings { handle, gateway_url, expected_tool_names }) =
        create_tls_gateway_with_four_tls_counters(user, config).await
    else {
        panic!("Invalid configuration ");
    };

    let test_future: BoxFuture<Result<()>> = async {
        let mut buf = Vec::new();
        File::open("../../assets/contextforgeCA/contextforge.intermediate.ca-chain.cert.pem")?.read_to_end(&mut buf)?;
        let certificates = reqwest::Certificate::from_pem_bundle(&buf)?;

        let client = create_tls_client(user, certificates);
        assert_list_tools(gateway_url, client, expected_tool_names).await
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

fn create_client(user: &str) -> reqwest::Client {
    reqwest::Client::builder().default_headers(auth_headers(user)).build().expect("This should work")
}

fn create_tls_client(user: &str, certificates: Vec<reqwest::Certificate>) -> reqwest::Client {
    reqwest::Client::builder()
        .https_only(true)
        .tls_certs_only(certificates)
        .default_headers(auth_headers(user))
        .build()
        .expect("This should work")
}

fn auth_headers(user: &str) -> HeaderMap {
    let mut default_headers = HeaderMap::new();
    let token = token(user);
    default_headers.insert(
        http::header::AUTHORIZATION,
        HeaderValue::from_str(format!("Bearer {token}").as_str()).expect("This should work"),
    );
    default_headers
}

async fn assert_list_tools(
    gateway_url: String,
    client: reqwest::Client,
    expected_tool_names: Vec<String>,
) -> Result<()> {
    info!("Seding request to {gateway_url}");

    let running_service = connect_client(gateway_url, client).await?;

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

async fn connect_client(
    gateway_url: String,
    client: reqwest::Client,
) -> Result<rmcp::service::RunningService<rmcp::RoleClient, InitializeRequestParams>> {
    let deadline = Instant::now() + CLIENT_CONNECT_TIMEOUT;
    loop {
        let config = StreamableHttpClientTransportConfig::with_uri(gateway_url.clone());
        let transport = StreamableHttpClientTransport::with_client(client.clone(), config);
        let request = InitializeRequestParams::default();

        match request.serve(transport).await {
            Ok(running_service) => return Ok(running_service),
            Err(error) if Instant::now() < deadline => {
                warn!("No Service {error:?}");
                tokio::time::sleep(TEST_POLL_INTERVAL).await;
            },
            Err(error) => {
                warn!("No Service {error:?}");
                return Err("Couldn't get a service".into());
            },
        }
    }
}
