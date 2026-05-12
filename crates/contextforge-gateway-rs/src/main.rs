mod logging;
mod runtime;

use std::sync::Arc;

use clap::Parser;
use contextforge_gateway_rs_lib::{Config, Gateway, get_config_store};
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rustls::crypto;
use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;
#[allow(clippy::print_stdout)]
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let provider = crypto::ring::default_provider();
    _ = provider.install_default();

    let config = Config::parse();
    println!("contextforge-gateway-rs {config:?}");
    let _guard = logging::init_tracing_logging(&config);

    let runtime = runtime::Runtime::from(&config);

    let user_config_store = get_config_store(&config)?;
    let gateway = Gateway::builder()
        .with_config(config)
        .with_user_config_store(Arc::new(user_config_store))
        .with_session_manager(Arc::new(LocalSessionManager::default()))
        .build();

    _ = runtime.execute(gateway);

    Ok(())
}
