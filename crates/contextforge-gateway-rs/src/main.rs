mod logging;
mod runtime;
#[cfg(feature = "test-plugins")]
mod test_plugins;

use std::sync::Arc;

use clap::Parser;
use contextforge_gateway_rs_cpex_runtime::CpexRuntimeRegistry;
use contextforge_gateway_rs_lib::{
    Config, Gateway, GatewayToolRuntime, RedisClient, RedisConfig, UserConfigStore, get_config_store,
};
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

    let redis_client = RedisClient::try_from(RedisConfig::try_from(&config)?)?;
    let plugin_runtime = CpexRuntimeRegistry::with_redis_config(redis_client);
    #[cfg(feature = "test-plugins")]
    let plugin_runtime = {
        let mut plugin_runtime = plugin_runtime;
        test_plugins::register(&mut plugin_runtime);
        plugin_runtime
    };
    let plugin_runtime = Arc::new(plugin_runtime);
    let gateway_plugin_runtime: Arc<dyn GatewayToolRuntime> = Arc::<CpexRuntimeRegistry>::clone(&plugin_runtime);
    let redis_store = get_config_store(&config)?;
    let user_config_store: Arc<dyn UserConfigStore + Send + Sync> = Arc::new(redis_store);
    let gateway = Gateway::builder()
        .with_config(config)
        .with_user_config_store(user_config_store)
        .with_session_manager(Arc::new(LocalSessionManager::default()))
        .with_plugin_runtime(gateway_plugin_runtime)
        .build();

    runtime.execute(gateway)
}
