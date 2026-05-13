use std::sync::Arc;

use contextforge_gateway_rs_cpex_runtime::CpexRuntimeRegistry;
use serde_json::{Value, json};

use super::{MemoryRuntimePluginConfigStore, TestPlugin, TestPluginFactory};

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
        runtime
            .register_factory(format!("test-{index}"), Box::new(TestPluginFactory::from_plugin(plugin)))
            .expect("test factory registers");
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
