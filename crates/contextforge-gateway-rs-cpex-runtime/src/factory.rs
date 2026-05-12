use std::sync::Arc;

use cpex_core::{
    cmf::CmfHook,
    error::PluginError,
    factory::{PluginFactory, PluginInstance},
    hooks::TypedHandlerAdapter,
    plugin::PluginConfig,
};
use cpex_payload_marker::PayloadMarkerPlugin;

pub const PAYLOAD_MARKER_KIND: &str = "cpex-payload-marker";

pub(crate) struct PayloadMarkerFactory;

impl PluginFactory for PayloadMarkerFactory {
    fn create(&self, config: &PluginConfig) -> Result<PluginInstance, Box<PluginError>> {
        let plugin = Arc::new(PayloadMarkerPlugin::new(config.clone()));
        let handlers = PayloadMarkerPlugin::CMF_HOOKS
            .iter()
            .filter(|hook| config.hooks.iter().any(|configured| configured == **hook))
            .map(|hook| {
                (
                    *hook,
                    Arc::new(TypedHandlerAdapter::<CmfHook, _>::new(Arc::clone(&plugin)))
                        as Arc<dyn cpex_core::registry::AnyHookHandler>,
                )
            })
            .collect::<Vec<_>>();

        if handlers.is_empty() {
            return Err(PluginError::Config {
                message: format!("plugin '{}' has no supported CMF hooks", config.name),
            }
            .boxed());
        }

        Ok(PluginInstance { plugin, handlers })
    }
}
