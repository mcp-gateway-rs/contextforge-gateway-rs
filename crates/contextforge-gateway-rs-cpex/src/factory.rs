use std::{marker::PhantomData, sync::Arc};

use cpex_core::{
    cmf::CmfHook,
    error::PluginError,
    factory::{PluginFactory, PluginInstance},
    hooks::{HookHandler, TypedHandlerAdapter, types::cmf_hook_names},
    plugin::{Plugin, PluginConfig},
    registry::AnyHookHandler,
};

pub struct CmfPluginFactory<P> {
    build: fn(PluginConfig) -> P,
    _plugin: PhantomData<P>,
}

impl<P> CmfPluginFactory<P> {
    pub fn new(build: fn(PluginConfig) -> P) -> Self {
        Self { build, _plugin: PhantomData }
    }
}

impl<P> PluginFactory for CmfPluginFactory<P>
where
    P: Plugin + HookHandler<CmfHook> + 'static,
{
    fn create(&self, config: &PluginConfig) -> Result<PluginInstance, Box<PluginError>> {
        let plugin = Arc::new((self.build)(config.clone()));
        let handlers = config
            .hooks
            .iter()
            .filter_map(|hook| cmf_hook_name(hook))
            .map(|hook| {
                (hook, Arc::new(TypedHandlerAdapter::<CmfHook, _>::new(Arc::clone(&plugin))) as Arc<dyn AnyHookHandler>)
            })
            .collect::<Vec<_>>();

        if handlers.is_empty() {
            return Err(Box::new(PluginError::Config {
                message: format!("plugin '{}' does not declare supported CMF hooks", config.name),
            }));
        }

        let plugin: Arc<dyn Plugin> = plugin;
        Ok(PluginInstance { plugin, handlers })
    }
}

fn cmf_hook_name(hook: &str) -> Option<&'static str> {
    match hook {
        cmf_hook_names::TOOL_PRE_INVOKE => Some(cmf_hook_names::TOOL_PRE_INVOKE),
        cmf_hook_names::TOOL_POST_INVOKE => Some(cmf_hook_names::TOOL_POST_INVOKE),
        _ => None,
    }
}
