use contextforge_gateway_rs_cpex_runtime::{CmfPluginFactory, CpexRuntimeRegistry};

pub fn register(runtime: &mut CpexRuntimeRegistry) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    runtime.register_factory(
        "contextforge/payload-marker",
        Box::new(CmfPluginFactory::new(cpex_payload_marker::PayloadMarkerPlugin::new)),
    )?;
    runtime.register_factory(
        "contextforge/text-prefixer",
        Box::new(CmfPluginFactory::new(cpex_text_prefixer::TextPrefixerPlugin::new)),
    )?;
    runtime.register_factory(
        "contextforge/tool-namespace",
        Box::new(CmfPluginFactory::new(cpex_tool_namespace::ToolNamespacePlugin::new)),
    )?;
    Ok(())
}
