#![allow(dead_code, reason = "shared CPEX test fixture is used by separate integration test targets")]

#[path = "../../../contextforge-gateway-rs-cpex/tests/support/config.rs"]
mod config;
mod gateway;
#[path = "../../../contextforge-gateway-rs-cpex/tests/support/plugin.rs"]
mod plugin;
#[path = "../../../contextforge-gateway-rs-cpex/tests/support/runtime.rs"]
mod runtime;
#[path = "../../../contextforge-gateway-rs-cpex/tests/support/tool.rs"]
mod tool;

pub(crate) use config::MemoryRuntimePluginConfigStore;
pub(crate) use gateway::start_gateway;
pub(crate) use plugin::{TestPlugin, TestPluginFactory};
pub(crate) use runtime::{runtime_with_post, runtime_with_pre, runtime_with_pre_and_post};
pub(crate) use tool::{error_code, sum_request, text};
