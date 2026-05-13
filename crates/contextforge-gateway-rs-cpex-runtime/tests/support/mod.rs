#![allow(dead_code, unused_imports, reason = "shared test fixture is used by separate integration test targets")]

mod config;
mod gateway;
mod plugin;
mod runtime;
mod tool;

pub(crate) use config::MemoryRuntimePluginConfigStore;
pub(crate) use gateway::start_gateway;
pub(crate) use plugin::{TestPlugin, TestPluginFactory};
pub(crate) use runtime::{plugin_config, runtime_with_post, runtime_with_pre, runtime_with_pre_and_post};
pub(crate) use tool::{error_code, sum_request, text};
