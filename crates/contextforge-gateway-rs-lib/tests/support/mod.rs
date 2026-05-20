#![allow(dead_code, unused_imports, reason = "shared CPEX test fixture is used by separate integration test targets")]

mod auth;
mod gateway;
pub(crate) mod mock_counter;
mod plugin;
mod runtime;
mod tool;
mod user_config_store;

pub(crate) use auth::token;
pub(crate) use gateway::start_gateway;
pub(crate) use plugin::{
    POST_DENY_ERROR_CODE, PRE_DENY_ERROR_CODE, REWRITTEN_SUM_A, REWRITTEN_SUM_B, TestPlugin, TestPluginFactory,
};
pub(crate) use runtime::{runtime_with_post, runtime_with_pre, runtime_with_pre_and_post};
pub(crate) use tool::{error_code, sum_request, text};
pub(crate) use user_config_store::MemoryUserConfigStore;
