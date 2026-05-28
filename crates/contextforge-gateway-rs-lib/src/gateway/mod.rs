mod backend_notifications;
mod mcp_call_validator;
pub(crate) mod mcp_gateway;
mod session_manager;
mod session_store;

pub use mcp_gateway::{LocalUserSessionStore, McpService};
pub(crate) use mcp_gateway::{SessionCleanupRegistry, cleanup_registered_session_or_owner};
pub(crate) use session_store::{UserSession, UserSessionStore};
