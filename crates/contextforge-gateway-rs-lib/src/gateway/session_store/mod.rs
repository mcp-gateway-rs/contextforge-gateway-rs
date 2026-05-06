mod local_session_store;
mod redis_session_store;

use std::sync::Arc;

use async_trait::async_trait;

use serde::{Deserialize, Serialize};

//pub use inmemory_config_store::InMemoryUserSessionStore;
pub use local_session_store::LocalUserSessionStore;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SessionMap {
    upstream_session_id: Option<Arc<str>>,
    backend_name: String,
}

impl SessionMap {
    #[allow(dead_code, reason = "session mapping access is used by store implementations as they evolve")]
    pub fn session(&self) -> Option<Arc<str>> {
        self.upstream_session_id.clone()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SessionMapping {
    pub session_mapping: Vec<SessionMap>,
}

#[allow(dead_code, reason = "session mapping access is used by store implementations as they evolve")]
impl SessionMapping {
    pub fn new() -> Self {
        Self { session_mapping: vec![] }
    }

    pub fn push(&mut self, host: String, upstream_session: Option<&Arc<str>>) {
        self.session_mapping.push(SessionMap { upstream_session_id: upstream_session.cloned(), backend_name: host });
    }

    pub fn get<'a>(&'a self, host: &'a str) -> Option<&'a SessionMap> {
        self.session_mapping.iter().find(|m| m.backend_name == host)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, thiserror::Error)]
pub enum SessionStoreError {
    #[error("data store disconnected")]
    InvalidConnection,
    #[error("no data for key")]
    NoDataForKey,
    #[error("data in wrong format")]
    DataWrongFormat,
    #[error("unable to encode the data")]
    DataEncoding,
    #[error("unable to write to store")]
    CantWriteData,
}

#[derive(Debug, Clone, Deserialize, Serialize, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct UserSession {
    name: &'static str,
    principal: String,
    downstream_session_id: Arc<str>,
}

impl UserSession {
    pub fn new(principal: String, downstream_session_id: Arc<str>) -> Self {
        Self { name: "UserSession", principal, downstream_session_id }
    }
}

#[async_trait]
pub trait UserSessionStore: Send + Sync {
    async fn get_session<'a>(&self, key: &'a UserSession) -> Result<Option<SessionMapping>, SessionStoreError>;
    async fn set_session<'a>(
        &self,
        key: &'a UserSession,
        session_mapping: &'a SessionMapping,
    ) -> Result<(), SessionStoreError>;
}
