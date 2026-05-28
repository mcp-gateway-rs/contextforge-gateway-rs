mod local_session_store;
mod redis_session_store;

use std::sync::Arc;

use async_trait::async_trait;
//pub use inmemory_config_store::InMemoryUserSessionStore;
pub use local_session_store::LocalUserSessionStore;
use serde::{Deserialize, Serialize};

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

/// Ephemeral downstream-to-upstream session mapping key.
///
/// The virtual host is part of the encoded key so one downstream session id can
/// be reused safely across different gateway routes for the same principal.
#[derive(Debug, Clone, Deserialize, Serialize, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct UserSession {
    name: &'static str,
    principal: String,
    virtual_host_id: String,
    downstream_session_id: Arc<str>,
}

impl UserSession {
    pub fn new(principal: String, virtual_host_id: String, downstream_session_id: Arc<str>) -> Self {
        Self { name: "UserSession", principal, virtual_host_id, downstream_session_id }
    }
}

#[async_trait]
pub trait UserSessionStore: Send + Sync {
    async fn get_session<'a>(&self, key: &'a UserSession) -> Result<Option<SessionMapping>, SessionStoreError>;
    async fn has_session<'a>(&self, key: &'a UserSession) -> Result<bool, SessionStoreError> {
        self.get_session(key).await.map(|session| session.is_some())
    }
    async fn set_session<'a>(
        &self,
        key: &'a UserSession,
        session_mapping: &'a SessionMapping,
    ) -> Result<(), SessionStoreError>;
    async fn remove_session<'a>(&self, key: &'a UserSession) -> Result<(), SessionStoreError>;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{LocalUserSessionStore, SessionMapping, UserSession, UserSessionStore};

    #[test]
    fn user_session_scopes_virtual_host() {
        let session_id = "session".into();
        let key = UserSession::new("subject".to_owned(), "virtual-host".to_owned(), session_id);

        assert_ne!(key, UserSession::new("subject".to_owned(), "other-virtual-host".to_owned(), "session".into()));
    }

    #[test]
    fn user_session_messagepack_key_scopes_virtual_host() {
        let first_key = UserSession::new("subject".to_owned(), "first-host".to_owned(), "session".into());
        let second_key = UserSession::new("subject".to_owned(), "second-host".to_owned(), "session".into());
        let first_encoded = rmp_serde::to_vec(&first_key).expect("session key should encode");
        let second_encoded = rmp_serde::to_vec(&second_key).expect("session key should encode");

        assert_ne!(first_encoded, second_encoded);
    }

    #[tokio::test]
    async fn local_store_scopes_same_downstream_session_by_virtual_host() {
        let store = LocalUserSessionStore::new();
        let session_id: Arc<str> = "session".into();
        let first_key = UserSession::new("subject".to_owned(), "first-host".to_owned(), Arc::<str>::clone(&session_id));
        let second_key = UserSession::new("subject".to_owned(), "second-host".to_owned(), session_id);
        let mut first_mapping = SessionMapping::new();
        let mut second_mapping = SessionMapping::new();
        let first_upstream: Arc<str> = "first-upstream".into();
        let second_upstream: Arc<str> = "second-upstream".into();
        first_mapping.push("backend".to_owned(), Some(&first_upstream));
        second_mapping.push("backend".to_owned(), Some(&second_upstream));

        store.set_session(&first_key, &first_mapping).await.expect("first mapping should be stored");
        store.set_session(&second_key, &second_mapping).await.expect("second mapping should be stored");

        let loaded_first = store.get_session(&first_key).await.expect("store should read first mapping").unwrap();
        let loaded_second = store.get_session(&second_key).await.expect("store should read second mapping").unwrap();

        assert_eq!(loaded_first.get("backend").and_then(super::SessionMap::session), Some(first_upstream));
        assert_eq!(loaded_second.get("backend").and_then(super::SessionMap::session), Some(second_upstream));
    }
}
