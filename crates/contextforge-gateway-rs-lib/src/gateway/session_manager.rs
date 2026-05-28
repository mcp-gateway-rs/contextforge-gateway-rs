use std::{collections::HashMap, sync::Arc};

use contextforge_gateway_rs_apis::user_store::VirtualHost;
use tokio::sync::{Mutex, broadcast};

use super::{
    backend_notifications::BackendNotification,
    mcp_call_validator::SessionKey,
    mcp_gateway::{BackendTransportKey, BackendTransportService, ServiceHolder},
};

pub struct SessionManager<'a> {
    virtual_host: &'a VirtualHost,
    session_key: &'a SessionKey,
    transports: &'a Arc<Mutex<HashMap<BackendTransportKey, BackendTransportService>>>,
}

impl<'a> SessionManager<'a> {
    pub fn new(
        virtual_host: &'a VirtualHost,
        session_key: &'a SessionKey,
        transports: &'a Arc<Mutex<HashMap<BackendTransportKey, BackendTransportService>>>,
    ) -> Self {
        Self { virtual_host, session_key, transports }
    }

    pub fn get_backend_names(&self) -> Vec<&str> {
        self.virtual_host.backends.keys().map(std::string::String::as_str).collect()
    }

    pub async fn borrow_transports(&self) -> Vec<ServiceHolder> {
        let transports = self.transports.lock().await;
        self.virtual_host
            .backends
            .keys()
            .filter_map(|name| {
                transports
                    .get(&BackendTransportKey::from((name.as_str(), self.session_key)))
                    .map(|b| ServiceHolder::new(name.clone(), b.service.clone()))
            })
            .collect()
    }

    pub(crate) async fn subscribe_backend_notifications(
        &self,
        backend_name: &str,
    ) -> Option<broadcast::Receiver<BackendNotification>> {
        let transports = self.transports.lock().await;
        transports
            .get(&BackendTransportKey::from((backend_name, self.session_key)))
            .map(|b| b.backend_notifications.subscribe())
    }

    pub(crate) async fn borrow_transport(&self, backend_name: &str) -> Option<ServiceHolder> {
        if !self.virtual_host.backends.contains_key(backend_name) {
            return None;
        }

        let transports = self.transports.lock().await;
        transports
            .get(&BackendTransportKey::from((backend_name, self.session_key)))
            .map(|b| ServiceHolder::new(backend_name.to_owned(), b.service.clone()))
    }
}
