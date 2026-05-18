use std::{collections::HashMap, sync::Arc};

use contextforge_gateway_rs_apis::user_store::VirtualHost;
use tokio::sync::Mutex;
use tracing::{debug, info};

use super::mcp_gateway::{BackendTransportKey, BackendTransportService, ServiceHolder};
use crate::layers::session_id::SessionId;

pub struct SessionManager<'a> {
    virtual_host: &'a VirtualHost,
    session_id: &'a SessionId,
    transports: &'a Arc<Mutex<HashMap<BackendTransportKey, BackendTransportService>>>,
}

impl<'a> SessionManager<'a> {
    pub fn new(
        virtual_host: &'a VirtualHost,
        session_id: &'a SessionId,
        transports: &'a Arc<Mutex<HashMap<BackendTransportKey, BackendTransportService>>>,
    ) -> Self {
        Self { virtual_host, session_id, transports }
    }

    pub fn get_backend_names(&self) -> Vec<&str> {
        self.virtual_host.backends.keys().map(std::string::String::as_str).collect()
    }

    pub async fn borrow_transports(&self) -> Vec<ServiceHolder> {
        let names: Vec<_> = self.virtual_host.backends.keys().cloned().collect();
        let mut transports = self.transports.lock().await;
        names
            .into_iter()
            .filter_map(|name| {
                transports
                    .get_mut(&BackendTransportKey::from((&name, self.session_id)))
                    .map(|b| ServiceHolder::new(name, b.service.clone()))
            })
            .collect()
    }

    // pub async fn return_transports(&self, backend_transports: impl Iterator<Item = ServiceHolder>) {
    //     let backend_transports = backend_transports.collect::<Vec<_>>();
    //     info!("Returning transports {:?} {backend_transports:?}", self.session_id);
    //     let mut transports = self.transports.lock().await;
    //     for svc_holder in backend_transports {
    //         transports
    //             .entry(BackendTransportKey::from((&svc_holder.name, self.session_id)))
    //             .and_modify(|e| e.service = svc_holder.running_service);
    //     }
    // }

    pub async fn cleanup_backends(&self, reason: &'static str) {
        let names: Vec<_> = self.virtual_host.backends.keys().cloned().collect();
        info!("Cleaning up backends {:?}", self.session_id);
        let mut transports = self.transports.lock().await;
        for name in names {
            let key = BackendTransportKey::from((&name, self.session_id));
            debug!("session_manager: removing transport for {key:?} {reason}");
            transports.remove(&key);
        }
    }
}
