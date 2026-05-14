use std::{
    collections::HashMap,
    hash::{BuildHasher, Hash},
    sync::Arc,
};

use contextforge_gateway_rs_apis::user_store::VirtualHost;
use tokio::sync::Mutex;
use tracing::{debug, info};

use super::mcp_gateway::{BackendTransportKey, BackendTransportService, ServiceHolder};
use crate::layers::session_id::SessionId;

pub(crate) fn borrow_transport_entry<K, V, Service, Hasher>(
    transports: &mut HashMap<K, V, Hasher>,
    key: &K,
    service_slot: impl FnOnce(&mut V) -> &mut Option<Service>,
) -> BorrowedTransport<Service>
where
    K: Eq + Hash,
    Hasher: BuildHasher,
{
    transports
        .get_mut(key)
        .map_or(BorrowedTransport::Missing, |entry| BorrowedTransport::Borrowed(service_slot(entry).take()))
}

pub(crate) enum BorrowedTransport<Service> {
    Borrowed(Option<Service>),
    Missing,
}

pub(crate) fn return_transport_entry<K, V, Service, Hasher>(
    transports: &mut HashMap<K, V, Hasher>,
    key: &K,
    running_service: Option<Service>,
    service_slot: impl FnOnce(&mut V) -> &mut Option<Service>,
) where
    K: Eq + Hash,
    Hasher: BuildHasher,
{
    if let Some(entry) = transports.get_mut(key) {
        *service_slot(entry) = running_service;
    }
}

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
                let key = BackendTransportKey::from((&name, self.session_id));
                match borrow_transport_entry(&mut transports, &key, |entry| &mut entry.service) {
                    BorrowedTransport::Borrowed(service) => Some(ServiceHolder::new(name, service)),
                    BorrowedTransport::Missing => None,
                }
            })
            .collect()
    }

    pub async fn return_transports(&self, backend_transports: impl Iterator<Item = ServiceHolder>) {
        let backend_transports = backend_transports.collect::<Vec<_>>();
        info!("Returning transports {:?} {backend_transports:?}", self.session_id);
        let mut transports = self.transports.lock().await;
        for svc_holder in backend_transports {
            let key = BackendTransportKey::from((&svc_holder.name, self.session_id));
            return_transport_entry(&mut transports, &key, svc_holder.running_service, |entry| &mut entry.service);
        }
    }

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

#[cfg(all(test, feature = "loom"))]
mod concurrency {
    use std::collections::HashMap;

    use loom::sync::{Arc, Mutex};
    use loom::thread;

    use super::{BorrowedTransport, borrow_transport_entry, return_transport_entry};

    #[derive(Clone, Debug, Hash, PartialEq, Eq)]
    struct TransportKey {
        backend: &'static str,
        session: &'static str,
    }

    #[derive(Debug)]
    struct TestServiceHolder {
        key: TransportKey,
        running_service: Option<usize>,
    }

    fn borrow_transports(
        transports: &Arc<Mutex<HashMap<TransportKey, Option<usize>>>>,
        keys: &[TransportKey],
    ) -> Vec<TestServiceHolder> {
        let mut transports = transports.lock().expect("transport mutex should not be poisoned");
        keys.iter()
            .filter_map(|key| match borrow_transport_entry(&mut transports, key, |service| service) {
                BorrowedTransport::Borrowed(running_service) => {
                    Some(TestServiceHolder { key: key.clone(), running_service })
                },
                BorrowedTransport::Missing => None,
            })
            .collect()
    }

    fn return_transports(
        transports: &Arc<Mutex<HashMap<TransportKey, Option<usize>>>>,
        holders: Vec<TestServiceHolder>,
    ) {
        let mut transports = transports.lock().expect("transport mutex should not be poisoned");
        for holder in holders {
            return_transport_entry(&mut transports, &holder.key, holder.running_service, |service| service);
        }
    }

    fn cleanup_transports(transports: &Arc<Mutex<HashMap<TransportKey, Option<usize>>>>, keys: &[TransportKey]) {
        let mut transports = transports.lock().expect("transport mutex should not be poisoned");
        for key in keys {
            transports.remove(key);
        }
    }

    #[test]
    fn concurrent_borrowers_do_not_erase_returned_transport() {
        loom::model(|| {
            let key = TransportKey { backend: "backend-a", session: "session-one" };
            let transports = Arc::new(Mutex::new(HashMap::from([(key.clone(), Some(7))])));
            let keys = [key.clone()];

            let first_transports = Arc::clone(&transports);
            let first_keys = keys.clone();
            let first = thread::spawn(move || {
                let borrowed = borrow_transports(&first_transports, &first_keys);
                thread::yield_now();
                return_transports(&first_transports, borrowed);
            });

            let second_transports = Arc::clone(&transports);
            let second = thread::spawn(move || {
                let borrowed = borrow_transports(&second_transports, &keys);
                thread::yield_now();
                return_transports(&second_transports, borrowed);
            });

            first.join().expect("first borrower should finish");
            second.join().expect("second borrower should finish");

            let final_service = transports.lock().expect("transport mutex should not be poisoned").get(&key).copied();
            assert_eq!(final_service, Some(Some(7)));
        });
    }

    #[test]
    fn cleanup_does_not_resurrect_returned_transport() {
        loom::model(|| {
            let key = TransportKey { backend: "backend-a", session: "session-one" };
            let transports = Arc::new(Mutex::new(HashMap::from([(key.clone(), Some(7))])));
            let keys = [key.clone()];

            let borrower_transports = Arc::clone(&transports);
            let borrower_keys = keys.clone();
            let borrower = thread::spawn(move || {
                let borrowed = borrow_transports(&borrower_transports, &borrower_keys);
                thread::yield_now();
                return_transports(&borrower_transports, borrowed);
            });

            let cleanup_transports_ref = Arc::clone(&transports);
            let cleanup = thread::spawn(move || {
                thread::yield_now();
                cleanup_transports(&cleanup_transports_ref, &keys);
            });

            borrower.join().expect("borrower should finish");
            cleanup.join().expect("cleanup should finish");

            let final_service = transports.lock().expect("transport mutex should not be poisoned").get(&key).copied();
            assert_ne!(final_service, Some(None));
        });
    }

    #[test]
    fn different_sessions_do_not_interfere() {
        loom::model(|| {
            let first_key = TransportKey { backend: "backend-a", session: "session-one" };
            let second_key = TransportKey { backend: "backend-a", session: "session-two" };
            let transports =
                Arc::new(Mutex::new(HashMap::from([(first_key.clone(), Some(7)), (second_key.clone(), Some(11))])));

            let first_transports = Arc::clone(&transports);
            let first_keys = [first_key.clone()];
            let first = thread::spawn(move || {
                let borrowed = borrow_transports(&first_transports, &first_keys);
                thread::yield_now();
                return_transports(&first_transports, borrowed);
            });

            let second_transports = Arc::clone(&transports);
            let second_keys = [second_key.clone()];
            let second = thread::spawn(move || {
                let borrowed = borrow_transports(&second_transports, &second_keys);
                thread::yield_now();
                return_transports(&second_transports, borrowed);
            });

            first.join().expect("first session should finish");
            second.join().expect("second session should finish");

            let transports = transports.lock().expect("transport mutex should not be poisoned");
            assert_eq!(transports.get(&first_key).copied(), Some(Some(7)));
            assert_eq!(transports.get(&second_key).copied(), Some(Some(11)));
        });
    }

    #[test]
    fn multiple_backends_preserve_unborrowed_services() {
        loom::model(|| {
            let borrowed_key = TransportKey { backend: "backend-a", session: "session-one" };
            let unborrowed_key = TransportKey { backend: "backend-b", session: "session-one" };
            let transports = Arc::new(Mutex::new(HashMap::from([
                (borrowed_key.clone(), Some(7)),
                (unborrowed_key.clone(), Some(11)),
            ])));
            let keys = [borrowed_key.clone()];

            let borrower_transports = Arc::clone(&transports);
            let borrower = thread::spawn(move || {
                let borrowed = borrow_transports(&borrower_transports, &keys);
                thread::yield_now();
                return_transports(&borrower_transports, borrowed);
            });

            borrower.join().expect("borrower should finish");

            let transports = transports.lock().expect("transport mutex should not be poisoned");
            assert_eq!(transports.get(&borrowed_key).copied(), Some(Some(7)));
            assert_eq!(transports.get(&unborrowed_key).copied(), Some(Some(11)));
        });
    }
}
