use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use contextforge_gateway_rs_cpex::GatewayPluginRuntimeHandle;
use rmcp::{
    ErrorData, RoleServer,
    model::{ProgressToken, ServerCapabilities},
    service::Peer,
};
use tokio::sync::{Mutex, RwLock, broadcast};
use typed_builder::TypedBuilder;

use super::{
    backend_notifications::{
        BackendClientService, BackendNotification, LogLevels, ProgressTokens, ResourceSubscriptions,
        SessionNotificationRelay, SessionRelayExit, drain_buffered_session_notifications, relay_backend_notifications,
    },
    mcp_call_validator::SessionKey,
};
pub use crate::gateway::session_store::LocalUserSessionStore;
use crate::gateway::{
    session_manager::SessionManager,
    session_store::{SessionStoreError, UserSession, UserSessionStore},
};

mod lifecycle;

mod handlers;
mod routing;

use routing::{ResourceSubscriptionAction, forward_resource_subscription, remove_resource_subscription};

pub(crate) use lifecycle::cleanup_registered_session_or_owner;
use lifecycle::{
    cleanup_finished_notification_relay, cleanup_notification_state, cleanup_pending_initialized_session,
    discard_buffered_notifications,
};

const BACKEND_NOTIFICATION_BUFFER_CAPACITY: usize = 64;
const MAX_BUFFERED_NOTIFICATIONS_AFTER_RELAY_STOP: usize = 16;
#[cfg(not(test))]
const UPSTREAM_NOTIFICATION_CONTROL_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(test)]
const UPSTREAM_NOTIFICATION_CONTROL_TIMEOUT: Duration = Duration::from_millis(10);
#[cfg(not(test))]
const PENDING_INITIALIZED_RELAY_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(test)]
const PENDING_INITIALIZED_RELAY_TIMEOUT: Duration = Duration::from_millis(10);

#[derive(Clone, TypedBuilder)]
#[builder(field_defaults(setter(prefix = "with_")))]
pub struct McpService<T>
where
    T: UserSessionStore,
{
    #[builder(default = Arc::new(RwLock::new(HashMap::new())), setter(skip))]
    subscriptions: ResourceSubscriptions,
    #[builder(default = Arc::new(RwLock::new(HashMap::new())), setter(skip))]
    log_levels: LogLevels,
    #[builder(default = Arc::new(RwLock::new(HashMap::new())), setter(skip))]
    progress_tokens: ProgressTokens,
    #[builder(default = Arc::new(Mutex::new(HashMap::new())), setter(skip))]
    transports: Arc<Mutex<HashMap<BackendTransportKey, BackendTransportService>>>,
    #[builder(default = Arc::new(Mutex::new(HashMap::new())), setter(skip))]
    transport_session_keys: TransportSessionKeys,
    #[builder(default = Arc::new(Mutex::new(NotificationRelayState::default())), setter(skip))]
    notification_relay_state: Arc<Mutex<NotificationRelayState>>,
    #[builder(default = Arc::new(Mutex::new(HashMap::new())), setter(skip))]
    pending_notification_relays: PendingNotificationRelays,
    #[builder(default = Arc::new(Mutex::new(HashMap::new())), setter(skip))]
    subscription_change_locks: SubscriptionChangeLocks,
    #[builder(default = Arc::new(Mutex::new(HashMap::new())))]
    session_cleanup_registry: SessionCleanupRegistry,
    #[builder(default = Arc::new(AtomicU64::new(1)), setter(skip))]
    notification_relay_generation: Arc<AtomicU64>,
    #[builder(default = UPSTREAM_NOTIFICATION_CONTROL_TIMEOUT)]
    upstream_notification_control_timeout: Duration,
    http_client: reqwest::Client,
    user_session_store: T,
    #[builder(default)]
    plugin_runtime: Option<GatewayPluginRuntimeHandle>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BackendTransportKey {
    backend_name: String,
    session_key: SessionKey,
}

#[derive(Debug)]
pub struct ServiceHolder {
    pub name: String,
    pub running_service: Option<BackendClientService>,
}

impl ServiceHolder {
    pub fn new(name: String, running_service: Option<BackendClientService>) -> ServiceHolder {
        Self { name, running_service }
    }
}

#[derive(Debug)]
pub struct BackendTransportService {
    #[expect(dead_code, reason = "stored backend capabilities are kept with transport state for future routing")]
    capabilities: Option<ServerCapabilities>,
    pub(crate) service: Option<BackendClientService>,
    pub(crate) backend_notifications: broadcast::Sender<BackendNotification>,
}

#[derive(Debug)]
struct NotificationRelayHandle {
    generation: u64,
    handle: tokio::task::JoinHandle<()>,
}

#[derive(Debug, Default)]
struct NotificationRelayState {
    relays: HashMap<BackendTransportKey, NotificationRelayHandle>,
}

struct PendingNotificationRelay {
    backend_name: String,
    notification_rx: broadcast::Receiver<BackendNotification>,
    downstream_peer: Peer<RoleServer>,
}

struct PendingNotificationRelaysForSession {
    relays: Vec<PendingNotificationRelay>,
    cleanup_handle: tokio::task::JoinHandle<()>,
}

type SubscriptionChangeLocks = Arc<Mutex<HashMap<BackendTransportKey, Arc<Mutex<()>>>>>;
type TransportSessionKeys = Arc<Mutex<HashMap<SessionKey, HashSet<BackendTransportKey>>>>;
type PendingNotificationRelays = Arc<Mutex<HashMap<SessionKey, PendingNotificationRelaysForSession>>>;
pub(crate) type SessionCleanupRegistry = Arc<Mutex<HashMap<UserSession, SessionCleanupHandle>>>;

#[derive(Clone)]
pub(crate) struct SessionCleanupHandle {
    session_key: SessionKey,
    subscriptions: ResourceSubscriptions,
    log_levels: LogLevels,
    progress_tokens: ProgressTokens,
    transports: Arc<Mutex<HashMap<BackendTransportKey, BackendTransportService>>>,
    transport_session_keys: TransportSessionKeys,
    notification_relay_state: Arc<Mutex<NotificationRelayState>>,
    pending_notification_relays: PendingNotificationRelays,
    subscription_change_locks: SubscriptionChangeLocks,
    user_session_store: Arc<dyn UserSessionStore>,
}

struct NotificationRelayCleanup<'a> {
    subscriptions: &'a ResourceSubscriptions,
    log_levels: &'a LogLevels,
    progress_tokens: &'a ProgressTokens,
    transports: &'a Arc<Mutex<HashMap<BackendTransportKey, BackendTransportService>>>,
    transport_session_keys: &'a TransportSessionKeys,
    notification_relay_state: &'a Arc<Mutex<NotificationRelayState>>,
    session_cleanup_registry: &'a SessionCleanupRegistry,
    subscription_change_locks: &'a SubscriptionChangeLocks,
    user_session_store: &'a dyn UserSessionStore,
}

struct PendingInitializedCleanup<'a> {
    transports: &'a Arc<Mutex<HashMap<BackendTransportKey, BackendTransportService>>>,
    transport_session_keys: &'a TransportSessionKeys,
    session_cleanup_registry: &'a SessionCleanupRegistry,
    subscription_change_locks: &'a SubscriptionChangeLocks,
    user_session_store: &'a dyn UserSessionStore,
    session_key: &'a SessionKey,
}

struct NotificationRelayResume<'a, 'b> {
    session_manager: &'a SessionManager<'b>,
    notification_rx: Option<broadcast::Receiver<BackendNotification>>,
    backend_name: String,
    session_key: SessionKey,
    downstream_peer: Peer<RoleServer>,
}

struct BufferedNotificationRelay {
    notification_rx: broadcast::Receiver<BackendNotification>,
    key: BackendTransportKey,
    backend_name: String,
    session_key: SessionKey,
    downstream_peer: Peer<RoleServer>,
}

struct ResourceSubscriptionChange<'a, 'b> {
    session_manager: &'a SessionManager<'b>,
    backend_name: String,
    backend_resource_uri: String,
    public_resource_uri: String,
    session_key: SessionKey,
    downstream_peer: Peer<RoleServer>,
    action: ResourceSubscriptionAction,
    timeout: Duration,
}

impl From<(&str, &SessionKey)> for BackendTransportKey {
    fn from((backend_name, session_key): (&str, &SessionKey)) -> Self {
        Self { backend_name: backend_name.to_owned(), session_key: session_key.clone() }
    }
}

impl<T> McpService<T>
where
    T: UserSessionStore + Clone + 'static,
{
    async fn activate_session_relays(&self, session_key: &SessionKey) {
        self.start_pending_notification_relays(session_key).await;
    }

    async fn track_progress_token(
        &self,
        session_key: &SessionKey,
        backend_name: &str,
        downstream_token: ProgressToken,
    ) {
        let mut progress_tokens = self.progress_tokens.write().await;
        progress_tokens
            .entry(session_key.clone())
            .or_default()
            .entry(backend_name.to_owned())
            .or_default()
            .insert(downstream_token);
    }

    async fn apply_resource_subscription_change(
        &self,
        change: ResourceSubscriptionChange<'_, '_>,
    ) -> Result<(), ErrorData> {
        let key = BackendTransportKey::from((change.backend_name.as_str(), &change.session_key));
        let change_lock = self.subscription_change_lock(&key).await;
        let _change_guard = change_lock.lock().await;
        let mut buffered_notifications =
            change.session_manager.subscribe_backend_notifications(&change.backend_name).await;
        self.stop_notification_relay(&change.backend_name, &change.session_key).await;
        if let Some(notification_rx) = buffered_notifications.as_mut() {
            discard_buffered_notifications(notification_rx);
        }

        let result = forward_resource_subscription(
            change.session_manager,
            &change.backend_name,
            &change.backend_resource_uri,
            change.action,
            change.timeout,
        )
        .await;

        match (change.action, &result) {
            (ResourceSubscriptionAction::Subscribe, Ok(())) => {
                self.subscriptions
                    .write()
                    .await
                    .entry(change.session_key.clone())
                    .or_default()
                    .entry(change.backend_name.clone())
                    .or_default()
                    .insert(change.public_resource_uri);
            },
            (ResourceSubscriptionAction::Unsubscribe, Ok(())) => {
                remove_resource_subscription(
                    &self.subscriptions,
                    &change.session_key,
                    &change.backend_name,
                    &change.public_resource_uri,
                )
                .await;
            },
            _ => {},
        }

        self.resume_notification_relay(NotificationRelayResume {
            session_manager: change.session_manager,
            notification_rx: buffered_notifications,
            backend_name: change.backend_name,
            session_key: change.session_key,
            downstream_peer: change.downstream_peer,
        })
        .await;
        result
    }

    async fn ensure_notification_relay(
        &self,
        session_manager: &SessionManager<'_>,
        backend_name: &str,
        session_key: &SessionKey,
        downstream_peer: Peer<RoleServer>,
    ) {
        let key = BackendTransportKey::from((backend_name, session_key));
        if self.notification_relay_is_active(&key).await || self.subscription_change_in_progress(&key).await {
            return;
        }
        self.start_pending_notification_relays(session_key).await;
        if self.notification_relay_is_active(&key).await {
            return;
        }
        let Some(notification_rx) = session_manager.subscribe_backend_notifications(backend_name).await else {
            return;
        };
        self.spawn_notification_relay(notification_rx, backend_name.to_owned(), session_key.clone(), downstream_peer)
            .await;
    }

    async fn store_pending_notification_relays(
        &self,
        relay_receivers: Vec<(String, broadcast::Receiver<BackendNotification>)>,
        session_key: &SessionKey,
        downstream_peer: Peer<RoleServer>,
    ) {
        if relay_receivers.is_empty() {
            return;
        }
        let pending_relays = relay_receivers
            .into_iter()
            .map(|(backend_name, notification_rx)| PendingNotificationRelay {
                backend_name,
                notification_rx,
                downstream_peer: downstream_peer.clone(),
            })
            .collect();
        let cleanup_handle = self.spawn_pending_initialized_cleanup(session_key.clone());
        if let Some(replaced) = self
            .pending_notification_relays
            .lock()
            .await
            .insert(session_key.clone(), PendingNotificationRelaysForSession { relays: pending_relays, cleanup_handle })
        {
            replaced.cleanup_handle.abort();
        }
    }

    fn spawn_pending_initialized_cleanup(&self, session_key: SessionKey) -> tokio::task::JoinHandle<()> {
        let pending_notification_relays = Arc::clone(&self.pending_notification_relays);
        let transports = Arc::clone(&self.transports);
        let transport_session_keys = Arc::clone(&self.transport_session_keys);
        let session_cleanup_registry = Arc::clone(&self.session_cleanup_registry);
        let subscription_change_locks = Arc::clone(&self.subscription_change_locks);
        let user_session_store = self.user_session_store.clone();
        tokio::spawn(async move {
            tokio::time::sleep(PENDING_INITIALIZED_RELAY_TIMEOUT).await;
            {
                let mut pending = pending_notification_relays.lock().await;
                if pending.remove(&session_key).is_none() {
                    return;
                }
            }
            cleanup_pending_initialized_session(PendingInitializedCleanup {
                transports: &transports,
                transport_session_keys: &transport_session_keys,
                session_cleanup_registry: &session_cleanup_registry,
                subscription_change_locks: &subscription_change_locks,
                user_session_store: &user_session_store,
                session_key: &session_key,
            })
            .await;
        })
    }

    async fn start_pending_notification_relays(&self, session_key: &SessionKey) {
        let pending = self.pending_notification_relays.lock().await.remove(session_key);
        let Some(pending) = pending else {
            return;
        };
        pending.cleanup_handle.abort();
        for pending_relay in pending.relays {
            self.spawn_notification_relay(
                pending_relay.notification_rx,
                pending_relay.backend_name,
                session_key.clone(),
                pending_relay.downstream_peer,
            )
            .await;
        }
    }

    async fn resume_notification_relay(&self, resume: NotificationRelayResume<'_, '_>) {
        let key = BackendTransportKey::from((resume.backend_name.as_str(), &resume.session_key));
        if let Some(notification_rx) = resume.notification_rx {
            self.drain_then_spawn_notification_relay(BufferedNotificationRelay {
                notification_rx,
                key,
                backend_name: resume.backend_name,
                session_key: resume.session_key,
                downstream_peer: resume.downstream_peer,
            })
            .await;
        } else {
            self.ensure_notification_relay(
                resume.session_manager,
                &resume.backend_name,
                &resume.session_key,
                resume.downstream_peer,
            )
            .await;
        }
    }

    async fn drain_then_spawn_notification_relay(&self, buffered_relay: BufferedNotificationRelay) {
        let notification_rx = match drain_buffered_session_notifications(SessionNotificationRelay {
            notification_rx: buffered_relay.notification_rx,
            backend_name: buffered_relay.backend_name.clone(),
            downstream_peer: buffered_relay.downstream_peer.clone(),
            resource_subscriptions: Arc::clone(&self.subscriptions),
            log_levels: Arc::clone(&self.log_levels),
            progress_tokens: Arc::clone(&self.progress_tokens),
            session_key: buffered_relay.session_key.clone(),
        })
        .await
        {
            Ok(notification_rx) => notification_rx,
            Err(SessionRelayExit::DownstreamClosed) => {
                self.cleanup_session_state(&buffered_relay.key.session_key).await;
                return;
            },
            Err(SessionRelayExit::BackendClosed | SessionRelayExit::NotificationForwardFailed) => return,
        };
        self.spawn_notification_relay_locked(
            notification_rx,
            buffered_relay.backend_name,
            buffered_relay.session_key,
            buffered_relay.downstream_peer,
        )
        .await;
    }
    async fn spawn_notification_relay(
        &self,
        notification_rx: broadcast::Receiver<BackendNotification>,
        backend_name: String,
        session_key: SessionKey,
        downstream_peer: Peer<RoleServer>,
    ) {
        let key = BackendTransportKey::from((backend_name.as_str(), &session_key));
        let change_lock = self.subscription_change_lock(&key).await;
        let Ok(_change_guard) = change_lock.try_lock() else {
            return;
        };
        self.spawn_notification_relay_locked(notification_rx, backend_name, session_key, downstream_peer).await;
    }

    async fn spawn_notification_relay_locked(
        &self,
        notification_rx: broadcast::Receiver<BackendNotification>,
        backend_name: String,
        session_key: SessionKey,
        downstream_peer: Peer<RoleServer>,
    ) {
        let key = BackendTransportKey::from((backend_name.as_str(), &session_key));
        let subscriptions = Arc::clone(&self.subscriptions);
        let log_levels = Arc::clone(&self.log_levels);
        let progress_tokens = Arc::clone(&self.progress_tokens);
        let transports = Arc::clone(&self.transports);
        let transport_session_keys = Arc::clone(&self.transport_session_keys);
        let notification_relay_state = Arc::clone(&self.notification_relay_state);
        let session_cleanup_registry = Arc::clone(&self.session_cleanup_registry);
        let subscription_change_locks = Arc::clone(&self.subscription_change_locks);
        let user_session_store = self.user_session_store.clone();
        let generation = self.notification_relay_generation.fetch_add(1, Ordering::Relaxed);
        let key_for_task = key.clone();
        let mut state = self.notification_relay_state.lock().await;
        if state.relays.get(&key).is_some_and(|relay| !relay.handle.is_finished()) {
            return;
        }
        state.relays.remove(&key);
        let handle = tokio::spawn(async move {
            let relay_subscriptions = Arc::clone(&subscriptions);
            let relay_log_levels = Arc::clone(&log_levels);
            let relay_progress_tokens = Arc::clone(&progress_tokens);
            let exit = relay_backend_notifications(SessionNotificationRelay {
                notification_rx,
                backend_name,
                downstream_peer,
                resource_subscriptions: relay_subscriptions,
                log_levels: relay_log_levels,
                progress_tokens: relay_progress_tokens,
                session_key,
            })
            .await;
            cleanup_finished_notification_relay(
                NotificationRelayCleanup {
                    subscriptions: &subscriptions,
                    log_levels: &log_levels,
                    progress_tokens: &progress_tokens,
                    transports: &transports,
                    transport_session_keys: &transport_session_keys,
                    notification_relay_state: &notification_relay_state,
                    session_cleanup_registry: &session_cleanup_registry,
                    subscription_change_locks: &subscription_change_locks,
                    user_session_store: &user_session_store,
                },
                &key_for_task,
                generation,
                exit,
            )
            .await;
        });
        state.relays.insert(key, NotificationRelayHandle { generation, handle });
    }

    async fn notification_relay_is_active(&self, key: &BackendTransportKey) -> bool {
        let mut state = self.notification_relay_state.lock().await;
        if state.relays.get(key).is_some_and(|relay| !relay.handle.is_finished()) {
            return true;
        }
        state.relays.remove(key);
        false
    }

    async fn subscription_change_lock(&self, key: &BackendTransportKey) -> Arc<Mutex<()>> {
        Arc::clone(
            self.subscription_change_locks.lock().await.entry(key.clone()).or_insert_with(|| Arc::new(Mutex::new(()))),
        )
    }

    async fn subscription_change_in_progress(&self, key: &BackendTransportKey) -> bool {
        let locks = self.subscription_change_locks.lock().await;
        locks.get(key).is_some_and(|lock| lock.try_lock().is_err())
    }

    async fn stop_notification_relay(&self, backend_name: &str, session_key: &SessionKey) {
        let key = BackendTransportKey::from((backend_name, session_key));
        let relay = self.notification_relay_state.lock().await.relays.remove(&key);
        if let Some(relay) = relay {
            relay.handle.abort();
            let _ = relay.handle.await;
        }
    }

    async fn cleanup_notification_state(&self, session_key: &SessionKey) {
        cleanup_notification_state(&self.subscriptions, &self.log_levels, &self.notification_relay_state, session_key)
            .await;
        self.progress_tokens.write().await.remove(session_key);
        if let Some(pending) = self.pending_notification_relays.lock().await.remove(session_key) {
            pending.cleanup_handle.abort();
        }
        self.subscription_change_locks.lock().await.retain(|key, _| &key.session_key != session_key);
    }

    async fn cleanup_transport_state(&self, session_key: &SessionKey) {
        let keys = self.transport_session_keys.lock().await.remove(session_key).unwrap_or_default();
        let mut transports = self.transports.lock().await;
        for key in keys {
            transports.remove(&key);
        }
    }

    async fn cleanup_session_state(&self, session_key: &SessionKey) {
        self.session_cleanup_registry.lock().await.remove(&session_key.to_user_session());
        self.cleanup_notification_state(session_key).await;
        self.cleanup_transport_state(session_key).await;
        let _ = self.user_session_store.remove_session(&session_key.to_user_session()).await;
    }

    async fn register_session_cleanup(&self, session_key: &SessionKey) {
        self.session_cleanup_registry.lock().await.insert(
            session_key.to_user_session(),
            SessionCleanupHandle {
                session_key: session_key.clone(),
                subscriptions: Arc::clone(&self.subscriptions),
                log_levels: Arc::clone(&self.log_levels),
                progress_tokens: Arc::clone(&self.progress_tokens),
                transports: Arc::clone(&self.transports),
                transport_session_keys: Arc::clone(&self.transport_session_keys),
                notification_relay_state: Arc::clone(&self.notification_relay_state),
                pending_notification_relays: Arc::clone(&self.pending_notification_relays),
                subscription_change_locks: Arc::clone(&self.subscription_change_locks),
                user_session_store: Arc::new(self.user_session_store.clone()),
            },
        );
    }
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use rmcp::model::LoggingLevel;

    use crate::gateway::session_store::SessionMapping;

    use super::lifecycle::remove_progress_token;
    use super::routing::{
        BackendResourcePair, BackendToolPair, merge_capabilities, split_resource_name, split_tool_name,
    };
    use super::*;

    fn test_subscriptions(session_key: &SessionKey) -> ResourceSubscriptions {
        Arc::new(RwLock::new(HashMap::from([(
            session_key.clone(),
            HashMap::from([(
                "backend".to_owned(),
                std::collections::HashSet::from(["backend-memo://insights".to_owned()]),
            )]),
        )])))
    }

    fn test_transport_session_keys(
        session_key: &SessionKey,
        keys: impl IntoIterator<Item = BackendTransportKey>,
    ) -> TransportSessionKeys {
        Arc::new(Mutex::new(HashMap::from([(session_key.clone(), keys.into_iter().collect())])))
    }

    fn test_session_cleanup_registry() -> SessionCleanupRegistry {
        Arc::new(Mutex::new(HashMap::new()))
    }

    #[test]
    fn test_splitting() {
        let tool_name = "counter-one-increment";
        let backend_names = vec!["counter-on", "counter-oneee", "counter-one"];
        let pair = BackendToolPair { backend_name: "counter-one", tool_name: "increment" };
        assert_eq!(Some(pair), split_tool_name(&tool_name, &backend_names));
        let tool_name = "counter-oneincrement";
        assert_eq!(None, split_tool_name(&tool_name, &backend_names));
        let tool_name = "counteroneincrement";
        assert_eq!(None, split_tool_name(&tool_name, &backend_names));
        let tool_name = "counter-one-get-value";
        let pair = BackendToolPair { backend_name: "counter-one", tool_name: "get-value" };
        assert_eq!(Some(pair), split_tool_name(&tool_name, &backend_names));

        let backend_names = vec!["counter_on", "counter_oneee", "counter_one"];
        let tool_name = "counter_one-get-value";
        let pair = BackendToolPair { backend_name: "counter_one", tool_name: "get-value" };
        assert_eq!(Some(pair), split_tool_name(&tool_name, &backend_names));

        let backend_names = vec!["counter", "counter-one"];
        let tool_name = "counter-one-get-value";
        let pair = BackendToolPair { backend_name: "counter-one", tool_name: "get-value" };
        assert_eq!(Some(pair), split_tool_name(&tool_name, &backend_names));

        let resource_uri = "counter-one-memo://insights";
        let pair = BackendResourcePair { backend_name: "counter-one", resource_uri: "memo://insights" };
        assert_eq!(Some(pair), split_resource_name(&resource_uri, &backend_names));

        let resource_uri = "unknown-memo://insights";
        assert_eq!(None, split_resource_name(&resource_uri, &backend_names));
    }

    #[test]
    fn merge_capabilities_advertises_relayed_notifications() {
        let backend_capabilities = ServerCapabilities::builder()
            .enable_prompts()
            .enable_prompts_list_changed()
            .enable_resources()
            .enable_resources_subscribe()
            .enable_resources_list_changed()
            .enable_tools()
            .enable_tool_list_changed()
            .build();
        let capabilities = merge_capabilities(&[Some(backend_capabilities)]);

        assert_eq!(capabilities.prompts.as_ref().and_then(|c| c.list_changed), None);
        assert_eq!(capabilities.resources.as_ref().and_then(|c| c.subscribe), Some(true));
        assert_eq!(capabilities.resources.as_ref().and_then(|c| c.list_changed), Some(true));
        assert_eq!(capabilities.tools.as_ref().and_then(|c| c.list_changed), Some(true));
    }

    #[test]
    fn merge_capabilities_omits_unsupported_backend_notifications() {
        let capabilities = merge_capabilities(&[]);

        assert_eq!(capabilities.prompts.as_ref().and_then(|c| c.list_changed), None);
        assert_eq!(capabilities.resources.as_ref().and_then(|c| c.subscribe), None);
        assert_eq!(capabilities.resources.as_ref().and_then(|c| c.list_changed), None);
        assert_eq!(capabilities.tools.as_ref().and_then(|c| c.list_changed), None);
    }

    #[tokio::test]
    async fn cleanup_notification_state_removes_session_entries() {
        let session_key = SessionKey::new("subject", "virtual-host", "session-a");
        let subscriptions = test_subscriptions(&session_key);
        let log_levels = Arc::new(RwLock::new(HashMap::from([(
            session_key.clone(),
            HashMap::from([("backend".to_owned(), LoggingLevel::Error)]),
        )])));
        let key = BackendTransportKey::from(("backend", &session_key));
        let notification_relay_state = Arc::new(Mutex::new(NotificationRelayState {
            relays: HashMap::from([(
                key.clone(),
                NotificationRelayHandle { generation: 1, handle: tokio::spawn(async {}) },
            )]),
        }));

        cleanup_notification_state(&subscriptions, &log_levels, &notification_relay_state, &session_key).await;

        assert!(!subscriptions.read().await.contains_key(&session_key));
        assert!(!log_levels.write().await.contains_key(&session_key));
        assert!(!notification_relay_state.lock().await.relays.contains_key(&key));
    }

    #[tokio::test]
    async fn cleanup_finished_notification_relay_removes_handle_only() {
        let session_key = SessionKey::new("subject", "virtual-host", "session-a");
        let key = BackendTransportKey::from(("backend-a", &session_key));
        let subscriptions = test_subscriptions(&session_key);
        let log_levels = Arc::new(RwLock::new(HashMap::from([(
            session_key.clone(),
            HashMap::from([("backend".to_owned(), LoggingLevel::Error)]),
        )])));
        let transports = Arc::new(Mutex::new(HashMap::new()));
        let transport_session_keys = test_transport_session_keys(&session_key, [key.clone()]);
        let notification_relay_state = Arc::new(Mutex::new(NotificationRelayState {
            relays: HashMap::from([(
                key.clone(),
                NotificationRelayHandle { generation: 1, handle: tokio::spawn(async {}) },
            )]),
        }));
        let subscription_change_locks = Arc::new(Mutex::new(HashMap::new()));
        let progress_tokens = Arc::new(RwLock::new(HashMap::new()));
        let session_cleanup_registry = test_session_cleanup_registry();
        let user_session_store = LocalUserSessionStore::new();

        cleanup_finished_notification_relay(
            NotificationRelayCleanup {
                subscriptions: &subscriptions,
                log_levels: &log_levels,
                progress_tokens: &progress_tokens,
                transports: &transports,
                transport_session_keys: &transport_session_keys,
                notification_relay_state: &notification_relay_state,
                session_cleanup_registry: &session_cleanup_registry,
                subscription_change_locks: &subscription_change_locks,
                user_session_store: &user_session_store,
            },
            &key,
            2,
            SessionRelayExit::BackendClosed,
        )
        .await;
        assert!(notification_relay_state.lock().await.relays.contains_key(&key));

        cleanup_finished_notification_relay(
            NotificationRelayCleanup {
                subscriptions: &subscriptions,
                log_levels: &log_levels,
                progress_tokens: &progress_tokens,
                transports: &transports,
                transport_session_keys: &transport_session_keys,
                notification_relay_state: &notification_relay_state,
                session_cleanup_registry: &session_cleanup_registry,
                subscription_change_locks: &subscription_change_locks,
                user_session_store: &user_session_store,
            },
            &key,
            1,
            SessionRelayExit::BackendClosed,
        )
        .await;

        assert!(!notification_relay_state.lock().await.relays.contains_key(&key));
        assert!(subscriptions.read().await.contains_key(&session_key));
        assert!(log_levels.write().await.contains_key(&session_key));
    }

    #[tokio::test]
    async fn notification_forward_failure_does_not_cleanup_session_state() {
        let session_key = SessionKey::new("subject", "virtual-host", "session-a");
        let key = BackendTransportKey::from(("backend-a", &session_key));
        let subscriptions = test_subscriptions(&session_key);
        let log_levels = Arc::new(RwLock::new(HashMap::from([(
            session_key.clone(),
            HashMap::from([("backend".to_owned(), LoggingLevel::Error)]),
        )])));
        let transport_session_keys = test_transport_session_keys(&session_key, [key.clone()]);
        let transports = Arc::new(Mutex::new(HashMap::from([(
            key.clone(),
            BackendTransportService {
                capabilities: None,
                service: None,
                backend_notifications: broadcast::channel(1).0,
            },
        )])));
        let notification_relay_state = Arc::new(Mutex::new(NotificationRelayState {
            relays: HashMap::from([(
                key.clone(),
                NotificationRelayHandle { generation: 1, handle: tokio::spawn(async {}) },
            )]),
        }));
        let subscription_change_locks = Arc::new(Mutex::new(HashMap::from([(key.clone(), Arc::new(Mutex::new(())))])));
        let progress_tokens = Arc::new(RwLock::new(HashMap::new()));
        let session_cleanup_registry = test_session_cleanup_registry();
        let user_session_store = LocalUserSessionStore::new();

        cleanup_finished_notification_relay(
            NotificationRelayCleanup {
                subscriptions: &subscriptions,
                log_levels: &log_levels,
                progress_tokens: &progress_tokens,
                transports: &transports,
                transport_session_keys: &transport_session_keys,
                notification_relay_state: &notification_relay_state,
                session_cleanup_registry: &session_cleanup_registry,
                subscription_change_locks: &subscription_change_locks,
                user_session_store: &user_session_store,
            },
            &key,
            1,
            SessionRelayExit::NotificationForwardFailed,
        )
        .await;

        assert!(!notification_relay_state.lock().await.relays.contains_key(&key));
        assert!(subscriptions.read().await.contains_key(&session_key));
        assert!(log_levels.write().await.contains_key(&session_key));
        assert!(transports.lock().await.contains_key(&key));
        assert!(subscription_change_locks.lock().await.contains_key(&key));
    }

    #[tokio::test]
    async fn cleanup_pending_initialized_session_removes_transport_and_owner_state() {
        let session_key = SessionKey::new("subject", "virtual-host", "session-a");
        let key = BackendTransportKey::from(("backend-a", &session_key));
        let transports = Arc::new(Mutex::new(HashMap::from([(
            key.clone(),
            BackendTransportService {
                capabilities: None,
                service: None,
                backend_notifications: broadcast::channel(1).0,
            },
        )])));
        let transport_session_keys = test_transport_session_keys(&session_key, [key.clone()]);
        let session_cleanup_registry = Arc::new(Mutex::new(HashMap::new()));
        let subscription_change_locks = Arc::new(Mutex::new(HashMap::from([(key.clone(), Arc::new(Mutex::new(())))])));
        let user_session_store = LocalUserSessionStore::new();
        user_session_store
            .set_session(&session_key.to_user_session(), &SessionMapping::new())
            .await
            .expect("session owner should store");

        cleanup_pending_initialized_session(PendingInitializedCleanup {
            transports: &transports,
            transport_session_keys: &transport_session_keys,
            session_cleanup_registry: &session_cleanup_registry,
            subscription_change_locks: &subscription_change_locks,
            user_session_store: &user_session_store,
            session_key: &session_key,
        })
        .await;

        assert!(!transports.lock().await.contains_key(&key));
        assert!(!transport_session_keys.lock().await.contains_key(&session_key));
        assert!(!subscription_change_locks.lock().await.contains_key(&key));
        assert!(
            !user_session_store.has_session(&session_key.to_user_session()).await.expect("owner check should work")
        );
    }

    #[tokio::test]
    async fn cleanup_registered_session_removes_full_session_state() {
        let user_session_store = LocalUserSessionStore::new();
        let service: McpService<LocalUserSessionStore> = McpService::builder()
            .with_user_session_store(user_session_store.clone())
            .with_http_client(reqwest::Client::new())
            .build();
        let session_key = SessionKey::new("subject", "virtual-host", "session-a");
        let user_session = session_key.to_user_session();
        let key = BackendTransportKey::from(("backend-a", &session_key));
        let token = ProgressToken(rmcp::model::NumberOrString::String("downstream".into()));

        user_session_store
            .set_session(&user_session, &SessionMapping::new())
            .await
            .expect("session owner should store");
        service.subscriptions.write().await.insert(
            session_key.clone(),
            HashMap::from([("backend-a".to_owned(), HashSet::from(["backend-a-test://resource".to_owned()]))]),
        );
        service
            .log_levels
            .write()
            .await
            .insert(session_key.clone(), HashMap::from([("backend-a".to_owned(), LoggingLevel::Info)]));
        service
            .progress_tokens
            .write()
            .await
            .insert(session_key.clone(), HashMap::from([("backend-a".to_owned(), HashSet::from([token]))]));
        service.transports.lock().await.insert(
            key.clone(),
            BackendTransportService {
                capabilities: None,
                service: None,
                backend_notifications: broadcast::channel(1).0,
            },
        );
        service.transport_session_keys.lock().await.insert(session_key.clone(), HashSet::from([key.clone()]));
        service
            .notification_relay_state
            .lock()
            .await
            .relays
            .insert(key.clone(), NotificationRelayHandle { generation: 1, handle: tokio::spawn(async {}) });
        let cleanup_handle = tokio::spawn(async {});
        service
            .pending_notification_relays
            .lock()
            .await
            .insert(session_key.clone(), PendingNotificationRelaysForSession { relays: Vec::new(), cleanup_handle });
        service.subscription_change_locks.lock().await.insert(key.clone(), Arc::new(Mutex::new(())));
        service.register_session_cleanup(&session_key).await;

        assert!(lifecycle::cleanup_registered_session(&service.session_cleanup_registry, &user_session).await);

        assert!(!service.session_cleanup_registry.lock().await.contains_key(&user_session));
        assert!(!service.subscriptions.read().await.contains_key(&session_key));
        assert!(!service.log_levels.write().await.contains_key(&session_key));
        assert!(!service.progress_tokens.write().await.contains_key(&session_key));
        assert!(!service.transports.lock().await.contains_key(&key));
        assert!(!service.transport_session_keys.lock().await.contains_key(&session_key));
        assert!(!service.notification_relay_state.lock().await.relays.contains_key(&key));
        assert!(!service.pending_notification_relays.lock().await.contains_key(&session_key));
        assert!(!service.subscription_change_locks.lock().await.contains_key(&key));
        assert!(!user_session_store.has_session(&user_session).await.expect("owner check should work"));
    }

    #[tokio::test]
    async fn cleanup_finished_notification_relay_removes_session_state_when_downstream_closes() {
        let session_key = SessionKey::new("subject", "virtual-host", "session-a");
        let key = BackendTransportKey::from(("backend-a", &session_key));
        let other_key = BackendTransportKey::from(("backend-b", &session_key));
        let failed_init_key = BackendTransportKey::from(("backend-c", &session_key));
        let subscriptions = test_subscriptions(&session_key);
        let log_levels = Arc::new(RwLock::new(HashMap::from([(
            session_key.clone(),
            HashMap::from([("backend".to_owned(), LoggingLevel::Error)]),
        )])));
        let transport_session_keys =
            test_transport_session_keys(&session_key, [key.clone(), other_key.clone(), failed_init_key.clone()]);
        let transports = Arc::new(Mutex::new(HashMap::from([
            (
                key.clone(),
                BackendTransportService {
                    capabilities: None,
                    service: None,
                    backend_notifications: broadcast::channel(1).0,
                },
            ),
            (
                other_key.clone(),
                BackendTransportService {
                    capabilities: None,
                    service: None,
                    backend_notifications: broadcast::channel(1).0,
                },
            ),
            (
                failed_init_key.clone(),
                BackendTransportService {
                    capabilities: None,
                    service: None,
                    backend_notifications: broadcast::channel(1).0,
                },
            ),
        ])));
        let notification_relay_state = Arc::new(Mutex::new(NotificationRelayState {
            relays: HashMap::from([
                (key.clone(), NotificationRelayHandle { generation: 1, handle: tokio::spawn(async {}) }),
                (other_key.clone(), NotificationRelayHandle { generation: 1, handle: tokio::spawn(async {}) }),
            ]),
        }));
        let subscription_change_locks = Arc::new(Mutex::new(HashMap::from([(key.clone(), Arc::new(Mutex::new(())))])));
        let progress_tokens = Arc::new(RwLock::new(HashMap::from([(
            session_key.clone(),
            HashMap::from([("backend-a".to_owned(), HashSet::new())]),
        )])));
        let session_cleanup_registry = test_session_cleanup_registry();
        let user_session_store = LocalUserSessionStore::new();
        user_session_store
            .set_session(&session_key.to_user_session(), &SessionMapping::new())
            .await
            .expect("session owner should store");

        cleanup_finished_notification_relay(
            NotificationRelayCleanup {
                subscriptions: &subscriptions,
                log_levels: &log_levels,
                progress_tokens: &progress_tokens,
                transports: &transports,
                transport_session_keys: &transport_session_keys,
                notification_relay_state: &notification_relay_state,
                session_cleanup_registry: &session_cleanup_registry,
                subscription_change_locks: &subscription_change_locks,
                user_session_store: &user_session_store,
            },
            &key,
            1,
            SessionRelayExit::DownstreamClosed,
        )
        .await;

        {
            let notification_relay_state = notification_relay_state.lock().await;
            assert!(!notification_relay_state.relays.contains_key(&key));
            assert!(!notification_relay_state.relays.contains_key(&other_key));
        }
        assert!(!subscriptions.read().await.contains_key(&session_key));
        assert!(!log_levels.write().await.contains_key(&session_key));
        assert!(!progress_tokens.write().await.contains_key(&session_key));
        assert!(!transports.lock().await.contains_key(&key));
        assert!(!transports.lock().await.contains_key(&other_key));
        assert!(!transports.lock().await.contains_key(&failed_init_key));
        assert!(!subscription_change_locks.lock().await.contains_key(&key));
        assert!(
            !user_session_store.has_session(&session_key.to_user_session()).await.expect("owner check should work")
        );
    }

    #[tokio::test]
    async fn stale_downstream_close_does_not_remove_newer_relay_state() {
        let session_key = SessionKey::new("subject", "virtual-host", "session-a");
        let key = BackendTransportKey::from(("backend-a", &session_key));
        let subscriptions = test_subscriptions(&session_key);
        let log_levels = Arc::new(RwLock::new(HashMap::from([(
            session_key.clone(),
            HashMap::from([("backend".to_owned(), LoggingLevel::Error)]),
        )])));
        let transport_session_keys = test_transport_session_keys(&session_key, [key.clone()]);
        let transports = Arc::new(Mutex::new(HashMap::from([(
            key.clone(),
            BackendTransportService {
                capabilities: None,
                service: None,
                backend_notifications: broadcast::channel(1).0,
            },
        )])));
        let notification_relay_state = Arc::new(Mutex::new(NotificationRelayState {
            relays: HashMap::from([(
                key.clone(),
                NotificationRelayHandle { generation: 2, handle: tokio::spawn(async {}) },
            )]),
        }));
        let subscription_change_locks = Arc::new(Mutex::new(HashMap::from([(key.clone(), Arc::new(Mutex::new(())))])));
        let progress_tokens = Arc::new(RwLock::new(HashMap::from([(
            session_key.clone(),
            HashMap::from([("backend-a".to_owned(), HashSet::new())]),
        )])));
        let session_cleanup_registry = test_session_cleanup_registry();
        let user_session_store = LocalUserSessionStore::new();
        user_session_store
            .set_session(&session_key.to_user_session(), &SessionMapping::new())
            .await
            .expect("session owner should store");

        cleanup_finished_notification_relay(
            NotificationRelayCleanup {
                subscriptions: &subscriptions,
                log_levels: &log_levels,
                progress_tokens: &progress_tokens,
                transports: &transports,
                transport_session_keys: &transport_session_keys,
                notification_relay_state: &notification_relay_state,
                session_cleanup_registry: &session_cleanup_registry,
                subscription_change_locks: &subscription_change_locks,
                user_session_store: &user_session_store,
            },
            &key,
            1,
            SessionRelayExit::DownstreamClosed,
        )
        .await;

        assert!(notification_relay_state.lock().await.relays.contains_key(&key));
        assert!(subscriptions.read().await.contains_key(&session_key));
        assert!(log_levels.write().await.contains_key(&session_key));
        assert!(progress_tokens.write().await.contains_key(&session_key));
        assert!(transports.lock().await.contains_key(&key));
        assert!(subscription_change_locks.lock().await.contains_key(&key));
        assert!(user_session_store.has_session(&session_key.to_user_session()).await.expect("owner check should work"));
    }

    #[tokio::test]
    async fn subscription_change_lock_blocks_only_while_guard_is_held() {
        let service: McpService<LocalUserSessionStore> = McpService::builder()
            .with_user_session_store(LocalUserSessionStore::new())
            .with_http_client(reqwest::Client::new())
            .build();
        let session_key = SessionKey::new("subject", "virtual-host", "session-a");
        let key = BackendTransportKey::from(("backend-a", &session_key));
        let lock = service.subscription_change_lock(&key).await;

        let guard = lock.lock().await;
        assert!(service.subscription_change_in_progress(&key).await);

        drop(guard);
        assert!(!service.subscription_change_in_progress(&key).await);

        service.cleanup_notification_state(&session_key).await;
        assert!(!service.subscription_change_locks.lock().await.contains_key(&key));
    }

    #[tokio::test]
    async fn progress_token_is_removed_after_response() {
        let service: McpService<LocalUserSessionStore> = McpService::builder()
            .with_user_session_store(LocalUserSessionStore::new())
            .with_http_client(reqwest::Client::new())
            .build();
        let session_key = SessionKey::new("subject", "virtual-host", "session-a");
        let token = ProgressToken(rmcp::model::NumberOrString::String("downstream".into()));

        service.track_progress_token(&session_key, "backend-a", token.clone()).await;
        assert!(service.progress_tokens.write().await.contains_key(&session_key));
        remove_progress_token(&service.progress_tokens, &session_key, "backend-a", &token).await;
        assert!(!service.progress_tokens.write().await.contains_key(&session_key));
    }

    #[tokio::test]
    async fn pending_initialized_cleanup_task_removes_stale_session_state() {
        let user_session_store = LocalUserSessionStore::new();
        let service: McpService<LocalUserSessionStore> = McpService::builder()
            .with_user_session_store(user_session_store.clone())
            .with_http_client(reqwest::Client::new())
            .build();
        let session_key = SessionKey::new("subject", "virtual-host", "session-a");
        let key = BackendTransportKey::from(("backend-a", &session_key));
        service.transports.lock().await.insert(
            key.clone(),
            BackendTransportService {
                capabilities: None,
                service: None,
                backend_notifications: broadcast::channel(1).0,
            },
        );
        service.transport_session_keys.lock().await.insert(session_key.clone(), HashSet::from([key.clone()]));
        service.subscription_change_locks.lock().await.insert(key.clone(), Arc::new(Mutex::new(())));
        user_session_store
            .set_session(&session_key.to_user_session(), &SessionMapping::new())
            .await
            .expect("session owner should store");

        let cleanup_handle = service.spawn_pending_initialized_cleanup(session_key.clone());
        service
            .pending_notification_relays
            .lock()
            .await
            .insert(session_key.clone(), PendingNotificationRelaysForSession { relays: Vec::new(), cleanup_handle });

        wait_until(|| async { !service.pending_notification_relays.lock().await.contains_key(&session_key) }).await;

        assert!(!service.pending_notification_relays.lock().await.contains_key(&session_key));
        assert!(!service.transports.lock().await.contains_key(&key));
        assert!(!service.transport_session_keys.lock().await.contains_key(&session_key));
        assert!(!service.subscription_change_locks.lock().await.contains_key(&key));
        assert!(
            !user_session_store.has_session(&session_key.to_user_session()).await.expect("owner check should work")
        );
    }

    #[tokio::test]
    async fn session_manager_scopes_transports_by_full_session_key() {
        let backend_name = "backend".to_owned();
        let virtual_host = contextforge_gateway_rs_apis::user_store::VirtualHost {
            backends: HashMap::from([(
                backend_name.clone(),
                contextforge_gateway_rs_apis::user_store::BackendMCPGateway {
                    url: "http://127.0.0.1:1".parse().expect("test URL should parse"),
                },
            )]),
        };
        let session_key = SessionKey::new("subject", "virtual-host", "session");
        let other_subject = SessionKey::new("other-subject", "virtual-host", "session");
        let other_virtual_host = SessionKey::new("subject", "other-virtual-host", "session");
        let (backend_notifications, _) = broadcast::channel(1);
        let transports = Arc::new(Mutex::new(HashMap::from([(
            BackendTransportKey::from((backend_name.as_str(), &session_key)),
            BackendTransportService { capabilities: None, service: None, backend_notifications },
        )])));

        let session_manager = SessionManager::new(&virtual_host, &session_key, &transports);
        assert_eq!(session_manager.borrow_transports().await.len(), 1);
        assert!(session_manager.borrow_transport("backend").await.is_some());

        let session_manager = SessionManager::new(&virtual_host, &other_subject, &transports);
        assert!(session_manager.borrow_transports().await.is_empty());
        assert!(session_manager.borrow_transport("backend").await.is_none());

        let session_manager = SessionManager::new(&virtual_host, &other_virtual_host, &transports);
        assert!(session_manager.borrow_transports().await.is_empty());
        assert!(session_manager.borrow_transport("backend").await.is_none());
    }

    async fn wait_until<F, Fut>(mut condition: F)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        loop {
            if condition().await {
                return;
            }
            assert!(tokio::time::Instant::now() < deadline, "condition did not become true before timeout");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}
