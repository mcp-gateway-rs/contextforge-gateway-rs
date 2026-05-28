//! RMCP notification forwarding from backend sessions.
//!
//! Backend notifications are captured through `ClientHandler` callbacks because
//! the gateway is an RMCP client of each backend. Session relays forward
//! progress, logging, resource, and list-change notifications
//! only when they can be scoped safely to the downstream session. This keeps
//! streamable HTTP/SSE details inside RMCP instead of proxying raw SSE frames.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use rmcp::{
    ClientHandler, RoleClient, RoleServer,
    model::{
        CallToolResult, InitializeRequestParams, LoggingLevel, LoggingMessageNotificationParam,
        ProgressNotificationParam, ProgressToken, ResourceUpdatedNotificationParam, ServerResult,
    },
    service::{NotificationContext, Peer, RequestHandle, RunningService, ServiceError},
};
use tokio::sync::{RwLock, broadcast};
use tracing::debug;

use super::mcp_call_validator::SessionKey;

const MAX_BUFFERED_NOTIFICATIONS_AFTER_RESPONSE: usize = 16;
#[cfg(not(test))]
const SESSION_RELAY_PEER_CLOSED_CHECK_INTERVAL: Duration = Duration::from_secs(1);
#[cfg(test)]
const SESSION_RELAY_PEER_CLOSED_CHECK_INTERVAL: Duration = Duration::from_millis(10);

/// Typed backend notifications relayed at the RMCP boundary, not raw SSE frames.
#[derive(Clone, Debug)]
pub enum BackendNotification {
    Logging(LoggingMessageNotificationParam),
    Progress(ProgressNotificationParam),
    ResourceUpdated(ResourceUpdatedNotificationParam),
    ResourceListChanged,
    ToolListChanged,
}

/// Captures backend server-to-client notifications for request and session relays.
#[derive(Clone, Debug)]
pub struct BackendClientHandler {
    pub(crate) client_info: InitializeRequestParams,
    pub(crate) backend_notifications: broadcast::Sender<BackendNotification>,
}

impl ClientHandler for BackendClientHandler {
    fn get_info(&self) -> InitializeRequestParams {
        self.client_info.clone()
    }

    async fn on_logging_message(
        &self,
        params: LoggingMessageNotificationParam,
        _context: NotificationContext<RoleClient>,
    ) {
        self.send_notification(BackendNotification::Logging(params));
    }

    async fn on_progress(&self, params: ProgressNotificationParam, _context: NotificationContext<RoleClient>) {
        self.send_notification(BackendNotification::Progress(params));
    }

    async fn on_resource_updated(
        &self,
        params: ResourceUpdatedNotificationParam,
        _context: NotificationContext<RoleClient>,
    ) {
        self.send_notification(BackendNotification::ResourceUpdated(params));
    }

    async fn on_resource_list_changed(&self, _context: NotificationContext<RoleClient>) {
        self.send_notification(BackendNotification::ResourceListChanged);
    }

    async fn on_tool_list_changed(&self, _context: NotificationContext<RoleClient>) {
        self.send_notification(BackendNotification::ToolListChanged);
    }
}

impl BackendClientHandler {
    fn send_notification(&self, notification: BackendNotification) {
        let _ = self.backend_notifications.send(notification);
    }
}

pub type BackendClientService = Arc<RunningService<RoleClient, BackendClientHandler>>;
pub(crate) type ResourceSubscriptions = Arc<RwLock<HashMap<SessionKey, HashMap<String, HashSet<String>>>>>;
pub(crate) type LogLevels = Arc<RwLock<HashMap<SessionKey, HashMap<String, LoggingLevel>>>>;
pub(crate) type ProgressTokens = Arc<RwLock<HashMap<SessionKey, HashMap<String, HashSet<ProgressToken>>>>>;

struct RelayScope<'a> {
    backend_name: &'a str,
    resource_subscriptions: &'a ResourceSubscriptions,
    log_levels: &'a LogLevels,
    progress_tokens: &'a ProgressTokens,
    session_key: &'a SessionKey,
}

pub(crate) struct SessionNotificationRelay {
    pub notification_rx: broadcast::Receiver<BackendNotification>,
    pub backend_name: String,
    pub downstream_peer: Peer<RoleServer>,
    pub resource_subscriptions: ResourceSubscriptions,
    pub log_levels: LogLevels,
    pub progress_tokens: ProgressTokens,
    pub session_key: SessionKey,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SessionRelayExit {
    BackendClosed,
    DownstreamClosed,
    NotificationForwardFailed,
}

/// Awaits a backend tool response. Progress notifications are relayed by the session relay.
pub(crate) async fn await_call_tool_response_with_notifications(
    request_handle: RequestHandle<RoleClient>,
) -> Result<CallToolResult, ServiceError> {
    call_tool_result(request_handle.await_response().await)
}

pub(crate) async fn relay_backend_notifications(mut relay: SessionNotificationRelay) -> SessionRelayExit {
    let scope = RelayScope {
        backend_name: &relay.backend_name,
        resource_subscriptions: &relay.resource_subscriptions,
        log_levels: &relay.log_levels,
        progress_tokens: &relay.progress_tokens,
        session_key: &relay.session_key,
    };

    loop {
        if relay.downstream_peer.is_transport_closed() {
            return SessionRelayExit::DownstreamClosed;
        }
        tokio::select! {
            event = relay.notification_rx.recv() => match event {
                Ok(notification) => {
                    if !forward_session_notification(&relay.downstream_peer, notification, &scope).await {
                        return if relay.downstream_peer.is_transport_closed() {
                            SessionRelayExit::DownstreamClosed
                        } else {
                            SessionRelayExit::NotificationForwardFailed
                        };
                    }
                },
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    debug!(skipped, "backend session notification receiver lagged");
                },
                Err(broadcast::error::RecvError::Closed) => return SessionRelayExit::BackendClosed,
            },
            () = tokio::time::sleep(SESSION_RELAY_PEER_CLOSED_CHECK_INTERVAL) => {},
        }
    }
}

pub(crate) async fn drain_buffered_session_notifications(
    mut relay: SessionNotificationRelay,
) -> Result<broadcast::Receiver<BackendNotification>, SessionRelayExit> {
    let scope = RelayScope {
        backend_name: &relay.backend_name,
        resource_subscriptions: &relay.resource_subscriptions,
        log_levels: &relay.log_levels,
        progress_tokens: &relay.progress_tokens,
        session_key: &relay.session_key,
    };

    let mut drained = 0;
    while let Some(notification) = next_buffered_notification(&mut relay.notification_rx, &mut drained) {
        if !forward_session_notification(&relay.downstream_peer, notification, &scope).await {
            return if relay.downstream_peer.is_transport_closed() {
                Err(SessionRelayExit::DownstreamClosed)
            } else {
                Err(SessionRelayExit::NotificationForwardFailed)
            };
        }
    }
    Ok(relay.notification_rx)
}

fn next_buffered_notification(
    notification_rx: &mut broadcast::Receiver<BackendNotification>,
    drained: &mut usize,
) -> Option<BackendNotification> {
    if *drained >= MAX_BUFFERED_NOTIFICATIONS_AFTER_RESPONSE {
        debug!(drained, "stopped draining backend notifications after response");
        return None;
    }

    loop {
        match notification_rx.try_recv() {
            Ok(notification) => {
                *drained += 1;
                return Some(notification);
            },
            Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                debug!(skipped, "backend notification receiver lagged while draining");
            },
            Err(broadcast::error::TryRecvError::Empty | broadcast::error::TryRecvError::Closed) => return None,
        }
    }
}

fn call_tool_result(response: Result<ServerResult, ServiceError>) -> Result<CallToolResult, ServiceError> {
    match response? {
        ServerResult::CallToolResult(result) => Ok(result),
        _ => Err(ServiceError::UnexpectedResponse),
    }
}

async fn forward_session_notification(
    downstream_peer: &Peer<RoleServer>,
    notification: BackendNotification,
    scope: &RelayScope<'_>,
) -> bool {
    let Some(notification) = scope_session_notification(notification, scope).await else {
        return true;
    };

    match notification {
        BackendNotification::Logging(params) => downstream_peer.notify_logging_message(params).await.is_ok(),
        BackendNotification::ResourceUpdated(params) => downstream_peer.notify_resource_updated(params).await.is_ok(),
        BackendNotification::ResourceListChanged => downstream_peer.notify_resource_list_changed().await.is_ok(),
        BackendNotification::ToolListChanged => downstream_peer.notify_tool_list_changed().await.is_ok(),
        BackendNotification::Progress(params) => downstream_peer.notify_progress(params).await.is_ok(),
    }
}

async fn scope_session_notification(
    notification: BackendNotification,
    scope: &RelayScope<'_>,
) -> Option<BackendNotification> {
    match notification {
        BackendNotification::Logging(params) => {
            let log_level = scope
                .log_levels
                .read()
                .await
                .get(scope.session_key)
                .and_then(|backend_levels| backend_levels.get(scope.backend_name))
                .copied()?;
            if !logging_level_enabled(params.level, log_level) {
                return None;
            }
            Some(BackendNotification::Logging(params))
        },
        BackendNotification::ResourceUpdated(mut params) => {
            let subscriptions = scope.resource_subscriptions.read().await;
            let session_subscriptions = subscriptions.get(scope.session_key)?;
            let backend_subscriptions = session_subscriptions.get(scope.backend_name)?;
            let scoped_uri = format!("{}-{}", scope.backend_name, params.uri);
            if !resource_uri_is_subscribed(backend_subscriptions, &scoped_uri) {
                return None;
            }
            params.uri = scoped_uri;
            Some(BackendNotification::ResourceUpdated(params))
        },
        BackendNotification::ResourceListChanged => Some(BackendNotification::ResourceListChanged),
        BackendNotification::ToolListChanged => Some(BackendNotification::ToolListChanged),
        BackendNotification::Progress(params) => {
            let progress_tokens = scope.progress_tokens.read().await;
            let backend_tokens = progress_tokens.get(scope.session_key)?;
            let tokens = backend_tokens.get(scope.backend_name)?;
            if !tokens.contains(&params.progress_token) {
                return None;
            }
            Some(BackendNotification::Progress(params))
        },
    }
}

fn logging_level_enabled(level: LoggingLevel, minimum: LoggingLevel) -> bool {
    logging_level_rank(level) >= logging_level_rank(minimum)
}

fn resource_uri_is_subscribed(backend_subscriptions: &HashSet<String>, scoped_uri: &str) -> bool {
    if backend_subscriptions.contains(scoped_uri) {
        return true;
    }
    backend_subscriptions
        .iter()
        .any(|subscribed| scoped_uri.strip_prefix(subscribed).is_some_and(|suffix| suffix.starts_with('/')))
}

fn logging_level_rank(level: LoggingLevel) -> u8 {
    // RMCP's `LoggingLevel` does not implement `Ord`; keep the MCP severity order local.
    match level {
        LoggingLevel::Debug => 0,
        LoggingLevel::Info => 1,
        LoggingLevel::Notice => 2,
        LoggingLevel::Warning => 3,
        LoggingLevel::Error => 4,
        LoggingLevel::Critical => 5,
        LoggingLevel::Alert => 6,
        LoggingLevel::Emergency => 7,
    }
}

#[cfg(test)]
mod tests {
    use rmcp::model::NumberOrString;

    use super::*;

    fn progress_notification(progress_token: ProgressToken) -> BackendNotification {
        BackendNotification::Progress(ProgressNotificationParam {
            progress_token,
            progress: 1.0,
            total: Some(1.0),
            message: Some("progress".to_owned()),
        })
    }

    fn session_key() -> SessionKey {
        SessionKey::new("subject", "virtual-host", "session")
    }

    const TEST_RESOURCE_URI: &str = "test://resource";
    const SCOPED_TEST_RESOURCE_URI: &str = "backend-test://resource";

    fn resource_subscriptions(session_key: &SessionKey, backend_name: &str, uris: &[&str]) -> ResourceSubscriptions {
        Arc::new(RwLock::new(HashMap::from([(
            session_key.clone(),
            HashMap::from([(backend_name.to_owned(), uris.iter().map(|uri| (*uri).to_owned()).collect())]),
        )])))
    }

    fn resource_update(uri: &str) -> BackendNotification {
        BackendNotification::ResourceUpdated(ResourceUpdatedNotificationParam { uri: uri.into() })
    }

    fn progress_tokens(
        session_key: &SessionKey,
        backend_name: &str,
        downstream_token: ProgressToken,
    ) -> ProgressTokens {
        Arc::new(RwLock::new(HashMap::from([(
            session_key.clone(),
            HashMap::from([(backend_name.to_owned(), HashSet::from([downstream_token]))]),
        )])))
    }

    async fn scope_session_for_test(
        notification: BackendNotification,
        resource_subscriptions: &ResourceSubscriptions,
        progress_tokens: &ProgressTokens,
    ) -> Option<BackendNotification> {
        let log_levels = Arc::new(RwLock::new(HashMap::new()));
        let session_key = session_key();
        scope_session_notification(
            notification,
            &RelayScope {
                backend_name: "backend",
                resource_subscriptions,
                log_levels: &log_levels,
                progress_tokens,
                session_key: &session_key,
            },
        )
        .await
    }

    #[tokio::test]
    async fn scopes_progress_to_downstream_token_only() {
        let session_key = session_key();
        let downstream_token = ProgressToken(NumberOrString::String("downstream".into()));
        let progress_tokens = progress_tokens(&session_key, "backend", downstream_token.clone());
        let scoped = scope_session_notification(
            progress_notification(downstream_token.clone()),
            &RelayScope {
                backend_name: "backend",
                resource_subscriptions: &resource_subscriptions(&session_key, "backend", &[]),
                log_levels: &Arc::new(RwLock::new(HashMap::new())),
                progress_tokens: &progress_tokens,
                session_key: &session_key,
            },
        )
        .await;

        assert!(matches!(
            scoped,
            Some(BackendNotification::Progress(params)) if params.progress_token == downstream_token
        ));

        let no_downstream_token = scope_session_notification(
            progress_notification(downstream_token.clone()),
            &RelayScope {
                backend_name: "backend",
                resource_subscriptions: &resource_subscriptions(&session_key, "backend", &[]),
                log_levels: &Arc::new(RwLock::new(HashMap::new())),
                progress_tokens: &Arc::new(RwLock::new(HashMap::new())),
                session_key: &session_key,
            },
        )
        .await;
        assert!(no_downstream_token.is_none());

        let stale = scope_session_notification(
            progress_notification(ProgressToken(NumberOrString::String("wrong".into()))),
            &RelayScope {
                backend_name: "backend",
                resource_subscriptions: &resource_subscriptions(&session_key, "backend", &[]),
                log_levels: &Arc::new(RwLock::new(HashMap::new())),
                progress_tokens: &progress_tokens,
                session_key: &session_key,
            },
        )
        .await;
        assert!(stale.is_none());
    }

    #[tokio::test]
    async fn scopes_resource_updated_to_subscribed_uri() {
        let session_key = session_key();
        let subscribed_resources = resource_subscriptions(&session_key, "backend", &[SCOPED_TEST_RESOURCE_URI]);
        let scoped = scope_session_for_test(
            resource_update(TEST_RESOURCE_URI),
            &subscribed_resources,
            &Arc::new(RwLock::new(HashMap::new())),
        )
        .await;

        assert!(matches!(
            scoped.as_ref(),
            Some(BackendNotification::ResourceUpdated(params)) if params.uri == SCOPED_TEST_RESOURCE_URI
        ));

        let sub_resource = scope_session_for_test(
            resource_update("test://resource/child"),
            &subscribed_resources,
            &Arc::new(RwLock::new(HashMap::new())),
        )
        .await;
        assert!(matches!(
            sub_resource.as_ref(),
            Some(BackendNotification::ResourceUpdated(params)) if params.uri == "backend-test://resource/child"
        ));

        let sibling_resource_with_common_prefix = scope_session_for_test(
            resource_update("test://resource-child"),
            &subscribed_resources,
            &Arc::new(RwLock::new(HashMap::new())),
        )
        .await;
        assert!(sibling_resource_with_common_prefix.is_none());

        let no_subscriptions = resource_subscriptions(&session_key, "backend", &[]);
        let unsubscribed = scope_session_for_test(
            resource_update(TEST_RESOURCE_URI),
            &no_subscriptions,
            &Arc::new(RwLock::new(HashMap::new())),
        )
        .await;
        assert!(unsubscribed.is_none());
    }

    #[tokio::test]
    async fn scopes_resource_updated_by_session_key() {
        let other_session_key = SessionKey::new("other-subject", "virtual-host", "session");
        let subscriptions = resource_subscriptions(&other_session_key, "backend", &[SCOPED_TEST_RESOURCE_URI]);
        let scoped = scope_session_for_test(
            resource_update(TEST_RESOURCE_URI),
            &subscriptions,
            &Arc::new(RwLock::new(HashMap::new())),
        )
        .await;

        assert!(scoped.is_none());
    }

    #[tokio::test]
    async fn scopes_resource_updated_by_backend_name() {
        let session_key = session_key();
        let subscriptions = resource_subscriptions(&session_key, "other-backend", &[SCOPED_TEST_RESOURCE_URI]);
        let scoped = scope_session_for_test(
            resource_update(TEST_RESOURCE_URI),
            &subscriptions,
            &Arc::new(RwLock::new(HashMap::new())),
        )
        .await;

        assert!(scoped.is_none());
    }

    #[tokio::test]
    async fn logging_notifications_require_session_level() {
        let session_key = session_key();
        let log_levels = Arc::new(RwLock::new(HashMap::new()));
        let subscriptions = resource_subscriptions(&session_key, "backend", &[]);
        let notification = BackendNotification::Logging(LoggingMessageNotificationParam {
            level: LoggingLevel::Info,
            logger: None,
            data: serde_json::Value::Null,
        });
        let scope = RelayScope {
            backend_name: "backend",
            resource_subscriptions: &subscriptions,
            log_levels: &log_levels,
            progress_tokens: &Arc::new(RwLock::new(HashMap::new())),
            session_key: &session_key,
        };

        assert!(scope_session_notification(notification.clone(), &scope).await.is_none());

        log_levels
            .write()
            .await
            .insert(session_key.clone(), HashMap::from([("backend".to_owned(), LoggingLevel::Info)]));
        assert!(matches!(
            scope_session_notification(notification, &scope).await,
            Some(BackendNotification::Logging(_))
        ));
    }

    #[test]
    fn drain_collects_only_configured_buffer() {
        let (tx, mut rx) = broadcast::channel(MAX_BUFFERED_NOTIFICATIONS_AFTER_RESPONSE * 2);
        for _ in 0..=MAX_BUFFERED_NOTIFICATIONS_AFTER_RESPONSE {
            tx.send(BackendNotification::Logging(LoggingMessageNotificationParam {
                level: LoggingLevel::Info,
                logger: None,
                data: serde_json::Value::Null,
            }))
            .expect("receiver should accept test notification");
        }

        let mut drained = 0;
        let mut buffered = Vec::new();
        while let Some(notification) = next_buffered_notification(&mut rx, &mut drained) {
            buffered.push(notification);
        }

        assert_eq!(buffered.len(), MAX_BUFFERED_NOTIFICATIONS_AFTER_RESPONSE);
        assert!(matches!(rx.try_recv(), Ok(BackendNotification::Logging(_))));
    }

    #[test]
    fn logging_level_filter_uses_mcp_severity_order() {
        let levels = [
            LoggingLevel::Debug,
            LoggingLevel::Info,
            LoggingLevel::Notice,
            LoggingLevel::Warning,
            LoggingLevel::Error,
            LoggingLevel::Critical,
            LoggingLevel::Alert,
            LoggingLevel::Emergency,
        ];

        for (minimum_rank, minimum) in levels.iter().copied().enumerate() {
            for (level_rank, level) in levels.iter().copied().enumerate() {
                assert_eq!(logging_level_enabled(level, minimum), level_rank >= minimum_rank);
            }
        }
    }
}
