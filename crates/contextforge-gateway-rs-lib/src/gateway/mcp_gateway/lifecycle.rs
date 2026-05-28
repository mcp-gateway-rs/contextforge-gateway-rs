use super::{
    BackendNotification, BackendTransportKey, LogLevels, MAX_BUFFERED_NOTIFICATIONS_AFTER_RELAY_STOP,
    NotificationRelayCleanup, NotificationRelayState, PendingInitializedCleanup, ProgressTokens, ResourceSubscriptions,
    SessionCleanupHandle, SessionCleanupRegistry, SessionKey, SessionRelayExit, SessionStoreError, UserSession,
    UserSessionStore,
};
use rmcp::model::ProgressToken;
use std::sync::Arc;
use tokio::sync::{Mutex, broadcast};

pub(crate) async fn cleanup_registered_session(registry: &SessionCleanupRegistry, user_session: &UserSession) -> bool {
    let Some(handle) = registry.lock().await.remove(user_session) else {
        return false;
    };
    cleanup_session_handle(handle).await;
    true
}

pub(crate) async fn cleanup_registered_session_or_owner(
    registry: &SessionCleanupRegistry,
    user_session_store: &dyn UserSessionStore,
    user_session: &UserSession,
) -> Result<(), SessionStoreError> {
    if cleanup_registered_session(registry, user_session).await {
        return Ok(());
    }
    user_session_store.remove_session(user_session).await
}

async fn cleanup_session_handle(handle: SessionCleanupHandle) {
    cleanup_notification_state(
        &handle.subscriptions,
        &handle.log_levels,
        &handle.notification_relay_state,
        &handle.session_key,
    )
    .await;
    handle.progress_tokens.write().await.remove(&handle.session_key);
    if let Some(pending) = handle.pending_notification_relays.lock().await.remove(&handle.session_key) {
        pending.cleanup_handle.abort();
    }
    let keys = handle.transport_session_keys.lock().await.remove(&handle.session_key).unwrap_or_default();
    {
        let mut transports = handle.transports.lock().await;
        for key in &keys {
            transports.remove(key);
        }
    }
    handle.subscription_change_locks.lock().await.retain(|key, _| key.session_key != handle.session_key);
    let _ = handle.user_session_store.remove_session(&handle.session_key.to_user_session()).await;
}

pub(super) async fn cleanup_notification_state(
    subscriptions: &ResourceSubscriptions,
    log_levels: &LogLevels,
    notification_relay_state: &Arc<Mutex<NotificationRelayState>>,
    session_key: &SessionKey,
) {
    subscriptions.write().await.remove(session_key);
    log_levels.write().await.remove(session_key);
    let mut state = notification_relay_state.lock().await;
    state.relays.retain(|key, relay| {
        if &key.session_key == session_key {
            relay.handle.abort();
            false
        } else {
            true
        }
    });
}

pub(super) async fn cleanup_finished_notification_relay(
    cleanup: NotificationRelayCleanup<'_>,
    key: &BackendTransportKey,
    generation: u64,
    exit: SessionRelayExit,
) {
    let relay_keys_to_cleanup = {
        let mut state = cleanup.notification_relay_state.lock().await;
        if state.relays.get(key).is_none_or(|relay| relay.generation != generation) {
            return;
        }

        match exit {
            SessionRelayExit::BackendClosed | SessionRelayExit::NotificationForwardFailed => {
                state.relays.remove(key);
                Vec::new()
            },
            SessionRelayExit::DownstreamClosed => {
                let mut relay_keys_to_cleanup = Vec::new();
                state.relays.retain(|relay_key, relay| {
                    if relay_key.session_key == key.session_key {
                        relay_keys_to_cleanup.push(relay_key.clone());
                        if relay_key != key {
                            relay.handle.abort();
                        }
                        false
                    } else {
                        true
                    }
                });
                relay_keys_to_cleanup
            },
        }
    };
    if exit != SessionRelayExit::DownstreamClosed {
        return;
    }
    cleanup.session_cleanup_registry.lock().await.remove(&key.session_key.to_user_session());
    let transport_keys_to_cleanup = cleanup
        .transport_session_keys
        .lock()
        .await
        .remove(&key.session_key)
        .unwrap_or_else(|| relay_keys_to_cleanup.into_iter().collect());

    if !transport_keys_to_cleanup.is_empty() {
        cleanup.subscriptions.write().await.remove(&key.session_key);
        cleanup.log_levels.write().await.remove(&key.session_key);
        cleanup.progress_tokens.write().await.remove(&key.session_key);
        let _ = cleanup.user_session_store.remove_session(&key.session_key.to_user_session()).await;
        {
            let mut transports = cleanup.transports.lock().await;
            for transport_key in &transport_keys_to_cleanup {
                transports.remove(transport_key);
            }
        }
        let mut subscription_change_locks = cleanup.subscription_change_locks.lock().await;
        for transport_key in transport_keys_to_cleanup {
            subscription_change_locks.remove(&transport_key);
        }
    }
}

pub(super) async fn cleanup_pending_initialized_session(cleanup: PendingInitializedCleanup<'_>) {
    cleanup.session_cleanup_registry.lock().await.remove(&cleanup.session_key.to_user_session());
    let transport_keys = cleanup.transport_session_keys.lock().await.remove(cleanup.session_key).unwrap_or_default();
    {
        let mut transports = cleanup.transports.lock().await;
        for transport_key in &transport_keys {
            transports.remove(transport_key);
        }
    }
    {
        let mut subscription_change_locks = cleanup.subscription_change_locks.lock().await;
        for transport_key in transport_keys {
            subscription_change_locks.remove(&transport_key);
        }
    }
    let _ = cleanup.user_session_store.remove_session(&cleanup.session_key.to_user_session()).await;
}

pub(super) async fn remove_progress_token(
    progress_tokens: &ProgressTokens,
    session_key: &SessionKey,
    backend_name: &str,
    downstream_token: &ProgressToken,
) {
    let mut progress_tokens = progress_tokens.write().await;
    let Some(backend_tokens) = progress_tokens.get_mut(session_key) else {
        return;
    };
    let Some(tokens) = backend_tokens.get_mut(backend_name) else {
        return;
    };
    tokens.remove(downstream_token);
    if tokens.is_empty() {
        backend_tokens.remove(backend_name);
    }
    if backend_tokens.is_empty() {
        progress_tokens.remove(session_key);
    }
}

pub(super) fn discard_buffered_notifications(notification_rx: &mut broadcast::Receiver<BackendNotification>) {
    let mut discarded = 0;
    while discarded < MAX_BUFFERED_NOTIFICATIONS_AFTER_RELAY_STOP {
        match notification_rx.try_recv() {
            Ok(_) | Err(broadcast::error::TryRecvError::Lagged(_)) => discarded += 1,
            Err(broadcast::error::TryRecvError::Empty | broadcast::error::TryRecvError::Closed) => break,
        }
    }
}
