use std::sync::Arc;

use async_trait::async_trait;
use lru_time_cache::LruCache;
use redis::{AsyncCommands, RedisError, cmd};
use tokio::sync::Mutex;

use super::{SessionMapping, SessionStoreError, UserSession, UserSessionStore};
use crate::{
    common::RedisClient,
    const_values::{LRU_CACHE_ENTRIES, LRU_CACHE_EXPIRY_DURATION},
};

#[derive(Clone)]
pub struct RedisUserSessionStore {
    redis_client: RedisClient,
    cache: Arc<Mutex<LruCache<UserSession, SessionMapping>>>,
}
impl RedisUserSessionStore {
    #[expect(dead_code, reason = "Redis-backed user sessions are implemented but not wired by default")]
    pub fn new(redis_client: RedisClient) -> Self {
        Self {
            redis_client,
            cache: Arc::new(Mutex::new(LruCache::with_expiry_duration_and_capacity(
                LRU_CACHE_EXPIRY_DURATION,
                LRU_CACHE_ENTRIES,
            ))),
        }
    }
}

#[async_trait]
impl UserSessionStore for RedisUserSessionStore {
    async fn get_session<'a>(&self, session_key: &'a UserSession) -> Result<Option<SessionMapping>, SessionStoreError> {
        let has_key = { self.cache.lock().await.contains_key(session_key) };
        if has_key {
            if let Some(user_session) = self.cache.lock().await.get_mut(session_key) {
                Ok(Some(user_session.clone()))
            } else {
                Ok(None)
            }
        } else {
            let Ok(key) = rmp_serde::encode::to_vec::<UserSession>(session_key) else {
                return Err(SessionStoreError::DataEncoding);
            };

            let Ok(mut connection) = self.redis_client.get_multiplexed_async_connection().await else {
                return Err(SessionStoreError::InvalidConnection);
            };

            let maybe_user_session: Result<Option<Vec<u8>>, RedisError> =
                cmd("GET").arg(key).take().query_async(&mut connection).await;

            let Ok(Some(user_session)) = maybe_user_session else {
                return Ok(None);
            };

            let Ok(user_session) = rmp_serde::decode::from_slice::<SessionMapping>(&user_session) else {
                return Err(SessionStoreError::DataWrongFormat);
            };

            self.cache.lock().await.insert(session_key.clone(), user_session.clone());
            Ok(Some(user_session))
        }
    }

    async fn set_session<'a>(
        &self,
        session_key: &'a UserSession,
        mapping: &'a SessionMapping,
    ) -> Result<(), SessionStoreError> {
        let Ok(key) = rmp_serde::encode::to_vec::<UserSession>(session_key) else {
            return Err(SessionStoreError::DataEncoding);
        };

        let Ok(encoded) = rmp_serde::encode::to_vec::<SessionMapping>(mapping) else {
            return Err(SessionStoreError::DataEncoding);
        };

        let Ok(mut connection) = self.redis_client.get_multiplexed_async_connection().await else {
            return Err(SessionStoreError::InvalidConnection);
        };

        if connection.set::<&[u8], &[u8], String>(&key, &encoded).await.is_ok() {
            self.cache.lock().await.insert(session_key.clone(), mapping.clone());
            Ok(())
        } else {
            return Err(SessionStoreError::CantWriteData);
        }
    }
}
