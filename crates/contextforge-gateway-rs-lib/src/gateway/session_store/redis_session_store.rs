use std::sync::Arc;

use async_trait::async_trait;
use lru_time_cache::LruCache;
use redis::{
    AsyncCommands, RedisError,
    aio::{ConnectionManager, ConnectionManagerConfig},
    cmd,
};
use tokio::sync::Mutex;

use super::{SessionMapping, SessionStoreError, UserSession, UserSessionStore};
use crate::{
    common::RedisClient,
    const_values::{LRU_CACHE_ENTRIES, LRU_CACHE_EXPIRY_DURATION, REDIS_RETRIES},
};

#[derive(Clone)]
pub struct RedisUserSessionStore {
    cache: Arc<Mutex<LruCache<UserSession, SessionMapping>>>,
    connection: ConnectionManager,
}

impl RedisUserSessionStore {
    #[expect(dead_code, reason = "Redis-backed user sessions are implemented but not wired by default")]
    pub async fn new(redis_client: &RedisClient) -> crate::Result<Self> {
        Ok(Self {
            cache: Arc::new(Mutex::new(LruCache::with_expiry_duration_and_capacity(
                LRU_CACHE_EXPIRY_DURATION,
                LRU_CACHE_ENTRIES,
            ))),
            connection: redis_client
                .get_connection_manager_with_config(
                    ConnectionManagerConfig::default().set_number_of_retries(REDIS_RETRIES),
                )
                .await
                .map_err(|_| SessionStoreError::InvalidConnection)?,
        })
    }
}

#[async_trait]
impl UserSessionStore for RedisUserSessionStore {
    async fn has_session<'a>(&self, session_key: &'a UserSession) -> Result<bool, SessionStoreError> {
        let has_key = { self.cache.lock().await.contains_key(session_key) };
        if has_key {
            return Ok(true);
        }

        let Ok(key) = rmp_serde::encode::to_vec::<UserSession>(session_key) else {
            return Err(SessionStoreError::DataEncoding);
        };

        let mut connection = self.connection.clone();
        let exists = cmd("EXISTS")
            .arg(key)
            .query_async::<bool>(&mut connection)
            .await
            .map_err(|_| SessionStoreError::InvalidConnection)?;
        Ok(exists)
    }

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

            let mut connection = self.connection.clone();

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

        let mut connection = self.connection.clone();

        if connection.set::<&[u8], &[u8], String>(&key, &encoded).await.is_ok() {
            self.cache.lock().await.insert(session_key.clone(), mapping.clone());
            Ok(())
        } else {
            return Err(SessionStoreError::CantWriteData);
        }
    }

    async fn remove_session<'a>(&self, session_key: &'a UserSession) -> Result<(), SessionStoreError> {
        let Ok(key) = rmp_serde::encode::to_vec::<UserSession>(session_key) else {
            return Err(SessionStoreError::DataEncoding);
        };

        let mut connection = self.connection.clone();
        if cmd("DEL").arg(key).query_async::<()>(&mut connection).await.is_ok() {
            self.cache.lock().await.remove(session_key);
            Ok(())
        } else {
            Err(SessionStoreError::CantWriteData)
        }
    }
}
