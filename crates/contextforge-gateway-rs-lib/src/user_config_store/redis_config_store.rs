use std::sync::Arc;

use async_trait::async_trait;
use contextforge_gateway_rs_apis::{User, user_store::UserConfig};
use lru_time_cache::LruCache;
use redis::{
    AsyncCommands, RedisError,
    aio::{ConnectionManager, ConnectionManagerConfig},
    cmd,
};

use tokio::sync::Mutex;

use super::{ConfigStoreError, UserConfigStore};
use crate::{
    common::RedisClient,
    const_values::{LRU_CACHE_ENTRIES, LRU_CACHE_EXPIRY_DURATION},
};

#[derive(Clone)]
pub struct RedisUserConfigStore {
    connection: ConnectionManager,
    cache: Arc<Mutex<LruCache<String, UserConfig>>>,
}
impl RedisUserConfigStore {
    pub async fn new(redis_client: &RedisClient) -> crate::Result<Self> {
        Ok(Self {
            connection: redis_client
                .get_connection_manager_with_config(ConnectionManagerConfig::default())
                .await
                .map_err(|_| ConfigStoreError::InvalidConnection)?,
            cache: Arc::new(Mutex::new(LruCache::with_expiry_duration_and_capacity(
                LRU_CACHE_EXPIRY_DURATION,
                LRU_CACHE_ENTRIES,
            ))),
        })
    }
}

#[async_trait]
impl UserConfigStore for RedisUserConfigStore {
    async fn get_config<'a>(&self, user_key: &'a User) -> Result<UserConfig, ConfigStoreError> {
        let has_key = { self.cache.lock().await.contains_key(user_key.key()) };
        if has_key {
            if let Some(user_config) = self.cache.lock().await.get_mut(user_key.key()) {
                Ok(user_config.clone())
            } else {
                return Err(ConfigStoreError::NoDataForKey);
            }
        } else {
            let Ok(key) = rmp_serde::encode::to_vec::<User>(user_key) else {
                return Err(ConfigStoreError::DataEncoding);
            };

            let mut connection = self.connection.clone();
            let maybe_user_config: Result<Option<Vec<u8>>, RedisError> =
                cmd("GET").arg(key).take().query_async(&mut connection).await;

            let Ok(Some(user_config)) = maybe_user_config else {
                return Err(ConfigStoreError::NoDataForKey);
            };

            let Ok(user_config) = rmp_serde::decode::from_slice::<UserConfig>(&user_config) else {
                return Err(ConfigStoreError::DataWrongFormat);
            };

            self.cache.lock().await.insert(user_key.key().to_owned(), user_config.clone());
            Ok(user_config)
        }
    }

    async fn set_config<'a>(&self, user_key: &'a User, config: &'a UserConfig) -> Result<(), ConfigStoreError> {
        let Ok(key) = rmp_serde::encode::to_vec::<User>(user_key) else {
            return Err(ConfigStoreError::DataEncoding);
        };

        let Ok(encoded) = rmp_serde::encode::to_vec::<UserConfig>(config) else {
            return Err(ConfigStoreError::DataEncoding);
        };

        let mut connection = self.connection.clone();

        if connection.set::<&[u8], &[u8], String>(&key, &encoded).await.is_ok() {
            self.cache.lock().await.insert(user_key.key().to_owned(), config.clone());
            Ok(())
        } else {
            return Err(ConfigStoreError::CantWriteData);
        }
    }
}
