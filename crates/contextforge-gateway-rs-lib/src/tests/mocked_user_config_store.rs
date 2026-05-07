use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use lru_time_cache::LruCache;
use tokio::sync::Mutex;

use crate::user_config_store::{ConfigStoreError, User, UserConfig, UserConfigStore};

#[derive(Clone)]
pub struct MockedUserConfigStore {
    pub user_map: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>,
    pub cache: Arc<Mutex<LruCache<String, UserConfig>>>,
}

impl Default for MockedUserConfigStore {
    fn default() -> Self {
        Self {
            user_map: Arc::new(Mutex::new(HashMap::new())),
            cache: Arc::new(Mutex::new(LruCache::with_capacity(10))),
        }
    }
}

#[async_trait]
impl UserConfigStore for MockedUserConfigStore {
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

            let user_map = self.user_map.lock().await;
            let maybe_user_config = user_map.get(&key);

            let Some(user_config) = maybe_user_config else {
                return Err(ConfigStoreError::NoDataForKey);
            };

            let Ok(user_config) = rmp_serde::decode::from_slice::<UserConfig>(user_config) else {
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

        let mut user_map = self.user_map.lock().await;
        user_map.insert(key.clone(), encoded);
        self.cache.lock().await.insert(user_key.key().to_owned(), config.clone());
        Ok(())
    }
}
