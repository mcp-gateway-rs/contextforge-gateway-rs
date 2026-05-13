use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, Hash, PartialEq, PartialOrd, Ord, Eq, JsonSchema)]
enum KeyType {
    UserConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize, Hash, PartialEq, PartialOrd, Ord, Eq, JsonSchema)]
pub struct User<'a> {
    name: KeyType,
    key: &'a str,
}

impl User<'_> {
    pub fn key(&self) -> &str {
        self.key
    }
}

impl<'a> User<'a> {
    pub fn new(key: &'a str) -> Self {
        Self { name: KeyType::UserConfig, key }
    }
}
