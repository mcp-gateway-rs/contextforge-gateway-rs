use std::{error::Error as StdError, fmt};

use thiserror::Error;

#[derive(Error)]
pub enum GatewayPluginRuntimeError {
    #[error("failed to register runtime {hook} hook")]
    Configuration {
        hook: &'static str,
        #[source]
        source: Box<dyn StdError + Send + Sync + 'static>,
    },
    #[error("failed to initialize plugin runtime")]
    Initialization {
        #[source]
        source: Box<dyn StdError + Send + Sync + 'static>,
    },
    #[error("runtime plugin config store disconnected")]
    ConfigStoreUnavailable,
    #[error("runtime plugin config is in wrong format")]
    ConfigWrongFormat,
    #[error("runtime plugin config is unsupported")]
    ConfigUnsupported,
    #[error("runtime plugin factories are already shared")]
    FactoryRegistryShared,
}

impl fmt::Debug for GatewayPluginRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration { hook, .. } => formatter.debug_struct("Configuration").field("hook", hook).finish(),
            Self::Initialization { .. } => formatter.write_str("Initialization"),
            Self::ConfigStoreUnavailable => formatter.write_str("ConfigStoreUnavailable"),
            Self::ConfigWrongFormat => formatter.write_str("ConfigWrongFormat"),
            Self::ConfigUnsupported => formatter.write_str("ConfigUnsupported"),
            Self::FactoryRegistryShared => formatter.write_str("FactoryRegistryShared"),
        }
    }
}
