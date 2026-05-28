use contextforge_gateway_rs_apis::user_store::{UserConfig, VirtualHost};
use std::sync::Arc;

use http::request::Parts;
use rmcp::{
    ErrorData, RoleServer,
    model::{ErrorCode, Extensions},
    service::RequestContext,
    transport::streamable_http_server::tower::DownstreamSessionId,
};
use tracing::info;

use crate::{
    common::ContextForgeClaims,
    gateway::UserSession,
    layers::{session_id::SessionId, virtual_host_id::VirtualHostId},
};

/// Gateway-local state key scoped wider than the downstream MCP session id.
///
/// The raw `Mcp-session-id` is client controlled and only identifies a
/// downstream MCP session. Backend transports and notification state also need
/// to be scoped by authenticated subject and virtual host so two callers cannot
/// collide when they reuse the same downstream session id.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SessionKey {
    subject: String,
    virtual_host_id: String,
    session_id: String,
}

impl SessionKey {
    pub(crate) fn new(subject: &str, virtual_host_id: &str, session_id: &str) -> Self {
        Self {
            subject: subject.to_owned(),
            virtual_host_id: virtual_host_id.to_owned(),
            session_id: session_id.to_owned(),
        }
    }

    pub(crate) fn from_authorized_extensions(extensions: &Extensions) -> Option<Self> {
        let parts = extensions.get::<Parts>()?;
        let session_id = parts.extensions.get::<SessionId>()?;
        let user_config = parts.extensions.get::<UserConfig>()?;
        let virtual_host_id = parts.extensions.get::<VirtualHostId>()?;
        let claims = parts.extensions.get::<ContextForgeClaims>()?;
        user_config.virtual_hosts.get(virtual_host_id.value())?;
        Some(Self::new(&claims.sub, virtual_host_id.value(), session_id.value()))
    }

    pub(crate) fn to_user_session(&self) -> UserSession {
        UserSession::new(self.subject.clone(), self.virtual_host_id.clone(), Arc::from(self.session_id.as_str()))
    }
}

impl std::fmt::Display for SessionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[redacted-key]")
    }
}

impl std::fmt::Debug for SessionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SessionKey([redacted])")
    }
}

pub struct AuthorizedCallContext<'a> {
    pub virtual_host: &'a VirtualHost,
    pub session_key: SessionKey,
}

pub struct InitializeCallContext<'a> {
    pub virtual_host: &'a VirtualHost,
    pub virtual_host_id: &'a str,
    pub downstream_session_id: &'a DownstreamSessionId,
    pub principal: &'a str,
    pub session_key: SessionKey,
}

pub struct AuthorizedCallValidator<'a> {
    call_name: &'a str,
    ctx: &'a RequestContext<RoleServer>,
}

impl<'a> AuthorizedCallValidator<'a> {
    pub fn new(call_name: &'a str, ctx: &'a RequestContext<RoleServer>) -> Self {
        Self { call_name, ctx }
    }
    pub fn validate(self) -> Result<AuthorizedCallContext<'a>, ErrorData> {
        let maybe_parts = self.ctx.extensions.get::<Parts>();
        let maybe_session_id = maybe_parts.and_then(|parts| parts.extensions.get::<SessionId>());
        let maybe_user_config = maybe_parts.and_then(|parts| parts.extensions.get::<UserConfig>());
        let maybe_claims = maybe_parts.and_then(|parts| parts.extensions.get::<ContextForgeClaims>());

        let maybe_virtual_host_id = maybe_parts.and_then(|parts| parts.extensions.get::<VirtualHostId>());
        info!(
            "{} context user_config={} session_id={} virtual_host_id={}",
            self.call_name,
            maybe_user_config.is_some(),
            maybe_session_id.is_some(),
            maybe_virtual_host_id.is_some()
        );

        let Some(session_id) = maybe_session_id else {
            return Err(ErrorData {
                code: ErrorCode::INTERNAL_ERROR,
                message: "Routing problem... session id not created".into(),
                data: None,
            });
        };

        let Some(user_config) = maybe_user_config else {
            return Err(ErrorData {
                code: ErrorCode::INTERNAL_ERROR,
                message: "Routing problem... user config not found".into(),
                data: None,
            });
        };

        let Some(virtual_host_id) = maybe_virtual_host_id else {
            return Err(ErrorData {
                code: ErrorCode::INTERNAL_ERROR,
                message: "Routing problem... virutal host not known".into(),
                data: None,
            });
        };

        let Some(virtual_host) = user_config.virtual_hosts.get(virtual_host_id.value()) else {
            return Err(ErrorData {
                code: ErrorCode::RESOURCE_NOT_FOUND,
                message: "No configuration".into(),
                data: None,
            });
        };

        let Some(claims) = maybe_claims else {
            return Err(ErrorData {
                code: ErrorCode::INTERNAL_ERROR,
                message: "Routing problem... claims not found".into(),
                data: None,
            });
        };

        Ok(AuthorizedCallContext {
            virtual_host,
            session_key: SessionKey::new(&claims.sub, virtual_host_id.value(), session_id.value()),
        })
    }
}

pub struct InitializeCallValidator<'a> {
    ctx: &'a RequestContext<RoleServer>,
}

impl<'a> InitializeCallValidator<'a> {
    pub fn new(ctx: &'a RequestContext<RoleServer>) -> Self {
        Self { ctx }
    }
    pub fn validate(self) -> Result<InitializeCallContext<'a>, ErrorData> {
        let maybe_parts = self.ctx.extensions.get::<Parts>();

        let maybe_downstream_session = self.ctx.extensions.get::<DownstreamSessionId>();
        let maybe_user_config = maybe_parts.and_then(|parts| parts.extensions.get::<UserConfig>());
        let maybe_virtual_host_id = maybe_parts.and_then(|parts| parts.extensions.get::<VirtualHostId>());
        let maybe_claims = maybe_parts.and_then(|parts| parts.extensions.get::<ContextForgeClaims>());
        info!(
            "initialize context user_config={} downstream_session_id={} virtual_host_id={}",
            maybe_user_config.is_some(),
            maybe_downstream_session.is_some(),
            maybe_virtual_host_id.is_some()
        );

        let Some(downstream_session_id) = maybe_downstream_session else {
            return Err(ErrorData {
                code: ErrorCode::INTERNAL_ERROR,
                message: "Routing problem... downstream session id not created".into(),
                data: None,
            });
        };

        let Some(user_config) = maybe_user_config else {
            return Err(ErrorData {
                code: ErrorCode::INTERNAL_ERROR,
                message: "Routing problem... user config not found".into(),
                data: None,
            });
        };

        let Some(virtual_host_id) = maybe_virtual_host_id else {
            return Err(ErrorData {
                code: ErrorCode::INTERNAL_ERROR,
                message: "Routing problem... virutal host not known".into(),
                data: None,
            });
        };

        let Some(virtual_host) = user_config.virtual_hosts.get(virtual_host_id.value()) else {
            return Err(ErrorData {
                code: ErrorCode::RESOURCE_NOT_FOUND,
                message: "No configuration".into(),
                data: None,
            });
        };

        let Some(claims) = maybe_claims else {
            return Err(ErrorData {
                code: ErrorCode::INTERNAL_ERROR,
                message: "Routing problem... claims not found".into(),
                data: None,
            });
        };

        Ok(InitializeCallContext {
            virtual_host,
            virtual_host_id: virtual_host_id.value(),
            downstream_session_id,
            principal: &claims.sub,
            session_key: SessionKey::new(&claims.sub, virtual_host_id.value(), downstream_session_id.value()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::SessionKey;

    #[test]
    fn session_key_scopes_subject_virtual_host_and_session() {
        let key = SessionKey::new("subject", "virtual-host", "session");

        assert_ne!(key, SessionKey::new("other-subject", "virtual-host", "session"));
        assert_ne!(key, SessionKey::new("subject", "other-virtual-host", "session"));
        assert_ne!(key, SessionKey::new("subject", "virtual-host", "other-session"));
    }

    #[test]
    fn session_key_display_redacts_components() {
        let key = SessionKey::new("subject", "virtual-host", "session");

        assert_eq!(key.to_string(), "[redacted-key]");
        assert!(!key.to_string().contains("subject"));
        assert!(!key.to_string().contains("session"));
        assert_eq!(format!("{key:?}"), "SessionKey([redacted])");
        assert!(!format!("{key:?}").contains("subject"));
        assert!(!format!("{key:?}").contains("session"));
    }
}
