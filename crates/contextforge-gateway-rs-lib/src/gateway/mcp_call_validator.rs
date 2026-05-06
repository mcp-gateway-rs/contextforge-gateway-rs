use http::request::Parts;
//use rmcp::{ErrorData, RoleServer, model::ErrorCode, service::RequestContext, transport::DownstreamSessionId};
use rmcp::{
    ErrorData, RoleServer, model::ErrorCode, service::RequestContext,
    transport::streamable_http_server::tower::DownstreamSessionId,
};
use tracing::info;

use crate::{
    layers::{session_id::SessionId, virtual_host_id::VirtualHostId},
    user_config_store::{UserConfig, VirtualHost},
};

pub struct AuthorizedCallValidator<'a> {
    call_name: &'a str,
    ctx: &'a RequestContext<RoleServer>,
}

impl<'a> AuthorizedCallValidator<'a> {
    pub fn new(call_name: &'a str, ctx: &'a RequestContext<RoleServer>) -> Self {
        Self { call_name, ctx }
    }
    pub fn validate(self) -> Result<(&'a VirtualHost, &'a SessionId), ErrorData> {
        let maybe_parts = self.ctx.extensions.get::<Parts>();
        let maybe_session_id = maybe_parts.and_then(|parts| parts.extensions.get::<SessionId>());
        let maybe_user_config = maybe_parts.and_then(|parts| parts.extensions.get::<UserConfig>());

        let maybe_virtual_host_id = maybe_parts.and_then(|parts| parts.extensions.get::<VirtualHostId>());
        info!(
            "{} user_config = {maybe_user_config:#?} session_id = {maybe_session_id:#?} virtual_host_id = {maybe_virtual_host_id:#?}",
            self.call_name
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

        Ok((virtual_host, session_id))
    }
}

pub struct InitializeCallValidator<'a> {
    ctx: &'a RequestContext<RoleServer>,
}

impl<'a> InitializeCallValidator<'a> {
    pub fn new(ctx: &'a RequestContext<RoleServer>) -> Self {
        Self { ctx }
    }
    pub fn validate(self) -> Result<(&'a VirtualHost, &'a DownstreamSessionId), ErrorData> {
        let maybe_parts = self.ctx.extensions.get::<Parts>();

        let maybe_downstream_session = self.ctx.extensions.get::<DownstreamSessionId>();
        let maybe_user_config = maybe_parts.and_then(|parts| parts.extensions.get::<UserConfig>());
        let maybe_virtual_host_id = maybe_parts.and_then(|parts| parts.extensions.get::<VirtualHostId>());
        info!(
            "intialize user_config = {maybe_user_config:#?} downstream_session_id = {maybe_downstream_session:#?} virtual_host_id = {maybe_virtual_host_id:#?}"
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

        Ok((virtual_host, downstream_session_id))
    }
}
