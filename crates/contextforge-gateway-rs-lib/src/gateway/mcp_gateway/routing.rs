use super::{ResourceSubscriptions, SessionKey};
use crate::gateway::session_manager::SessionManager;
use itertools::Itertools;
use rmcp::{
    ErrorData,
    model::{
        ClientRequest, ErrorCode, ListResourcesResult, ListToolsResult, Resource, ResourcesCapability,
        ServerCapabilities, ServerResult, SubscribeRequest, SubscribeRequestParams, Tool, ToolsCapability,
        UnsubscribeRequest, UnsubscribeRequestParams,
    },
    service::{PeerRequestOptions, ServiceError},
};
use std::time::Duration;
use tracing::warn;

pub(super) async fn remove_resource_subscription(
    subscriptions: &ResourceSubscriptions,
    session_key: &SessionKey,
    backend_name: &str,
    public_resource_uri: &str,
) {
    let mut subscriptions = subscriptions.write().await;
    if let Some(session_subs) = subscriptions.get_mut(session_key) {
        if let Some(backend_subs) = session_subs.get_mut(backend_name) {
            backend_subs.remove(public_resource_uri);
            if backend_subs.is_empty() {
                session_subs.remove(backend_name);
            }
        }
        if session_subs.is_empty() {
            subscriptions.remove(session_key);
        }
    }
}

pub(super) fn resource_subscription_target(
    session_manager: &SessionManager<'_>,
    resource_uri: &str,
) -> Result<(String, String), ErrorData> {
    let backend_names = session_manager.get_backend_names();
    let Some(BackendResourcePair { backend_name, resource_uri }) = split_resource_name(resource_uri, &backend_names)
    else {
        return Err(ErrorData {
            code: ErrorCode::INTERNAL_ERROR,
            message: "Routing problem... wrong resource name".into(),
            data: None,
        });
    };

    Ok((backend_name.to_owned(), resource_uri.to_owned()))
}

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq)]
pub(super) struct BackendToolPair<'a> {
    pub(super) backend_name: &'a str,
    pub(super) tool_name: &'a str,
}

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq)]
pub(super) struct BackendResourcePair<'a> {
    pub(super) backend_name: &'a str,
    pub(super) resource_uri: &'a str,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ResourceSubscriptionAction {
    Subscribe,
    Unsubscribe,
}

pub(super) async fn forward_resource_subscription(
    session_manager: &SessionManager<'_>,
    backend_name: &str,
    resource_uri: &str,
    action: ResourceSubscriptionAction,
    timeout: Duration,
) -> Result<(), ErrorData> {
    let Some(service_holder) = session_manager.borrow_transport(backend_name).await else {
        return Err(ErrorData {
            code: ErrorCode::INTERNAL_ERROR,
            message: "Routing problem... got no response from backend".into(),
            data: None,
        });
    };
    let Some(service) = service_holder.running_service else {
        return Err(ErrorData {
            code: ErrorCode::INTERNAL_ERROR,
            message: "Routing problem... got no response from backend".into(),
            data: None,
        });
    };

    let request = match action {
        ResourceSubscriptionAction::Subscribe => {
            ClientRequest::SubscribeRequest(SubscribeRequest::new(SubscribeRequestParams::new(resource_uri.to_owned())))
        },
        ResourceSubscriptionAction::Unsubscribe => ClientRequest::UnsubscribeRequest(UnsubscribeRequest::new(
            UnsubscribeRequestParams::new(resource_uri.to_owned()),
        )),
    };
    let mut options = PeerRequestOptions::no_options();
    options.timeout = Some(timeout);
    let response = match service.send_cancellable_request(request, options).await {
        Ok(handle) => handle.await_response().await,
        Err(error) => Err(error),
    };

    match response {
        Ok(ServerResult::EmptyResult(_)) => Ok(()),
        Ok(_) => Err(ErrorData::internal_error("Backend resource subscription failed", None)),
        Err(ServiceError::Timeout { .. }) => {
            warn!(backend = backend_name, "backend resource subscription forwarding timed out");
            Err(ErrorData::internal_error("Backend resource subscription timed out", None))
        },
        Err(error) => {
            warn!(?error, "backend resource subscription forwarding failed");
            Err(ErrorData::internal_error("Backend resource subscription failed", None))
        },
    }
}

pub(super) fn split_tool_name<'a, T: AsRef<str> + ?Sized, N: AsRef<str>>(
    tool_name: &'a T,
    backend_names: &'a [N],
) -> Option<BackendToolPair<'a>> {
    split_backend_prefixed_name(tool_name, backend_names)
        .map(|(backend_name, tool_name)| BackendToolPair { backend_name, tool_name })
}

pub(super) const TEST_IMAGE_DATA: &str =
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8/5+hHgAHggJ/PchI7wAAAABJRU5ErkJggg==";
// Small base64-encoded WAV (silence)

pub(super) fn merge_capabilities(server_capabilities: &[Option<ServerCapabilities>]) -> ServerCapabilities {
    let mut capabilities =
        ServerCapabilities::builder().enable_prompts().enable_resources().enable_tools().enable_logging().build();

    let resources = capabilities.resources.get_or_insert_with(ResourcesCapability::default);
    resources.subscribe = backend_capability(server_capabilities, |capabilities| {
        capabilities.resources.as_ref().and_then(|resources| resources.subscribe)
    })
    .then_some(true);
    resources.list_changed = backend_capability(server_capabilities, |capabilities| {
        capabilities.resources.as_ref().and_then(|resources| resources.list_changed)
    })
    .then_some(true);

    capabilities.tools.get_or_insert_with(ToolsCapability::default).list_changed =
        backend_capability(server_capabilities, |capabilities| {
            capabilities.tools.as_ref().and_then(|tools| tools.list_changed)
        })
        .then_some(true);

    capabilities
}

fn backend_capability(
    server_capabilities: &[Option<ServerCapabilities>],
    supports: impl Fn(&ServerCapabilities) -> Option<bool>,
) -> bool {
    server_capabilities.iter().any(|capabilities| capabilities.as_ref().and_then(&supports).unwrap_or(false))
}

pub(super) fn merge_tools(tools: Vec<(String, ListToolsResult)>) -> Vec<Tool> {
    tools
        .into_iter()
        .flat_map(|(backend_name, result)| {
            result.tools.into_iter().map(move |mut t| {
                t.name = format!("{backend_name}-{}", t.name).into();
                t
            })
        })
        .sorted_by(|t, o| t.name.cmp(&o.name))
        .collect::<Vec<_>>()
}

pub(super) fn merge_resources(resources: Vec<(String, ListResourcesResult)>) -> Vec<Resource> {
    resources
        .into_iter()
        .flat_map(|(backend_name, result)| {
            result.resources.into_iter().map(move |mut t| {
                t.name = format!("{backend_name}-{}", t.name);
                t.uri = format!("{backend_name}-{}", t.uri);
                t
            })
        })
        .sorted_by(|t, o| t.name.cmp(&o.name))
        .collect::<Vec<_>>()
}

pub(super) fn split_resource_name<'a, T: AsRef<str> + ?Sized, N: AsRef<str>>(
    resource_uri: &'a T,
    backend_names: &'a [N],
) -> Option<BackendResourcePair<'a>> {
    split_backend_prefixed_name(resource_uri, backend_names)
        .map(|(backend_name, resource_uri)| BackendResourcePair { backend_name, resource_uri })
}

fn split_backend_prefixed_name<'a, T: AsRef<str> + ?Sized, N: AsRef<str>>(
    value: &'a T,
    backend_names: &'a [N],
) -> Option<(&'a str, &'a str)> {
    let value = value.as_ref();
    backend_names
        .iter()
        .filter_map(|name| {
            let name = name.as_ref();
            value.strip_prefix(name).and_then(|suffix| suffix.strip_prefix('-')).map(|unprefixed| (name, unprefixed))
        })
        .max_by_key(|(name, _)| name.len())
}
