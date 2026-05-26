use std::{collections::HashMap, sync::Arc};

use contextforge_gateway_rs_cpex::{GatewayPluginRuntimeHandle, ToolPreCallResult};
use itertools::Itertools;
use rmcp::{
    ErrorData, RoleServer, ServerHandler, ServiceExt,
    model::{
        AnnotateAble, CallToolRequest, CallToolRequestParams, CallToolResult, ClientRequest, CompleteRequestParams,
        CompleteResult, CompletionInfo, ErrorCode, GetPromptRequestParams, GetPromptResult, Implementation,
        InitializeRequestParams, InitializeResult, ListPromptsResult, ListResourceTemplatesResult, ListResourcesResult,
        ListToolsResult, PaginatedRequestParams, Prompt, PromptArgument, PromptMessage, PromptMessageContent,
        PromptMessageRole, RawImageContent, RawResourceTemplate, ReadResourceRequestParams, ReadResourceResult,
        Reference, Resource, ResourcesCapability, ServerCapabilities, SetLevelRequestParams, SubscribeRequestParams,
        Tool, ToolsCapability, UnsubscribeRequestParams,
    },
    service::{Peer, PeerRequestOptions, RequestContext},
    transport::{StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig},
};
use tokio::sync::{Mutex, broadcast};
use tracing::{debug, info, warn};
use typed_builder::TypedBuilder;

use super::{
    backend_notifications::{
        BackendClientHandler, BackendClientService, BackendNotification, LogLevels, ResourceSubscriptions,
        SessionNotificationRelay, await_call_tool_response_with_notifications, drain_buffered_session_notifications,
        relay_backend_notifications,
    },
    mcp_call_validator::{AuthorizedCallValidator, SessionKey},
};
pub use crate::gateway::session_store::LocalUserSessionStore;
use crate::gateway::{
    mcp_call_validator::InitializeCallValidator,
    session_manager::SessionManager,
    session_store::{UserSession, UserSessionStore},
};

#[derive(Clone, TypedBuilder)]
#[builder(field_defaults(setter(prefix = "with_")))]
pub struct McpService<T>
where
    T: UserSessionStore,
{
    #[builder(default = Arc::new(Mutex::new(HashMap::new())))]
    subscriptions: ResourceSubscriptions,
    #[builder(default = Arc::new(Mutex::new(HashMap::new())))]
    log_levels: LogLevels,
    #[builder(default = Arc::new(Mutex::new(HashMap::new())))]
    transports: Arc<Mutex<HashMap<BackendTransportKey, BackendTransportService>>>,
    #[builder(default = Arc::new(Mutex::new(HashMap::new())))]
    notification_relays: Arc<Mutex<HashMap<BackendTransportKey, tokio::task::JoinHandle<()>>>>,
    http_client: reqwest::Client,
    user_session_store: T,
    #[builder(default)]
    plugin_runtime: Option<GatewayPluginRuntimeHandle>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BackendTransportKey {
    backend_name: String,
    session_key: SessionKey,
}

#[derive(Debug)]
pub struct ServiceHolder {
    pub name: String,
    pub running_service: Option<BackendClientService>,
}

impl ServiceHolder {
    pub fn new(name: String, running_service: Option<BackendClientService>) -> ServiceHolder {
        Self { name, running_service }
    }
}

#[derive(Debug)]
pub struct BackendTransportService {
    #[expect(dead_code, reason = "stored backend capabilities are kept with transport state for future routing")]
    capabilities: Option<ServerCapabilities>,
    pub(crate) service: Option<BackendClientService>,
    pub(crate) backend_notifications: broadcast::Sender<BackendNotification>,
}

impl From<(&str, &SessionKey)> for BackendTransportKey {
    fn from((backend_name, session_key): (&str, &SessionKey)) -> Self {
        Self { backend_name: backend_name.to_owned(), session_key: session_key.clone() }
    }
}

impl<T> ServerHandler for McpService<T>
where
    T: UserSessionStore + Send + Sync + 'static,
{
    async fn initialize(
        &self,
        request: InitializeRequestParams,
        cx: RequestContext<RoleServer>,
    ) -> Result<InitializeResult, ErrorData> {
        let call_validator = InitializeCallValidator::new(&cx);
        let call_context = call_validator.validate()?;
        self.cleanup_notification_state(&call_context.session_key).await;
        let user_session = UserSession::new(
            call_context.principal.to_owned(),
            Arc::clone(&call_context.downstream_session_id.session_id),
        );
        let session_mapping = self
            .user_session_store
            .get_session(&user_session)
            .await
            .map_err(|_| ErrorData {
                code: ErrorCode::INTERNAL_ERROR,
                message: "Internal problem... session store can't be accessed".into(),
                data: None,
            })?
            .unwrap_or_default();

        let tasks: Vec<_> = call_context
            .virtual_host
            .backends
            .iter()
            .map(|(name, backend)| {
                let client = self.http_client.clone();
                let request = request.clone();
                let backend_url = backend.url.clone();
                let backend_notifications = broadcast::channel(64).0;
                let backend_notification_rx = backend_notifications.subscribe();
                let backend_notifications_for_transport = backend_notifications.clone();

                Box::pin(async move {
                    let mut headers = HashMap::new();
                    if backend_url.scheme() == "https"
                        && let Some(host) = backend_url.host_str()
                    {
                        let host = backend_url.port().map_or_else(|| host.to_owned(), |port| format!("{host}:{port}"));

                        if let Ok(value) = http::HeaderValue::from_str(&host) {
                            headers.insert(http::header::HOST, value);
                        } else {
                            warn!("Really can't set the host header for {:?}", backend_url.host_str());
                        }
                    }

                    let config =
                        StreamableHttpClientTransportConfig::with_uri(backend_url.to_string()).custom_headers(headers);
                    let transport = StreamableHttpClientTransport::with_client(client, config);
                    let backend_client = BackendClientHandler { client_info: request, backend_notifications };
                    let maybe_running_service = backend_client.serve(transport).await;
                    if let Ok(running_service) = maybe_running_service {
                        info!("initialize: initialized backend {name:?}");
                        (name, backend_notifications_for_transport, backend_notification_rx, Some(running_service))
                    } else {
                        warn!("initialize: unable to initialize backend {name:?}");
                        (name, backend_notifications_for_transport, backend_notification_rx, None)
                    }
                })
            })
            .collect();

        let initialization_results = futures::future::join_all(tasks).await;

        let mut relay_receivers = Vec::new();
        let (capabilities, backend_services): (Vec<_>, Vec<_>) = initialization_results
            .into_iter()
            .map(|(name, backend_notifications, backend_notification_rx, running_service): (_, _, _, _)| {
                info!("initialize: adding transport for backend {name} running={}", running_service.is_some());

                let server_capabilities = running_service
                    .as_ref()
                    .and_then(|rs| rs.peer().peer_info().as_ref().map(|pi| pi.capabilities.clone()));
                if running_service.is_some() {
                    relay_receivers.push((name.clone(), backend_notification_rx));
                }

                (
                    server_capabilities.clone(),
                    (
                        name.clone(),
                        BackendTransportService {
                            capabilities: server_capabilities,
                            service: running_service.map(Arc::new),
                            backend_notifications,
                        },
                    ),
                )
            })
            .unzip();

        let _ = self.user_session_store.set_session(&user_session, &session_mapping).await;

        let mut transports = self.transports.lock().await;
        for (name, svc) in backend_services {
            transports.entry(BackendTransportKey::from((name.as_str(), &call_context.session_key))).insert_entry(svc);
        }
        drop(transports);

        for (backend_name, notification_rx) in relay_receivers {
            self.spawn_notification_relay(
                notification_rx,
                backend_name,
                call_context.session_key.clone(),
                cx.peer.clone(),
            )
            .await;
        }

        Ok(InitializeResult::new(merge_capabilities(&capabilities))
            .with_server_info(Implementation::new("rust-conformance-server", "0.1.0"))
            .with_instructions("Rust MCP conformance test server"))
    }

    async fn ping(&self, _cx: RequestContext<RoleServer>) -> Result<(), ErrorData> {
        Ok(())
    }

    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        cx: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let mcp_call_validator = AuthorizedCallValidator::new("list_tools", &cx);
        let call_context = mcp_call_validator.validate()?;

        let session_manager =
            SessionManager::new(call_context.virtual_host, &call_context.session_key, &self.transports);
        let backend_transports: Vec<_> = session_manager.borrow_transports().await;

        let list_tools_tasks = backend_transports
            .into_iter()
            .map(|service_holder| {
                let request = request.clone();
                async move {
                    if let Some(service) = service_holder.running_service {
                        let response = service.list_tools(request).await;
                        (service_holder.name, Some(response))
                    } else {
                        (service_holder.name, None)
                    }
                }
            })
            .collect::<Vec<_>>();

        let responses = futures::future::join_all(list_tools_tasks).await;

        let responses = responses
            .into_iter()
            .filter_map(
                |(name, response)| if let Some(Ok(response)) = response { Some((name, response)) } else { None },
            )
            .collect::<Vec<_>>();

        let merged_list_tools = merge_tools(responses);

        Ok(ListToolsResult { meta: None, tools: merged_list_tools, next_cursor: None })
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let mcp_call_validator = AuthorizedCallValidator::new("call_tool", &cx);
        let call_context = mcp_call_validator.validate()?;
        let session_manager =
            SessionManager::new(call_context.virtual_host, &call_context.session_key, &self.transports);

        let backend_names = session_manager.get_backend_names();

        let Some(BackendToolPair { backend_name, tool_name }) = split_tool_name(&request.name, &backend_names) else {
            return Err(ErrorData {
                code: ErrorCode::INTERNAL_ERROR,
                message: "Routing problem... wrong tool name".into(),
                data: None,
            });
        };
        let backend_name = backend_name.to_owned();
        let tool_name = tool_name.to_owned();
        let request_name = request.name.clone();
        let pre_result = if let Some(plugin_runtime) = &self.plugin_runtime {
            plugin_runtime.before_tool_call(&request, &tool_name, &backend_name).await?
        } else {
            ToolPreCallResult::unchanged()
        };
        let post_state = pre_result.state;
        let mut routed_request = request;
        pre_result.arguments.apply_to_request(&mut routed_request, &tool_name);

        let notification_rx = session_manager.subscribe_backend_notifications(&backend_name).await;
        let backend_transports = session_manager.borrow_transports().await;
        let downstream_peer = cx.peer.clone();
        let downstream_progress_token = cx.meta.get_progress_token();
        self.ensure_notification_relay(&session_manager, &backend_name, &call_context.session_key, cx.peer.clone())
            .await;

        let mut target_service = None;
        for service_holder in backend_transports {
            debug!(
                "call_tool: Finding backend for {} {service_holder:?} {backend_name} tool_name = {tool_name}",
                &request_name,
            );
            if service_holder.name == backend_name {
                if target_service.is_some() {
                    warn!("call_tool: More than one tool matching for tool name {}", request_name);
                    session_manager.cleanup_backends("call_tool: invalid session.. duplicate tools detected").await;
                    self.cleanup_notification_state(&call_context.session_key).await;
                    return Err(ErrorData {
                        code: ErrorCode::INVALID_REQUEST,
                        message: "Routing problem... multiple matching tools".into(),
                        data: None,
                    });
                }
                target_service = Some(service_holder);
            }
        }

        let Some(target_service) = target_service else {
            return Err(ErrorData {
                code: ErrorCode::INTERNAL_ERROR,
                message: "Routing problem... got no responses from backends".into(),
                data: None,
            });
        };
        let Some(service) = target_service.running_service else {
            warn!(
                "call_tool: trying to call a tool for which we have no backend {target_service:?} {backend_name} tool_name = {tool_name}"
            );
            return Err(ErrorData {
                code: ErrorCode::INTERNAL_ERROR,
                message: "Routing problem... got no responses from backends".into(),
                data: None,
            });
        };

        let request_handle = service
            .send_cancellable_request(
                ClientRequest::CallToolRequest(CallToolRequest::new(routed_request)),
                PeerRequestOptions::no_options(),
            )
            .await;
        let response = match request_handle {
            Ok(request_handle) => {
                await_call_tool_response_with_notifications(
                    notification_rx,
                    downstream_peer,
                    downstream_progress_token,
                    request_handle,
                )
                .await
            },
            Err(error) => Err(error),
        };
        let service_name = target_service.name;

        let response = response.map_err(|error| {
            warn!("call_tool: backend {service_name} {error:?}");
            ErrorData {
                code: ErrorCode::INTERNAL_ERROR,
                message: "Routing problem... got no responses from backends".into(),
                data: None,
            }
        })?;
        info!("call_tool: backend {service_name} completed");

        match (&self.plugin_runtime, post_state) {
            (Some(plugin_runtime), Some(post_state)) => {
                plugin_runtime.after_tool_call(&tool_name, response, Some(post_state)).await
            },
            _ => Ok(response),
        }
    }

    async fn list_resources(
        &self,
        request: Option<PaginatedRequestParams>,
        cx: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        let mcp_call_validator = AuthorizedCallValidator::new("list_resources", &cx);
        let call_context = mcp_call_validator.validate()?;

        let session_manager =
            SessionManager::new(call_context.virtual_host, &call_context.session_key, &self.transports);
        let backend_transports: Vec<_> = session_manager.borrow_transports().await;

        let list_resources_tasks = backend_transports
            .into_iter()
            .map(|service_holder| {
                let request = request.clone();
                async move {
                    if let Some(service) = service_holder.running_service {
                        let response = service.list_resources(request).await;
                        (service_holder.name, Some(response))
                    } else {
                        (service_holder.name, None)
                    }
                }
            })
            .collect::<Vec<_>>();

        let responses = futures::future::join_all(list_resources_tasks).await;

        let responses = responses
            .into_iter()
            .filter_map(
                |(name, response)| if let Some(Ok(response)) = response { Some((name, response)) } else { None },
            )
            .collect::<Vec<_>>();

        let merged_list_resources = merge_resources(responses);

        Ok(ListResourcesResult { meta: None, resources: merged_list_resources, next_cursor: None })
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        cx: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, ErrorData> {
        let mcp_call_validator = AuthorizedCallValidator::new("read_resource", &cx);
        let call_context = mcp_call_validator.validate()?;
        let session_manager =
            SessionManager::new(call_context.virtual_host, &call_context.session_key, &self.transports);

        let backend_names = session_manager.get_backend_names();

        let Some(BackendResourcePair { backend_name, resource_uri }) =
            split_resource_name(&request.uri, &backend_names)
        else {
            return Err(ErrorData {
                code: ErrorCode::INTERNAL_ERROR,
                message: "Routing problem... wrong resource name".into(),
                data: None,
            });
        };

        let backend_transports = session_manager.borrow_transports().await;

        let read_resource_tasks = backend_transports
            .into_iter()
            .filter_map(|service_holder| {
                debug!(
                    "read_resource: Finding backend for {} {service_holder:?} {backend_name} read_resource = {resource_uri}",
                    &request.uri,
                );
                (service_holder.name == backend_name).then(|| {
                    let mut request = request.clone();
                    request.uri = String::from(resource_uri);
                    async move {
                        if let Some(service) = service_holder.running_service {
                            let response = service.read_resource(request).await;

                            (service_holder.name, Some(response))
                        } else {
                            warn!("call_tool: trying to call a tool for which we have no backend {service_holder:?} {backend_name} resource_name = {resource_uri}");
                            (service_holder.name, None)
                        }
                    }
                })
            })
            .collect::<Vec<_>>();

        if read_resource_tasks.len() > 1 {
            warn!("read_resource: More than one tool matching for tool name {}", request.uri);

            session_manager.cleanup_backends("read_resource: invalid session.. duplicate resources detected").await;
            self.cleanup_notification_state(&call_context.session_key).await;

            return Err(ErrorData {
                code: ErrorCode::INVALID_REQUEST,
                message: "Routing problem... multiple matching resources".into(),
                data: None,
            });
        }

        let responses = futures::future::join_all(read_resource_tasks).await;

        let responses = responses
            .into_iter()
            .filter_map(
                |(name, response)| if let Some(Ok(response)) = response { Some((name, response)) } else { None },
            )
            .collect::<Vec<_>>();

        responses.into_iter().next().map(|(_, r)| r).ok_or(ErrorData {
            code: ErrorCode::INTERNAL_ERROR,
            message: "Routing problem... got no responses from backends".into(),
            data: None,
        })
    }

    async fn list_resource_templates(
        &self,
        _request: Option<PaginatedRequestParams>,
        _cx: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, ErrorData> {
        info!("list_resource_templates");
        Ok(ListResourceTemplatesResult {
            meta: None,
            resource_templates: vec![
                RawResourceTemplate {
                    uri_template: "test://template/{id}/data".into(),
                    name: "Dynamic Resource".into(),
                    title: None,
                    description: Some("A dynamic resource with parameter substitution".into()),
                    mime_type: Some("application/json".into()),
                    icons: None,
                }
                .no_annotation(),
            ],
            next_cursor: None,
        })
    }

    async fn subscribe(
        &self,
        request: SubscribeRequestParams,
        cx: RequestContext<RoleServer>,
    ) -> Result<(), ErrorData> {
        let mcp_call_validator = AuthorizedCallValidator::new("subscribe", &cx);
        let call_context = mcp_call_validator.validate()?;
        let session_manager =
            SessionManager::new(call_context.virtual_host, &call_context.session_key, &self.transports);
        let (backend_name, resource_uri) = resource_subscription_target(&session_manager, &request.uri)?;

        self.stop_notification_relay(&backend_name, &call_context.session_key).await;
        let buffered_notifications = session_manager.subscribe_backend_notifications(&backend_name).await;

        if let Err(error) = forward_resource_subscription(
            &session_manager,
            &backend_name,
            &resource_uri,
            ResourceSubscriptionAction::Subscribe,
        )
        .await
        {
            self.ensure_notification_relay(&session_manager, &backend_name, &call_context.session_key, cx.peer.clone())
                .await;
            return Err(error);
        }

        self.subscriptions
            .lock()
            .await
            .entry(call_context.session_key.clone())
            .or_default()
            .insert(request.uri.clone());

        if let Some(notification_rx) = buffered_notifications {
            drain_buffered_session_notifications(SessionNotificationRelay {
                notification_rx,
                backend_name: backend_name.clone(),
                downstream_peer: cx.peer.clone(),
                resource_subscriptions: Arc::clone(&self.subscriptions),
                log_levels: Arc::clone(&self.log_levels),
                session_key: call_context.session_key.clone(),
            })
            .await;
        }
        self.ensure_notification_relay(&session_manager, &backend_name, &call_context.session_key, cx.peer.clone())
            .await;

        Ok(())
    }

    async fn unsubscribe(
        &self,
        request: UnsubscribeRequestParams,
        cx: RequestContext<RoleServer>,
    ) -> Result<(), ErrorData> {
        let mcp_call_validator = AuthorizedCallValidator::new("unsubscribe", &cx);
        let call_context = mcp_call_validator.validate()?;
        let session_manager =
            SessionManager::new(call_context.virtual_host, &call_context.session_key, &self.transports);
        let (backend_name, resource_uri) = resource_subscription_target(&session_manager, &request.uri)?;
        forward_resource_subscription(
            &session_manager,
            &backend_name,
            &resource_uri,
            ResourceSubscriptionAction::Unsubscribe,
        )
        .await?;

        remove_resource_subscription(&self.subscriptions, &call_context.session_key, request.uri.as_str()).await;
        Ok(())
    }

    async fn list_prompts(
        &self,
        _request: Option<PaginatedRequestParams>,
        _cx: RequestContext<RoleServer>,
    ) -> Result<ListPromptsResult, ErrorData> {
        info!("list_prompts");

        Ok(ListPromptsResult {
            meta: None,
            prompts: vec![
                Prompt::new("test_simple_prompt", Some("A simple test prompt with no arguments"), None),
                Prompt::new(
                    "test_prompt_with_arguments",
                    Some("A test prompt that accepts arguments"),
                    Some(vec![
                        PromptArgument::new("name").with_description("The name to greet").with_required(true),
                        PromptArgument::new("style").with_description("The greeting style").with_required(false),
                    ]),
                ),
                Prompt::new(
                    "test_prompt_with_embedded_resource",
                    Some("A test prompt that includes an embedded resource"),
                    None,
                ),
                Prompt::new("test_prompt_with_image", Some("A test prompt that includes an image"), None),
            ],
            next_cursor: None,
        })
    }

    async fn get_prompt(
        &self,
        request: GetPromptRequestParams,
        _cx: RequestContext<RoleServer>,
    ) -> Result<GetPromptResult, ErrorData> {
        info!("get_prompt");
        match request.name.as_str() {
            "test_simple_prompt" => Ok(GetPromptResult::new(vec![PromptMessage::new_text(
                PromptMessageRole::User,
                "This is a simple test prompt.",
            )])
            .with_description("A simple test prompt")),
            "test_prompt_with_arguments" => {
                let args = request.arguments.unwrap_or_default();
                let name = args.get("name").and_then(|v| v.as_str()).unwrap_or("World");
                let style = args.get("style").and_then(|v| v.as_str()).unwrap_or("friendly");
                Ok(GetPromptResult::new(vec![PromptMessage::new_text(
                    PromptMessageRole::User,
                    format!("Please greet {name} in a {style} style."),
                )])
                .with_description("A prompt with arguments"))
            },
            "test_prompt_with_embedded_resource" => Ok(GetPromptResult::new(vec![
                PromptMessage::new_text(PromptMessageRole::User, "Here is a resource:"),
                PromptMessage::new_resource(
                    PromptMessageRole::User,
                    "test://static-text".into(),
                    Some("text/plain".into()),
                    Some("Resource content for prompt".into()),
                    None,
                    None,
                    None,
                ),
            ])
            .with_description("A prompt with an embedded resource")),
            "test_prompt_with_image" => {
                let image_content =
                    RawImageContent { data: TEST_IMAGE_DATA.into(), mime_type: "image/png".into(), meta: None };
                Ok(GetPromptResult::new(vec![
                    PromptMessage::new_text(PromptMessageRole::User, "Here is an image:"),
                    PromptMessage::new(
                        PromptMessageRole::User,
                        PromptMessageContent::Image { image: image_content.no_annotation() },
                    ),
                ])
                .with_description("A prompt with an image"))
            },
            _ => Err(ErrorData::invalid_params(format!("Unknown prompt: {}", request.name), None)),
        }
    }

    async fn complete(
        &self,
        request: CompleteRequestParams,
        _cx: RequestContext<RoleServer>,
    ) -> Result<CompleteResult, ErrorData> {
        info!("complete");
        let values = match &request.r#ref {
            Reference::Resource(_) => {
                if request.argument.name == "id" {
                    vec!["1".into(), "2".into(), "3".into()]
                } else {
                    vec![]
                }
            },
            Reference::Prompt(prompt_ref) => {
                if request.argument.name == "name" {
                    vec!["Alice".into(), "Bob".into(), "Charlie".into()]
                } else if request.argument.name == "style" {
                    vec!["friendly".into(), "formal".into(), "casual".into()]
                } else {
                    vec![prompt_ref.name.clone()]
                }
            },
        };
        Ok(CompleteResult::new(CompletionInfo::new(values).map_err(|e| ErrorData::internal_error(e, None))?))
    }

    async fn set_level(&self, request: SetLevelRequestParams, cx: RequestContext<RoleServer>) -> Result<(), ErrorData> {
        let mcp_call_validator = AuthorizedCallValidator::new("set_level", &cx);
        let call_context = mcp_call_validator.validate()?;
        self.log_levels.lock().await.insert(call_context.session_key.clone(), request.level);
        let session_manager =
            SessionManager::new(call_context.virtual_host, &call_context.session_key, &self.transports);
        for service_holder in session_manager.borrow_transports().await {
            if let Some(service) = service_holder.running_service
                && let Err(error) = service.set_level(request.clone()).await
            {
                debug!(backend = service_holder.name, ?error, "backend logging set_level forwarding failed");
            }
        }
        Ok(())
    }
}

impl<T> McpService<T>
where
    T: UserSessionStore,
{
    async fn ensure_notification_relay(
        &self,
        session_manager: &SessionManager<'_>,
        backend_name: &str,
        session_key: &SessionKey,
        downstream_peer: Peer<RoleServer>,
    ) {
        let key = BackendTransportKey::from((backend_name, session_key));
        if self.notification_relay_is_active(&key).await {
            return;
        }
        let Some(notification_rx) = session_manager.subscribe_backend_notifications(backend_name).await else {
            return;
        };
        self.spawn_notification_relay(notification_rx, backend_name.to_owned(), session_key.clone(), downstream_peer)
            .await;
    }

    async fn spawn_notification_relay(
        &self,
        notification_rx: broadcast::Receiver<BackendNotification>,
        backend_name: String,
        session_key: SessionKey,
        downstream_peer: Peer<RoleServer>,
    ) {
        self.prune_finished_notification_relays().await;
        let key = BackendTransportKey::from((backend_name.as_str(), &session_key));
        {
            let mut relays = self.notification_relays.lock().await;
            if relays.get(&key).is_some_and(|handle| !handle.is_finished()) {
                return;
            }
            relays.remove(&key);
        }
        let subscriptions = Arc::clone(&self.subscriptions);
        let log_levels = Arc::clone(&self.log_levels);
        let mut relays = self.notification_relays.lock().await;
        relays.entry(key).or_insert_with(|| {
            tokio::spawn(async move {
                relay_backend_notifications(SessionNotificationRelay {
                    notification_rx,
                    backend_name,
                    downstream_peer,
                    resource_subscriptions: subscriptions,
                    log_levels,
                    session_key,
                })
                .await;
            })
        });
    }

    async fn notification_relay_is_active(&self, key: &BackendTransportKey) -> bool {
        let mut relays = self.notification_relays.lock().await;
        if relays.get(key).is_some_and(|handle| !handle.is_finished()) {
            return true;
        }
        relays.remove(key);
        false
    }

    async fn stop_notification_relay(&self, backend_name: &str, session_key: &SessionKey) {
        let key = BackendTransportKey::from((backend_name, session_key));
        if let Some(handle) = self.notification_relays.lock().await.remove(&key) {
            handle.abort();
        }
    }

    async fn prune_finished_notification_relays(&self) {
        let mut relays = self.notification_relays.lock().await;
        relays.retain(|_, handle| !handle.is_finished());
    }

    async fn cleanup_notification_state(&self, session_key: &SessionKey) {
        cleanup_notification_state(&self.subscriptions, &self.log_levels, &self.notification_relays, session_key).await;
    }
}

async fn cleanup_notification_state(
    subscriptions: &ResourceSubscriptions,
    log_levels: &LogLevels,
    notification_relays: &Arc<Mutex<HashMap<BackendTransportKey, tokio::task::JoinHandle<()>>>>,
    session_key: &SessionKey,
) {
    subscriptions.lock().await.remove(session_key);
    log_levels.lock().await.remove(session_key);
    let mut relays = notification_relays.lock().await;
    let keys = relays.keys().filter(|key| key.session_key == *session_key).cloned().collect::<Vec<_>>();
    for key in keys {
        if let Some(handle) = relays.remove(&key) {
            handle.abort();
        }
    }
}

async fn remove_resource_subscription(
    subscriptions: &ResourceSubscriptions,
    session_key: &SessionKey,
    resource_uri: &str,
) {
    let mut subscriptions = subscriptions.lock().await;
    if let Some(session_subs) = subscriptions.get_mut(session_key) {
        session_subs.remove(resource_uri);
        if session_subs.is_empty() {
            subscriptions.remove(session_key);
        }
    }
}

fn resource_subscription_target(
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
struct BackendToolPair<'a> {
    backend_name: &'a str,
    tool_name: &'a str,
}

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq)]
struct BackendResourcePair<'a> {
    backend_name: &'a str,
    resource_uri: &'a str,
}

#[derive(Clone, Copy)]
enum ResourceSubscriptionAction {
    Subscribe,
    Unsubscribe,
}

async fn forward_resource_subscription(
    session_manager: &SessionManager<'_>,
    backend_name: &str,
    resource_uri: &str,
    action: ResourceSubscriptionAction,
) -> Result<(), ErrorData> {
    let backend_transports = session_manager.borrow_transports().await;
    let mut response = None;
    for service_holder in backend_transports {
        if service_holder.name == backend_name
            && let Some(service) = service_holder.running_service
        {
            response = Some(match action {
                ResourceSubscriptionAction::Subscribe => {
                    service.subscribe(SubscribeRequestParams::new(resource_uri.to_owned())).await
                },
                ResourceSubscriptionAction::Unsubscribe => {
                    service.unsubscribe(UnsubscribeRequestParams::new(resource_uri.to_owned())).await
                },
            });
        }
    }

    let Some(response) = response else {
        return Err(ErrorData {
            code: ErrorCode::INTERNAL_ERROR,
            message: "Routing problem... got no response from backend".into(),
            data: None,
        });
    };

    response.map_err(|error| {
        warn!(?error, "backend resource subscription forwarding failed");
        ErrorData::internal_error("Backend resource subscription failed", None)
    })?;
    Ok(())
}

fn split_tool_name<'a, N: AsRef<str>>(tool_name: &'a str, backend_names: &'a [N]) -> Option<BackendToolPair<'a>> {
    split_prefixed_backend_name(tool_name, backend_names)
        .map(|(backend_name, tool_name)| BackendToolPair { backend_name, tool_name })
}

const TEST_IMAGE_DATA: &str =
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8/5+hHgAHggJ/PchI7wAAAABJRU5ErkJggg==";
// Small base64-encoded WAV (silence)

fn merge_capabilities(server_capabilities: &[Option<ServerCapabilities>]) -> ServerCapabilities {
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

fn merge_tools(tools: Vec<(String, ListToolsResult)>) -> Vec<Tool> {
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

fn merge_resources(resources: Vec<(String, ListResourcesResult)>) -> Vec<Resource> {
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

fn split_resource_name<'a, N: AsRef<str>>(
    resource_uri: &'a str,
    backend_names: &'a [N],
) -> Option<BackendResourcePair<'a>> {
    split_prefixed_backend_name(resource_uri, backend_names)
        .map(|(backend_name, resource_uri)| BackendResourcePair { backend_name, resource_uri })
}

fn split_prefixed_backend_name<'a, N: AsRef<str>>(
    value: &'a str,
    backend_names: &'a [N],
) -> Option<(&'a str, &'a str)> {
    backend_names
        .iter()
        .filter_map(|backend_name| {
            let backend_name = backend_name.as_ref();
            value
                .strip_prefix(backend_name)
                .and_then(|suffix| suffix.strip_prefix('-'))
                .map(|suffix| (backend_name, suffix))
        })
        .max_by_key(|(backend_name, _)| backend_name.len())
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use rmcp::model::LoggingLevel;

    use super::*;

    #[test]
    fn test_splitting() {
        let tool_name = "counter-one-increment";
        let backend_names = vec!["counter-on", "counter-oneee", "counter-one"];
        let pair = BackendToolPair { backend_name: "counter-one", tool_name: "increment" };
        assert_eq!(Some(pair), split_tool_name(tool_name, &backend_names));
        let tool_name = "counter-oneincrement";
        assert_eq!(None, split_tool_name(tool_name, &backend_names));
        let tool_name = "counteroneincrement";
        assert_eq!(None, split_tool_name(tool_name, &backend_names));
        let tool_name = "counter-one-get-value";
        let pair = BackendToolPair { backend_name: "counter-one", tool_name: "get-value" };
        assert_eq!(Some(pair), split_tool_name(tool_name, &backend_names));

        let backend_names = vec!["counter_on", "counter_oneee", "counter_one"];
        let tool_name = "counter_one-get-value";
        let pair = BackendToolPair { backend_name: "counter_one", tool_name: "get-value" };
        assert_eq!(Some(pair), split_tool_name(tool_name, &backend_names));

        let backend_names = vec!["a", "a-b"];
        let tool_name = "a-b-get-value";
        let pair = BackendToolPair { backend_name: "a-b", tool_name: "get-value" };
        assert_eq!(Some(pair), split_tool_name(tool_name, &backend_names));

        let resource_uri = "a-b-memo://insights";
        let pair = BackendResourcePair { backend_name: "a-b", resource_uri: "memo://insights" };
        assert_eq!(Some(pair), split_resource_name(resource_uri, &backend_names));
    }

    #[test]
    fn merge_capabilities_advertises_relayed_notifications() {
        let backend_capabilities = ServerCapabilities::builder()
            .enable_prompts()
            .enable_prompts_list_changed()
            .enable_resources()
            .enable_resources_subscribe()
            .enable_resources_list_changed()
            .enable_tools()
            .enable_tool_list_changed()
            .build();
        let capabilities = merge_capabilities(&[Some(backend_capabilities)]);

        assert_eq!(capabilities.prompts.as_ref().and_then(|c| c.list_changed), None);
        assert_eq!(capabilities.resources.as_ref().and_then(|c| c.subscribe), Some(true));
        assert_eq!(capabilities.resources.as_ref().and_then(|c| c.list_changed), Some(true));
        assert_eq!(capabilities.tools.as_ref().and_then(|c| c.list_changed), Some(true));
    }

    #[test]
    fn merge_capabilities_omits_unsupported_backend_notifications() {
        let capabilities = merge_capabilities(&[]);

        assert_eq!(capabilities.prompts.as_ref().and_then(|c| c.list_changed), None);
        assert_eq!(capabilities.resources.as_ref().and_then(|c| c.subscribe), None);
        assert_eq!(capabilities.resources.as_ref().and_then(|c| c.list_changed), None);
        assert_eq!(capabilities.tools.as_ref().and_then(|c| c.list_changed), None);
    }

    #[tokio::test]
    async fn cleanup_notification_state_removes_session_entries() {
        let session_key = SessionKey::new("subject", "virtual-host", "session-a");
        let subscriptions = Arc::new(Mutex::new(HashMap::from([(
            session_key.clone(),
            std::collections::HashSet::from(["backend-memo://insights".to_owned()]),
        )])));
        let log_levels = Arc::new(Mutex::new(HashMap::from([(session_key.clone(), LoggingLevel::Error)])));
        let notification_relays = Arc::new(Mutex::new(HashMap::new()));

        cleanup_notification_state(&subscriptions, &log_levels, &notification_relays, &session_key).await;

        assert!(!subscriptions.lock().await.contains_key(&session_key));
        assert!(!log_levels.lock().await.contains_key(&session_key));
    }
}
