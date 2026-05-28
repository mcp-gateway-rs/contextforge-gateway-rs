use super::{
    BACKEND_NOTIFICATION_BUFFER_CAPACITY, BackendTransportKey, BackendTransportService, McpService,
    ResourceSubscriptionChange,
    lifecycle::remove_progress_token,
    routing::{
        BackendResourcePair, BackendToolPair, ResourceSubscriptionAction, TEST_IMAGE_DATA, merge_capabilities,
        merge_resources, merge_tools, resource_subscription_target, split_resource_name, split_tool_name,
    },
};
use crate::gateway::{
    backend_notifications::{BackendClientHandler, await_call_tool_response_with_notifications},
    mcp_call_validator::{AuthorizedCallValidator, InitializeCallValidator, SessionKey},
    session_manager::SessionManager,
    session_store::{SessionMapping, UserSession, UserSessionStore},
};
use contextforge_gateway_rs_cpex::ToolPreCallResult;
use rmcp::{
    ErrorData, RoleServer, ServerHandler, ServiceExt,
    model::{
        AnnotateAble, CallToolRequest, CallToolRequestParams, CallToolResult, CancelledNotificationParam,
        ClientRequest, CompleteRequestParams, CompleteResult, CompletionInfo, ErrorCode, GetPromptRequestParams,
        GetPromptResult, Implementation, InitializeRequestParams, InitializeResult, ListPromptsResult,
        ListResourceTemplatesResult, ListResourcesResult, ListToolsResult, Meta, PaginatedRequestParams, Prompt,
        PromptArgument, PromptMessage, PromptMessageContent, PromptMessageRole, RawImageContent, RawResourceTemplate,
        ReadResourceRequestParams, ReadResourceResult, Reference, ServerResult, SetLevelRequest, SetLevelRequestParams,
        SubscribeRequestParams, UnsubscribeRequestParams,
    },
    service::{NotificationContext, PeerRequestOptions, RequestContext, ServiceError},
    transport::{StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig},
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

impl<T> ServerHandler for McpService<T>
where
    T: UserSessionStore + Clone + Send + Sync + 'static,
{
    async fn initialize(
        &self,
        request: InitializeRequestParams,
        cx: RequestContext<RoleServer>,
    ) -> Result<InitializeResult, ErrorData> {
        let call_validator = InitializeCallValidator::new(&cx);
        let call_context = call_validator.validate()?;
        self.cleanup_session_state(&call_context.session_key).await;
        let user_session = UserSession::new(
            call_context.principal.to_owned(),
            call_context.virtual_host_id.to_owned(),
            Arc::clone(&call_context.downstream_session_id.session_id),
        );
        let session_mapping = SessionMapping::new();

        let tasks: Vec<_> = call_context
            .virtual_host
            .backends
            .iter()
            .map(|(name, backend)| {
                let client = self.http_client.clone();
                let request = request.clone();
                let backend_url = backend.url.clone();
                let backend_notifications = broadcast::channel(BACKEND_NOTIFICATION_BUFFER_CAPACITY).0;
                let backend_notification_rx = backend_notifications.subscribe();
                let backend_notifications_for_transport = backend_notifications.clone();

                async move {
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
                    match maybe_running_service {
                        Ok(running_service) => {
                            info!("initialize: initialized backend {name:?}");
                            (name, backend_notifications_for_transport, backend_notification_rx, Some(running_service))
                        },
                        Err(error) => {
                            warn!(?error, "initialize: unable to initialize backend {name:?}");
                            (name, backend_notifications_for_transport, backend_notification_rx, None)
                        },
                    }
                }
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

        self.user_session_store.set_session(&user_session, &session_mapping).await.map_err(|_| ErrorData {
            code: ErrorCode::INTERNAL_ERROR,
            message: "Internal problem... session store can't be accessed".into(),
            data: None,
        })?;
        self.register_session_cleanup(&call_context.session_key).await;

        let mut transport_keys = HashSet::new();
        let mut transports = self.transports.lock().await;
        for (name, svc) in backend_services {
            if svc.service.is_none() {
                continue;
            }
            let key = BackendTransportKey::from((name.as_str(), &call_context.session_key));
            transports.entry(key.clone()).insert_entry(svc);
            transport_keys.insert(key);
        }
        drop(transports);
        if !transport_keys.is_empty() {
            self.transport_session_keys.lock().await.insert(call_context.session_key.clone(), transport_keys);
        }

        self.store_pending_notification_relays(relay_receivers, &call_context.session_key, cx.peer.clone()).await;

        Ok(InitializeResult::new(merge_capabilities(&capabilities))
            .with_server_info(Implementation::new("rust-conformance-server", "0.1.0"))
            .with_instructions("Rust MCP conformance test server"))
    }

    async fn on_initialized(&self, cx: NotificationContext<RoleServer>) {
        if let Some(session_key) = SessionKey::from_authorized_extensions(&cx.extensions) {
            self.start_pending_notification_relays(&session_key).await;
        }
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
        self.activate_session_relays(&call_context.session_key).await;

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

        let responses = futures::future::join_all(list_tools_tasks)
            .await
            .into_iter()
            .filter_map(|(name, response)| {
                debug!("list_tools: backend {name} response_ok={}", response.as_ref().is_some_and(Result::is_ok));
                response.and_then(Result::ok).map(|response| (name, response))
            })
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
        self.activate_session_relays(&call_context.session_key).await;
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

        let downstream_progress_token = cx.meta.get_progress_token();
        self.ensure_notification_relay(&session_manager, &backend_name, &call_context.session_key, cx.peer.clone())
            .await;

        let Some(target_service) = session_manager.borrow_transport(&backend_name).await else {
            return Err(ErrorData {
                code: ErrorCode::INTERNAL_ERROR,
                message: "Routing problem... got no responses from backends".into(),
                data: None,
            });
        };
        debug!(
            "call_tool: Found backend for {} {target_service:?} {backend_name} tool_name = {tool_name}",
            &request_name,
        );
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

        let pre_result = if let Some(plugin_runtime) = &self.plugin_runtime {
            plugin_runtime.before_tool_call(&request, &tool_name, &backend_name).await?
        } else {
            ToolPreCallResult::unchanged()
        };
        let post_state = pre_result.state;
        let mut routed_request = request;
        pre_result.arguments.apply_to_request(&mut routed_request, &tool_name);

        let service_name = target_service.name;

        let mut request_options = PeerRequestOptions::no_options();
        if let Some(downstream_progress_token) = downstream_progress_token.clone() {
            self.track_progress_token(&call_context.session_key, &backend_name, downstream_progress_token.clone())
                .await;
            request_options.meta = Some(Meta::with_progress_token(downstream_progress_token));
        }

        let request_handle = match service
            .send_cancellable_request(
                ClientRequest::CallToolRequest(CallToolRequest::new(routed_request)),
                request_options,
            )
            .await
        {
            Ok(request_handle) => request_handle,
            Err(error) => {
                if let Some(downstream_progress_token) = downstream_progress_token.as_ref() {
                    remove_progress_token(
                        &self.progress_tokens,
                        &call_context.session_key,
                        &backend_name,
                        downstream_progress_token,
                    )
                    .await;
                }
                warn!("call_tool: backend {service_name} {error:?}");
                return Err(ErrorData {
                    code: ErrorCode::INTERNAL_ERROR,
                    message: "Routing problem... got no responses from backends".into(),
                    data: None,
                });
            },
        };
        let cancel_peer = request_handle.peer.clone();
        let cancel_request_id = request_handle.id.clone();
        let response = await_call_tool_response_with_notifications(request_handle);
        tokio::pin!(response);
        let response = tokio::select! {
            response = &mut response => response.map_err(|error| {
                warn!("call_tool: backend {service_name} {error:?}");
                ErrorData {
                    code: ErrorCode::INTERNAL_ERROR,
                    message: "Routing problem... got no responses from backends".into(),
                    data: None,
                }
            }),
            () = cx.ct.cancelled() => {
                let _ = cancel_peer
                    .notify_cancelled(CancelledNotificationParam {
                        request_id: cancel_request_id,
                        reason: Some("downstream request cancelled".to_owned()),
                    })
                    .await;
                Err(ErrorData {
                    code: ErrorCode::INTERNAL_ERROR,
                    message: "Routing problem... downstream request cancelled".into(),
                    data: None,
                })
            },
        };
        if let Some(downstream_progress_token) = downstream_progress_token {
            remove_progress_token(
                &self.progress_tokens,
                &call_context.session_key,
                &backend_name,
                &downstream_progress_token,
            )
            .await;
        }
        let response = response?;

        let response = match (&self.plugin_runtime, post_state) {
            (Some(plugin_runtime), Some(post_state)) => {
                plugin_runtime.after_tool_call(&tool_name, response, Some(post_state)).await
            },
            _ => Ok(response),
        }?;
        info!("call_tool: backend {service_name} completed");
        Ok(response)
    }

    async fn list_resources(
        &self,
        request: Option<PaginatedRequestParams>,
        cx: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        let mcp_call_validator = AuthorizedCallValidator::new("list_resources", &cx);
        let call_context = mcp_call_validator.validate()?;
        self.activate_session_relays(&call_context.session_key).await;

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

        let responses = futures::future::join_all(list_resources_tasks)
            .await
            .into_iter()
            .filter_map(|(name, response)| {
                debug!("list_resources: backend {name} response_ok={}", response.as_ref().is_some_and(Result::is_ok));
                response.and_then(Result::ok).map(|response| (name, response))
            })
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
        self.activate_session_relays(&call_context.session_key).await;
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
        let backend_name = backend_name.to_owned();
        let resource_uri = resource_uri.to_owned();

        let Some(service_holder) = session_manager.borrow_transport(&backend_name).await else {
            return Err(ErrorData {
                code: ErrorCode::INTERNAL_ERROR,
                message: "Routing problem... got no responses from backends".into(),
                data: None,
            });
        };
        debug!(
            "read_resource: Found backend for {} {service_holder:?} {backend_name} read_resource = {resource_uri}",
            &request.uri,
        );
        let Some(service) = service_holder.running_service else {
            warn!(
                "read_resource: trying to read a resource for which we have no backend {service_holder:?} {backend_name} resource_name = {resource_uri}"
            );
            return Err(ErrorData {
                code: ErrorCode::INTERNAL_ERROR,
                message: "Routing problem... got no responses from backends".into(),
                data: None,
            });
        };
        let mut request = request;
        request.uri = resource_uri;
        let response = service.read_resource(request).await;
        debug!("read_resource: backend {backend_name} response_ok={}", response.is_ok());

        response.map_err(|_| ErrorData {
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
        self.activate_session_relays(&call_context.session_key).await;
        let session_manager =
            SessionManager::new(call_context.virtual_host, &call_context.session_key, &self.transports);
        let (backend_name, resource_uri) = resource_subscription_target(&session_manager, &request.uri)?;
        self.apply_resource_subscription_change(ResourceSubscriptionChange {
            session_manager: &session_manager,
            backend_name,
            backend_resource_uri: resource_uri,
            public_resource_uri: request.uri,
            session_key: call_context.session_key.clone(),
            downstream_peer: cx.peer.clone(),
            action: ResourceSubscriptionAction::Subscribe,
            timeout: self.upstream_notification_control_timeout,
        })
        .await
    }

    async fn unsubscribe(
        &self,
        request: UnsubscribeRequestParams,
        cx: RequestContext<RoleServer>,
    ) -> Result<(), ErrorData> {
        let mcp_call_validator = AuthorizedCallValidator::new("unsubscribe", &cx);
        let call_context = mcp_call_validator.validate()?;
        self.activate_session_relays(&call_context.session_key).await;
        let session_manager =
            SessionManager::new(call_context.virtual_host, &call_context.session_key, &self.transports);
        let (backend_name, resource_uri) = resource_subscription_target(&session_manager, &request.uri)?;
        self.apply_resource_subscription_change(ResourceSubscriptionChange {
            session_manager: &session_manager,
            backend_name,
            backend_resource_uri: resource_uri,
            public_resource_uri: request.uri,
            session_key: call_context.session_key.clone(),
            downstream_peer: cx.peer.clone(),
            action: ResourceSubscriptionAction::Unsubscribe,
            timeout: self.upstream_notification_control_timeout,
        })
        .await
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
        self.activate_session_relays(&call_context.session_key).await;
        let session_manager =
            SessionManager::new(call_context.virtual_host, &call_context.session_key, &self.transports);
        let forwards = session_manager.borrow_transports().await.into_iter().filter_map(|service_holder| {
            service_holder.running_service.map(|service| {
                let request = request.clone();
                async move {
                    let mut options = PeerRequestOptions::no_options();
                    options.timeout = Some(self.upstream_notification_control_timeout);
                    let response = match service
                        .send_cancellable_request(
                            ClientRequest::SetLevelRequest(SetLevelRequest::new(request)),
                            options,
                        )
                        .await
                    {
                        Ok(handle) => handle.await_response().await,
                        Err(error) => Err(error),
                    };
                    match response {
                        Ok(ServerResult::EmptyResult(_)) => Some(service_holder.name),
                        Ok(other) => {
                            debug!(
                                backend = service_holder.name,
                                ?other,
                                "backend logging set_level forwarding returned unexpected response"
                            );
                            None
                        },
                        Err(ServiceError::Timeout { .. }) => {
                            debug!(backend = service_holder.name, "backend logging set_level forwarding timed out");
                            None
                        },
                        Err(error) => {
                            debug!(
                                backend = service_holder.name,
                                ?error,
                                "backend logging set_level forwarding failed"
                            );
                            None
                        },
                    }
                }
            })
        });
        let backend_levels = futures::future::join_all(forwards)
            .await
            .into_iter()
            .flatten()
            .map(|backend_name| (backend_name, request.level))
            .collect::<HashMap<_, _>>();
        if backend_levels.is_empty() {
            self.log_levels.write().await.remove(&call_context.session_key);
        } else {
            self.log_levels.write().await.insert(call_context.session_key.clone(), backend_levels);
        }
        Ok(())
    }
}
