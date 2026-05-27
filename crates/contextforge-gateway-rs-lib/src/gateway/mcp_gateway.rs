use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use contextforge_gateway_rs_apis::user_store::UserConfig;
use contextforge_gateway_rs_cpex::{GatewayPluginRuntimeHandle, ToolPreCallResult};
use http::request::Parts;
use itertools::Itertools;
use rmcp::{
    ErrorData, RoleClient, RoleServer, ServerHandler, ServiceExt,
    model::{
        AnnotateAble, CallToolRequestParams, CallToolResult, CompleteRequestParams, CompleteResult, CompletionInfo,
        ErrorCode, GetPromptRequestParams, GetPromptResult, Implementation, InitializeRequestParams, InitializeResult,
        ListPromptsResult, ListResourceTemplatesResult, ListResourcesResult, ListToolsResult, LoggingLevel,
        PaginatedRequestParams, Prompt, RawResourceTemplate, ReadResourceRequestParams, ReadResourceResult, Reference,
        Resource, ServerCapabilities, SetLevelRequestParams, SubscribeRequestParams, Tool, UnsubscribeRequestParams,
    },
    service::{RequestContext, RunningService},
    transport::{StreamableHttpClientTransport, streamable_http_client::StreamableHttpClientTransportConfig},
};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};
use typed_builder::TypedBuilder;

use super::mcp_call_validator::AuthorizedCallValidator;
pub use crate::gateway::session_store::LocalUserSessionStore;
use crate::{
    SessionId,
    gateway::{
        mcp_call_validator::InitializeCallValidator,
        session_manager::SessionManager,
        session_store::{UserSession, UserSessionStore},
    },
};

#[derive(Clone, TypedBuilder)]
#[builder(field_defaults(setter(prefix = "with_")))]
pub struct McpService<T>
where
    T: UserSessionStore,
{
    #[builder(default = Arc::new(Mutex::new(HashSet::new())))]
    subscriptions: Arc<Mutex<HashSet<String>>>,
    #[builder(default = Arc::new(Mutex::new(HashMap::new())))]
    transports: Arc<Mutex<HashMap<BackendTransportKey, BackendTransportService>>>,
    #[builder(default = Arc::new(Mutex::new(LoggingLevel::Debug)))]
    log_level: Arc<Mutex<LoggingLevel>>,
    http_client: reqwest::Client,
    user_session_store: T,
    #[builder(default)]
    plugin_runtime: Option<GatewayPluginRuntimeHandle>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BackendTransportKey {
    backend_name: String,
    session_id: String,
}

type McpClientService = Arc<RunningService<RoleClient, InitializeRequestParams>>;

#[derive(Debug)]
pub struct ServiceHolder {
    pub name: String,
    pub running_service: Option<McpClientService>,
}

impl ServiceHolder {
    pub fn new(name: String, running_service: Option<McpClientService>) -> ServiceHolder {
        Self { name, running_service }
    }
}

#[derive(Debug)]
pub struct BackendTransportService {
    #[expect(dead_code, reason = "stored backend capabilities are kept with transport state for future routing")]
    capabilities: Option<ServerCapabilities>,
    pub(crate) service: Option<McpClientService>,
}

impl From<(&str, &str)> for BackendTransportKey {
    fn from((backend_name, session_name): (&str, &str)) -> Self {
        Self { backend_name: backend_name.to_owned(), session_id: session_name.to_owned() }
    }
}

impl From<(&String, &SessionId)> for BackendTransportKey {
    fn from((backend_name, session_name): (&String, &SessionId)) -> Self {
        Self { backend_name: backend_name.to_owned(), session_id: session_name.value().to_owned() }
    }
}

impl From<(Option<ServerCapabilities>, Option<McpClientService>)> for BackendTransportService {
    fn from((capabilities, service): (Option<ServerCapabilities>, Option<McpClientService>)) -> Self {
        Self { capabilities, service }
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
        let (virtual_host, downstream_session_id) = call_validator.validate()?;
        let session_mapping = if let Ok(maybe_session_mapping) = self
            .user_session_store
            .get_session(&UserSession::new(String::new(), Arc::clone(&downstream_session_id.session_id)))
            .await
        {
            maybe_session_mapping.unwrap_or_default()
        } else {
            return Err(ErrorData {
                code: ErrorCode::INTERNAL_ERROR,
                message: "Internal problem... session store can't be accessed".into(),
                data: None,
            });
        };

        let tasks: Vec<_> = virtual_host
            .backends
            .iter()
            .map(|(name, backend)| {
                let client = self.http_client.clone();
                let request = request.clone();
                let backend_url = backend.url.clone();
                let downstream_session_id = downstream_session_id.clone();

                    Box::pin(async move {
                        let mut headers = HashMap::new();
                        if let Some(host) = backend_url.host_str() && backend_url.scheme() == "https"{
                            let host = if let Some(port) = backend_url.port(){
                                format!("{host}:{port}")
                            }else{
                                host.to_owned()
                            };

                            if let Ok(value) = http::HeaderValue::from_str(&host){
                                headers.insert(http::header::HOST, value);
                            }else{
                                warn!("Really can't set the host header for {:?}",backend_url.host_str());
                            }
                        }

                        let config = StreamableHttpClientTransportConfig::with_uri(backend_url.to_string())
                            .custom_headers(headers);
                        let transport = StreamableHttpClientTransport::with_client(client, config);
                        let maybe_running_service = request.serve(transport).await;
                        if let Ok(running_service) = maybe_running_service {
                            info!("initialize: intialized for {downstream_session_id:?} {name:?}");
                            (name, Some(running_service))
                        } else {
                            warn!("initialize: Unable to initialize for {downstream_session_id:?} {name:?} {maybe_running_service:?}",);
                            (name, None)
                        }
                    })
            }).collect();

        let initialization_results: Vec<(&String, Option<RunningService<RoleClient, InitializeRequestParams>>)> =
            futures::future::join_all(tasks).await;

        let (capabilities, backend_services): (Vec<_>, Vec<_>) = initialization_results
            .into_iter()
            .map(|(name, running_service):(_,_)| {
                info!("initialize: Adding transport: session_id {downstream_session_id:#?} backend {name} {running_service:?}");

                let server_capabilities =
                    running_service.as_ref()
                        .and_then(|rs|
                            rs.peer()
                                .peer_info()
                                .as_ref()
                                .map(|pi| pi.capabilities.clone()));
                (
                    (name.clone(), server_capabilities.clone()),
                    (name.clone(), BackendTransportService::from((server_capabilities, running_service.map(Arc::new)))),
                )
            })
            .unzip();

        let _ = self
            .user_session_store
            .set_session(
                &UserSession::new(String::new(), Arc::clone(&downstream_session_id.session_id)),
                &session_mapping,
            )
            .await;

        let mut transports = self.transports.lock().await;
        for (name, svc) in backend_services {
            transports
                .entry(BackendTransportKey::from((name.as_str(), downstream_session_id.value())))
                .insert_entry(svc);
        }
        drop(transports);

        Ok(InitializeResult::new(merge_capabilities(capabilities))
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
        let (virtual_host, session_id) = mcp_call_validator.validate()?;

        let session_manager = SessionManager::new(virtual_host, session_id, &self.transports);
        let backend_transports: Vec<_> = session_manager.borrow_transports().await;

        let list_tools_tasks = backend_transports
            .into_iter()
            .map(|service_holder| {
                let request = request.clone();
                async move {
                    if let Some(service) = service_holder.running_service {
                        //let service = service.read().await;
                        let response = service.list_tools(request).await;
                        (service_holder.name, Some(response))
                    } else {
                        (service_holder.name, None)
                    }
                }
            })
            .collect::<Vec<_>>();

        let list_tools_tasks_results: Vec<(String, Option<_>)> = futures::future::join_all(list_tools_tasks).await;

        let responses: Vec<_> = list_tools_tasks_results
            .into_iter()
            .map(|(name, response)| {
                info!("list_tools: backend {name} {response:?}");
                (name, response)
            })
            .collect();

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
        let (virtual_host, session_id) = mcp_call_validator.validate()?;
        let session_manager = SessionManager::new(virtual_host, session_id, &self.transports);

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

        let backend_transports = session_manager.borrow_transports().await;
        info!("Borrowed transports {session_id:?} {backend_transports:?}");
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
                    return Err(ErrorData {
                        code: ErrorCode::INVALID_REQUEST,
                        message: "Routing problem... multiple matching tools".into(),
                        data: None,
                    });
                }
                target_service = Some(service_holder);
            }
        }

        let Some(mut target_service) = target_service else {
            return Err(ErrorData {
                code: ErrorCode::INTERNAL_ERROR,
                message: "Routing problem... got no responses from backends".into(),
                data: None,
            });
        };
        let Some(service) = target_service.running_service.take() else {
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

        let service_name = target_service.name.clone();
        let response = service.call_tool(routed_request).await;
        let response = response.map_err(|error| {
            warn!("call_tool: backend {service_name} {error:?}");
            ErrorData {
                code: ErrorCode::INTERNAL_ERROR,
                message: "Routing problem... got no responses from backends".into(),
                data: None,
            }
        })?;
        let response = match (&self.plugin_runtime, post_state) {
            (Some(plugin_runtime), Some(post_state)) => {
                plugin_runtime.after_tool_call(&tool_name, response, Some(post_state)).await?
            },
            _ => response,
        };
        info!("call_tool: backend {service_name} completed");
        Ok(response)
    }

    async fn list_resources(
        &self,
        request: Option<PaginatedRequestParams>,
        cx: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        let mcp_call_validator = AuthorizedCallValidator::new("list_resources", &cx);
        let (virtual_host, session_id) = mcp_call_validator.validate()?;

        let session_manager = SessionManager::new(virtual_host, session_id, &self.transports);
        let backend_transports: Vec<_> = session_manager.borrow_transports().await;

        let list_resources_tasks = backend_transports
            .into_iter()
            .map(|service_holder| {
                let request = request.clone();
                async move {
                    if let Some(service) = service_holder.running_service {
                        //let service = service.read().await;
                        let response = service.list_resources(request).await;
                        (service_holder.name, Some(response))
                    } else {
                        (service_holder.name, None)
                    }
                }
            })
            .collect::<Vec<_>>();

        let list_tools_tasks_results: Vec<(String, Option<_>)> = futures::future::join_all(list_resources_tasks).await;

        let responses: Vec<_> = list_tools_tasks_results
            .into_iter()
            .map(|(name, response)| {
                info!("list_resources: backend {name} {response:?}");
                (name, response)
            })
            .collect();

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
        let (virtual_host, session_id) = mcp_call_validator.validate()?;
        let session_manager = SessionManager::new(virtual_host, session_id, &self.transports);

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
        info!("Borrowed transports {session_id:?} {backend_transports:?}");

        let call_tool_tasks: Vec<_> = backend_transports
            .into_iter()
            .map(|service_holder| {
                debug!(
                    "read_resource: Finding backend for {} {service_holder:?} {backend_name} read_resource = {resource_uri}",
                    &request.uri,

                );
                (service_holder.name == backend_name).then(|| {
                    let mut request = request.clone();
                    request.uri = String::from(resource_uri);
                        async move {
                            if let Some(service) = service_holder.running_service {
                                //let service = service.read().await;
                                let response = service.read_resource(request).await;

                                (service_holder.name, Some(response))
                            } else {
                                warn!("call_tool: trying to call a tool for which we have no backend {service_holder:?} {backend_name} resource_name = {resource_uri}");
                                (service_holder.name, None)
                            }
                        }
                })
            }).collect();

        let call_tool_tasks = call_tool_tasks.into_iter().flatten().collect::<Vec<_>>();
        if call_tool_tasks.len() > 1 {
            warn!("read_resource: More than one tool matching for tool name {}", request.uri);

            session_manager.cleanup_backends("read_resource: invalid session.. duplicate resources detected").await;

            return Err(ErrorData {
                code: ErrorCode::INVALID_REQUEST,
                message: "Routing problem... multiple matching resources".into(),
                data: None,
            });
        }

        let call_tool_tasks_results: Vec<_> = futures::future::join_all(call_tool_tasks).await;

        let responses: Vec<_> = call_tool_tasks_results
            .into_iter()
            .map(|(name, response)| {
                info!("read_resource: backend {name} {response:?}");
                (name, response)
            })
            .collect();

        let responses = responses
            .into_iter()
            .filter_map(
                |(name, response)| if let Some(Ok(response)) = response { Some((name, response)) } else { None },
            )
            .collect::<Vec<_>>();

        responses.first().cloned().map(|(_, r)| r).ok_or(ErrorData {
            code: ErrorCode::INTERNAL_ERROR,
            message: "Routing problem... got no responses from backends".into(),
            data: None,
        })
    }

    async fn list_resource_templates(
        &self,
        _request: Option<PaginatedRequestParams>,
        cx: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, ErrorData> {
        let maybe_parts = cx.extensions.get::<Parts>();
        let maybe_session = maybe_parts.and_then(|parts| parts.extensions.get::<SessionId>());
        let maybe_user_config = maybe_parts.and_then(|parts| parts.extensions.get::<UserConfig>());
        info!("list_resource_templates user_config = {maybe_user_config:#?} session_id = {maybe_session:#?}");
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
        let maybe_parts = cx.extensions.get::<Parts>();
        let maybe_session = maybe_parts.and_then(|parts| parts.extensions.get::<SessionId>());
        let maybe_user_config = maybe_parts.and_then(|parts| parts.extensions.get::<UserConfig>());
        info!("subscribe user_config = {maybe_user_config:#?} session_id = {maybe_session:#?}");

        let mut subs = self.subscriptions.lock().await;
        subs.insert(request.uri.clone());
        Ok(())
    }

    async fn unsubscribe(
        &self,
        request: UnsubscribeRequestParams,
        cx: RequestContext<RoleServer>,
    ) -> Result<(), ErrorData> {
        let maybe_parts = cx.extensions.get::<Parts>();
        let maybe_session = maybe_parts.and_then(|parts| parts.extensions.get::<SessionId>());
        let maybe_user_config = maybe_parts.and_then(|parts| parts.extensions.get::<UserConfig>());
        info!("unsubscribe user_config = {maybe_user_config:#?} session_id = {maybe_session:#?}");

        let mut subs = self.subscriptions.lock().await;
        subs.remove(request.uri.as_str());
        Ok(())
    }

    async fn list_prompts(
        &self,
        request: Option<PaginatedRequestParams>,
        cx: RequestContext<RoleServer>,
    ) -> Result<ListPromptsResult, ErrorData> {
        let mcp_call_validator = AuthorizedCallValidator::new("list_prompts", &cx);
        let (virtual_host, session_id) = mcp_call_validator.validate()?;

        let session_manager = SessionManager::new(virtual_host, session_id, &self.transports);
        let backend_transports: Vec<_> = session_manager.borrow_transports().await;

        let list_prompts_tasks = backend_transports
            .into_iter()
            .map(|service_holder| {
                let request = request.clone();
                async move {
                    if let Some(service) = service_holder.running_service {
                        let response = service.list_prompts(request).await;
                        (service_holder.name, Some(response))
                    } else {
                        (service_holder.name, None)
                    }
                }
            })
            .collect::<Vec<_>>();

        let list_prompts_tasks_results: Vec<(String, Option<_>)> =
            futures::future::join_all(list_prompts_tasks).await;

        let responses = list_prompts_tasks_results
            .into_iter()
            .map(|(name, response)| {
                info!("list_prompts: backend {name} {response:?}");
                (name, response)
            })
            .filter_map(
                |(name, response)| if let Some(Ok(response)) = response { Some((name, response)) } else { None },
            )
            .collect::<Vec<_>>();

        Ok(ListPromptsResult { meta: None, prompts: merge_prompts(responses), next_cursor: None })
    }

    async fn get_prompt(
        &self,
        request: GetPromptRequestParams,
        cx: RequestContext<RoleServer>,
    ) -> Result<GetPromptResult, ErrorData> {
        let mcp_call_validator = AuthorizedCallValidator::new("get_prompt", &cx);
        let (virtual_host, session_id) = mcp_call_validator.validate()?;
        let session_manager = SessionManager::new(virtual_host, session_id, &self.transports);

        let backend_names = session_manager.get_backend_names();

        let Some(BackendPromptPair { backend_name, prompt_name }) =
            split_prompt_name(&request.name, &backend_names)
        else {
            return Err(ErrorData {
                code: ErrorCode::INTERNAL_ERROR,
                message: "Routing problem... wrong prompt name".into(),
                data: None,
            });
        };

        let backend_transports = session_manager.borrow_transports().await;
        info!("Borrowed transports {session_id:?} {backend_transports:?}");

        let get_prompt_tasks: Vec<_> = backend_transports
            .into_iter()
            .map(|service_holder| {
                debug!(
                    "get_prompt: Finding backend for {} {service_holder:?} {backend_name} prompt = {prompt_name}",
                    &request.name,
                );
                (service_holder.name == backend_name).then(|| {
                    let mut request = request.clone();
                    request.name = prompt_name.to_owned();
                    async move {
                        if let Some(service) = service_holder.running_service {
                            let response = service.get_prompt(request).await;
                            (service_holder.name, Some(response))
                        } else {
                            warn!(
                                "get_prompt: no backend for {service_holder:?} {backend_name} prompt = {prompt_name}"
                            );
                            (service_holder.name, None)
                        }
                    }
                })
            })
            .collect();

        let get_prompt_tasks = get_prompt_tasks.into_iter().flatten().collect::<Vec<_>>();
        if get_prompt_tasks.len() > 1 {
            warn!("get_prompt: More than one prompt matching for prompt name {}", request.name);
            session_manager.cleanup_backends("get_prompt: invalid session.. duplicate prompts detected").await;
            return Err(ErrorData {
                code: ErrorCode::INVALID_REQUEST,
                message: "Routing problem... multiple matching prompts".into(),
                data: None,
            });
        }

        let get_prompt_tasks_results: Vec<_> = futures::future::join_all(get_prompt_tasks).await;

        let responses = get_prompt_tasks_results
            .into_iter()
            .map(|(name, response)| {
                info!("get_prompt: backend {name} {response:?}");
                (name, response)
            })
            .filter_map(
                |(name, response)| if let Some(Ok(response)) = response { Some((name, response)) } else { None },
            )
            .collect::<Vec<_>>();

        responses.first().cloned().map(|(_, r)| r).ok_or(ErrorData {
            code: ErrorCode::INTERNAL_ERROR,
            message: "Routing problem... got no responses from backends".into(),
            data: None,
        })
    }

    async fn complete(
        &self,
        request: CompleteRequestParams,
        cx: RequestContext<RoleServer>,
    ) -> Result<CompleteResult, ErrorData> {
        let maybe_parts = cx.extensions.get::<Parts>();
        let maybe_session = maybe_parts.and_then(|parts| parts.extensions.get::<SessionId>());
        let maybe_user_config = maybe_parts.and_then(|parts| parts.extensions.get::<UserConfig>());
        info!("complete user_config = {maybe_user_config:#?} session_id = {maybe_session:#?}");
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
        let maybe_parts = cx.extensions.get::<Parts>();
        let maybe_session = maybe_parts.and_then(|parts| parts.extensions.get::<SessionId>());
        let maybe_user_config = maybe_parts.and_then(|parts| parts.extensions.get::<UserConfig>());
        info!("set_level user_config = {maybe_user_config:#?} session_id = {maybe_session:#?}");
        let mut level = self.log_level.lock().await;
        *level = request.level;
        Ok(())
    }
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

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq)]
struct BackendPromptPair<'a> {
    backend_name: &'a str,
    prompt_name: &'a str,
}

fn split_tool_name<'a, T: AsRef<str>, N: AsRef<str>>(
    tool_name: &'a T,
    backend_names: &'a [N],
) -> Option<BackendToolPair<'a>> {
    for name in backend_names {
        let tool_name = tool_name.as_ref();
        let name = name.as_ref();
        let extended_name = name.to_owned() + "-";
        if tool_name.starts_with(&extended_name) {
            return Some(BackendToolPair { backend_name: name, tool_name: &tool_name[extended_name.len()..] });
        }
    }
    None
}

fn merge_capabilities(_server_capabilities: Vec<(String, Option<ServerCapabilities>)>) -> ServerCapabilities {
    ServerCapabilities::builder().enable_prompts().enable_resources().enable_tools().enable_logging().build()
}

fn merge_tools(tools: Vec<(String, ListToolsResult)>) -> Vec<Tool> {
    tools
        .into_iter()
        .flat_map(|(backend_name, result)| {
            result
                .tools
                .into_iter()
                .map(|mut t| {
                    t.name = format!("{backend_name}-{}", t.name).into();
                    t
                })
                .collect::<Vec<_>>()
        })
        .sorted_by(|t, o| t.name.cmp(&o.name))
        .collect::<Vec<_>>()
}

fn merge_resources(resources: Vec<(String, ListResourcesResult)>) -> Vec<Resource> {
    resources
        .into_iter()
        .flat_map(|(backend_name, result)| {
            result
                .resources
                .into_iter()
                .map(|mut t| {
                    t.name = format!("{backend_name}-{}", t.name);
                    t.uri = format!("{backend_name}-{}", t.uri);
                    t
                })
                .collect::<Vec<_>>()
        })
        .sorted_by(|t, o| t.name.cmp(&o.name))
        .collect::<Vec<_>>()
}

fn split_resource_name<'a, T: AsRef<str>, N: AsRef<str>>(
    resource_uri: &'a T,
    backend_names: &'a [N],
) -> Option<BackendResourcePair<'a>> {
    for name in backend_names {
        let resource_uri = resource_uri.as_ref();
        let name = name.as_ref();
        let extended_name = name.to_owned() + "-";
        if resource_uri.starts_with(&extended_name) {
            return Some(BackendResourcePair {
                backend_name: name,
                resource_uri: &resource_uri[extended_name.len()..],
            });
        }
    }
    None
}

fn split_prompt_name<'a, T: AsRef<str>, N: AsRef<str>>(
    prompt_name: &'a T,
    backend_names: &'a [N],
) -> Option<BackendPromptPair<'a>> {
    for name in backend_names {
        let prompt_name = prompt_name.as_ref();
        let name = name.as_ref();
        let extended_name = name.to_owned() + "-";
        if prompt_name.starts_with(&extended_name) {
            return Some(BackendPromptPair {
                backend_name: name,
                prompt_name: &prompt_name[extended_name.len()..],
            });
        }
    }
    None
}

fn merge_prompts(prompts: Vec<(String, ListPromptsResult)>) -> Vec<Prompt> {
    prompts
        .into_iter()
        .flat_map(|(backend_name, result)| {
            result
                .prompts
                .into_iter()
                .map(|mut p| {
                    p.name = format!("{backend_name}-{}", p.name);
                    p
                })
                .collect::<Vec<_>>()
        })
        .sorted_by(|a, b| a.name.cmp(&b.name))
        .collect::<Vec<_>>()
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_splitting() {
        let tool_name = "counter-one-increment";
        let backend_names = vec!["counter-on", "counter-oneee", "counter-one"];
        let pair = BackendToolPair { backend_name: "counter-one", tool_name: "increment" };
        assert_eq!(Some(pair), split_tool_name(&tool_name, &backend_names));
        let tool_name = "counter-oneincrement";
        assert_eq!(None, split_tool_name(&tool_name, &backend_names));
        let tool_name = "counteroneincrement";
        assert_eq!(None, split_tool_name(&tool_name, &backend_names));
        let tool_name = "counter-one-get-value";
        let pair = BackendToolPair { backend_name: "counter-one", tool_name: "get-value" };
        assert_eq!(Some(pair), split_tool_name(&tool_name, &backend_names));

        let backend_names = vec!["counter_on", "counter_oneee", "counter_one"];
        let tool_name = "counter_one-get-value";
        let pair = BackendToolPair { backend_name: "counter_one", tool_name: "get-value" };
        assert_eq!(Some(pair), split_tool_name(&tool_name, &backend_names));
    }
}
