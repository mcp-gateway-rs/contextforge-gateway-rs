# AGENTS.md

Architecture notes for agents working on `contextforge-gateway-rs`.

This repo is the Rust dataplane part of ContextForge. It must stay compatible with the external ContextForge control plane in `https://github.com/IBM/mcp-context-forge`, but it must not become a control-plane, UI, IAM, or metrics-storage app.

## Mental Model

The gateway accepts downstream MCP streamable HTTP traffic, authenticates the caller, loads that user's virtual-host config, opens or reuses MCP client sessions to configured backend MCP servers, then presents those backends as one merged MCP server.

```text
MCP client
  -> /contextforge-rs/servers/{virtual_host_id}/mcp
  -> JWT auth
  -> user config lookup
  -> virtual host backend list
  -> per-backend MCP client sessions
  -> merged tools/resources/prompts back to client
```

Existing ContextForge control-plane and management concerns live outside this repo. This repo owns the fast dataplane path only.

## Dataplane Direction

Current direction:

- Control plane stays external. It owns management APIs, UI, user/admin workflows, configuration, credentials, policies, and durable storage.
- Dataplane owns the hot request/response path. It consumes runtime config, routes traffic, applies policy/guardrails/plugins, calls upstreams, and emits telemetry.
- Runtime config arrives from the control plane through Redis now, possibly xDS/gRPC later.
- `/servers/{virtual_host_id}/mcp` should behave like the legacy ContextForge MCP endpoint from a client point of view.
- Nginx/front-door routing may send only `/servers/{uuid}/mcp` traffic here while all other ContextForge traffic stays on existing CF paths.
- Platform scope starts with MCP and should keep auth, policy, telemetry, and config ingestion reusable for A2A and LLM/model gateway traffic.
- Plugins may need request/response payload access, not just headers. Performance-sensitive paths should keep CPU, memory, locking, and task boundaries explicit.
- This project is still early development with no external users; prefer the right architecture over preserving unstable APIs or compatibility surfaces.

## Workspace Layers

```text
crates/contextforge-gateway-rs
  process shell: config parsing, logging, runtime, dependency assembly

crates/contextforge-gateway-rs-lib
  dataplane: listeners, Axum middleware, auth, config lookup, MCP fanout/routing, sessions

crates/contextforge-gateway-rs-apis
  shared contract: user config model and schema generation

crates/contextforge-load-test
  performance harness: end-to-end MCP traffic driver
```

Most product behavior belongs in `contextforge-gateway-rs-lib`; avoid adding dataplane logic to the binary crate.

## Target Pipeline

The gateway is a bidirectional AI traffic pipeline:

```text
downstream request
  -> authentication / authorization
  -> rate limiting
  -> routing / protocol selection
  -> request payload/header modification
  -> optional retrieval / augmentation
  -> request guardrails
  -> upstream MCP / A2A / model provider

upstream response
  -> response guardrails
  -> response payload/header modification
  -> metrics / tracing / logging
  -> downstream response
```

Current code implements the MCP subset. Preserve ordering: auth and config before backend selection; request plugins/mutation before upstream calls; response plugins/mutation before returning; telemetry around both sides.

The intended runtime shape is a hot request path plus support loops:

- listener and Hyper/Axum stack on Tokio executors
- JWT/auth middleware
- session extraction
- user/config retrieval
- MCP session management around gateway logic
- request and response plugin hooks
- upstream client boundary
- separate configuration and metrics collection work

Keep allocation, locking, cross-task communication, and shared mutable state intentional.

## Request Flow

Gateway route:

```text
/contextforge-rs/servers/{virtual_host_id}/mcp
```

Startup assembly:

1. `contextforge-gateway-rs/src/main.rs` parses `Config`.
2. It builds `RedisUserConfigStore`.
3. It builds `Gateway` with config, user config store, and RMCP local session manager.
4. `Gateway::run_gateway` creates an RMCP `StreamableHttpService`.
5. Axum middleware wraps the service and TCP/TLS listeners expose it.

Per request, middleware populates extensions:

1. `claims_layer` validates JWT and stores `ContextForgeGatewayClaims`.
2. `user_config_store_layer` uses JWT subject as Redis key and stores `UserConfig`.
3. `SessionIdLayer` reads `Mcp-session-id` into `SessionId`.
4. `virtual_host_id_layer` extracts `{virtual_host_id}` from the path.

`McpService` reads those extensions through `InitializeCallValidator` or `AuthorizedCallValidator`.

## Runtime Config Model

User config is the routing source of truth:

```text
UserConfig
  virtual_hosts: HashMap<virtual_host_id, VirtualHost>

VirtualHost
  backends: HashMap<backend_name, BackendMCPGateway>

BackendMCPGateway
  url: Url
```

Current persistence:

- User key is `User::new(jwt_subject)`.
- Keys and values are MessagePack-encoded.
- `RedisUserConfigStore` keeps an in-process LRU cache in front of Redis.

Expected config growth:

- route selection across multiple MCP endpoints
- principal/virtual-host filters for tools, resources, and prompts
- backend auth/TLS material references
- request/response header pass/add/remove rules
- plugin/CPEX hook settings
- pagination/SSE behavior where protocol handling needs config
- future A2A and LLM routing/provider settings

Keep persistent config access behind `UserConfigStore`. Do not push Redis details into routing code.

## Backend Sessions

Initialization fans out:

1. Client calls `initialize`.
2. `McpService::initialize` validates virtual host and downstream session id.
3. It creates one RMCP client transport per backend URL.
4. It forwards the initialize request to each backend.
5. It stores backend services under `(backend_name, downstream_session_id)`.
6. It returns merged gateway capabilities downstream.

Later calls reuse backend services from the shared transport map. `SessionManager::borrow_transports` temporarily removes services from the map; callers must return them with `return_transports` or deliberately remove them with `cleanup_backends`.

Load-balanced deployments are unresolved. Known options are sticky routing by `Mcp-session-id`, remote session mapping through Redis/external cache, or active-hot-standby where clients reinitialize after failover. Do not assume any node can serve any stateful MCP session unless session state has moved out of process.

## MCP Routing Semantics

Backends are namespaced by prefix.

- Backend tool `increment` from backend `gateway-one` becomes `gateway-one-increment`.
- `call_tool` splits `{backend_name}-{tool_name}` and routes only to that backend.
- Resources follow the same prefix/split model.
- Listed tools/resources are merged and sorted before returning.

Do not change this naming contract without updating merge logic, split logic, and tests.

Tracked MCP gaps:

- `list_tools`, `list_resources`, and `list_prompts` pagination should gather all backend pages before returning merged output.
- SSE responses should stream downstream as backend chunks arrive.
- Stateless MCP calls are a target for parity and future load-balancing.
- Filtering must use principal plus runtime config, not hard-coded backend behavior.

## Transports

Keep three transport concerns separate:

- Downstream listener transport: how clients reach the gateway; implemented in `transports/tcp.rs` and `transports/tls.rs`.
- Upstream backend transport: how the gateway reaches backend MCP servers; implemented through `reqwest::Client` plus RMCP `StreamableHttpClientTransport`.
- Config-store transport: how runtime config is loaded; currently Redis.

Transport security is moving from static process config toward runtime config:

- downstream TLS remains listener-level gateway config
- upstream TLS/mTLS should be selectable per backend
- backend authorization headers may come from runtime config
- PEM certificate material may be stored or referenced through Redis-managed config

## Plugins And Policy

Plugins may inspect or mutate request/response bodies, so they affect architecture more than simple header filters.

Likely hook points:

- after auth/config lookup, before backend selection
- before forwarding upstream
- after receiving backend response
- before returning merged/listed results downstream

When adding plugins, define behavior for streaming/SSE, failures, timeouts, backpressure, resource ownership, and OpenTelemetry attribution. Plugin execution must not block unrelated sessions or poison shared gateway state.

## Module Boundaries

- Keep config validation in `common.rs`.
- Keep request extension extraction in `layers/`.
- Keep MCP fanout/routing in `gateway/`.
- Keep downstream listener logic in `transports/`.
- Keep shared config shapes in `contextforge-gateway-rs-apis`.
- Keep protocol-neutral concerns separate from MCP-specific logic so A2A/LLM support can reuse auth, TLS, config, telemetry, plugin execution, and session strategy.

## Invariants

- JWT subject selects the user config.
- Path virtual host id selects one `VirtualHost` inside that user's config.
- Backend name is part of the public tool/resource namespace.
- Backend service ownership is temporarily moved out of the shared map during calls.
- Redis encoding is MessagePack.
- End-to-end behavior is defined by merged MCP semantics, not by leaking backend identity directly.
- This dataplane consumes control-plane config and enforces it; it does not own UI, OpenAPI management APIs, durable observability storage, or customer IAM.
