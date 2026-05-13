# ContextForge Dataplane


## Running
1. Start Redis and gateways
```
docker compose -f docker/docker-compose-local.yaml up -d
```

2. Run gateway
```bash 
    cargo run --bin contextforge-gateway-rs -- --address 0.0.0.0:8001 --redis-port 6379 --redis-address 127.0.0.1 --token-verification-public-key assets/jwt.key.pub  --token-verification-private-key assets/jwt.key --number-of-cpus 16 --redis-mode=plain-text --upstream-connection-mode=plain-text-or-tls
```

This should spin up Redis instance and two mcp-gateways: a simple counter and a conformance test server from mcp-rust-sdk

3. Get a test JWT token
```bash
curl --request GET \
  --url http://127.0.0.1:8001/contextforge-rs/admin/tokens/admin@example.com \
  --header 'accept: application/json' \  
  --header 'content-type: application/json'
```

4. Use the token to add a test user to Redis
```bash
curl --request POST \
  --url http://127.0.0.1:8001/contextforge-rs/admin/userconfigs/admin@example.com \
  --header 'authorization: Bearer {{token}}' \
  --header 'content-type: application/json' \
  --data '{
  "virtualHosts": {
      "c0ffee00f001f00lf00ldeadbeefdead": {
        "backends": {
          "gateway-one": {
            "url": "http://127.0.0.1:5555/mcp"
          },
          "gateway-two": {
            "url": "http://127.0.0.1:5556/mcp"
          }        
        }
      }
    }
}'
```

6. Spin up MCP Inspector to test the calls


## Runtime CPEX Plugins

Runtime CPEX plugins are disabled by default. Enable hook execution when starting the gateway:

```bash
cargo run --release --bin contextforge-gateway-rs -- \
  --address 0.0.0.0:8001 \
  --redis-port 6379 \
  --redis-address 127.0.0.1 \
  --token-verification-public-key assets/jwt.key.pub \
  --token-verification-private-key assets/jwt.key \
  --number-of-cpus 16 \
  --runtime-plugins-enabled true
```

Plugin configuration is stored in Redis at key `ContextForgeGatewayRuntimePluginConfig`. The value can be JSON or MessagePack with `version: 1` and `cpex` containing the CPEX config. The gateway reloads that key while running, builds a new initialized CPEX runtime, and swaps the runtime registry to the new immutable `PluginManager`. The existing `PluginManager` is not mutated after initialization.

This integration currently passes only tool payloads. CPEX configs that enable route-based plugin selection or depend on user, tenant, server, agent, tag, tool metadata, route overrides, or other extension scopes are rejected in this PR. Redis write access to this key is a control-plane trust boundary because it controls which registered hooks run.

### Payload Marker Demo

This demo uses [`cpex-payload-marker`](https://github.com/contextforge-gateway-rs/cpex-plugins-rs/tree/main/crates/cpex-payload-marker). The plugin must be included in the gateway build before the gateway starts. Redis runtime registration activates already-registered factories; it does not load new Rust code into a running process.

Build the gateway with the demo plugin factories:

```bash
CARGO_NET_GIT_FETCH_WITH_CLI=true cargo check -p contextforge-gateway-rs --features test-plugins
```

The `test-plugins` feature includes those demo plugin crates and registers their factories through the gateway's generic CMF factory adapter.

Start Redis and the sample MCP backends:

```bash
GATEWAY_CPU_LIMIT=1 \
GATEWAY_CPU_RESERVATION=0.5 \
GATEWAY_MEM_LIMIT=1G \
GATEWAY_MEM_RESERVATION=512M \
docker compose -f docker/docker-compose-local.yaml up -d
```

Start the gateway with runtime plugins enabled:

```bash
CARGO_NET_GIT_FETCH_WITH_CLI=true cargo run --release --features test-plugins --bin contextforge-gateway-rs -- \
  --address 0.0.0.0:8001 \
  --redis-port 6379 \
  --redis-address 127.0.0.1 \
  --token-verification-public-key assets/jwt.key.pub \
  --token-verification-private-key assets/jwt.key \
  --number-of-cpus 16 \
  --redis-mode=plain-text \
  --upstream-connection-mode=plain-text-or-tls \
  --runtime-plugins-enabled true
```

Get a token:

```bash
TOKEN=$(curl --silent --show-error --request GET \
  --url http://127.0.0.1:8001/contextforge-rs/admin/tokens/admin@example.com \
  --header 'accept: application/json' \
  --header 'content-type: application/json')
```

Create the gateway user config:

```bash
curl --silent --show-error --request POST \
  --url http://127.0.0.1:8001/contextforge-rs/admin/userconfigs/admin@example.com \
  --header "authorization: Bearer ${TOKEN}" \
  --header 'content-type: application/json' \
  --data '{
    "virtualHosts": {
      "c0ffee00f001f00lf00ldeadbeefdead": {
        "backends": {
          "gateway-one": { "url": "http://127.0.0.1:5555/mcp" }
        }
      }
    }
  }'
```

Register the payload marker config in Redis:

```bash
docker compose -f docker/docker-compose-local.yaml exec -T redis redis-cli SET ContextForgeGatewayRuntimePluginConfig '{
  "version": 1,
  "cpex": {
    "plugins": [
      {
        "name": "payload-marker",
        "kind": "contextforge/payload-marker",
        "hooks": ["cmf.tool_post_invoke"]
      }
    ]
  }
}'
```

```bash
sleep 3
```

Open an MCP session:

```bash
INIT_HEADERS=$(mktemp)
```

```bash
curl --silent --show-error \
  --dump-header "${INIT_HEADERS}" \
  --url http://127.0.0.1:8001/contextforge-rs/servers/c0ffee00f001f00lf00ldeadbeefdead/mcp \
  --header "authorization: Bearer ${TOKEN}" \
  --header 'content-type: application/json' \
  --header 'accept: application/json, text/event-stream' \
  --data '{
    "jsonrpc": "2.0",
    "id": 0,
    "method": "initialize",
    "params": {
      "protocolVersion": "2025-11-25",
      "capabilities": {},
      "clientInfo": { "name": "curl", "version": "0.1.0" }
    }
  }'
```

```bash
SESSION_ID=$(awk 'tolower($1) == "mcp-session-id:" { gsub("\r", "", $2); print $2 }' "${INIT_HEADERS}")
```

Send a tool request:

```bash
curl --silent --show-error \
  --url http://127.0.0.1:8001/contextforge-rs/servers/c0ffee00f001f00lf00ldeadbeefdead/mcp \
  --header "authorization: Bearer ${TOKEN}" \
  --header "mcp-session-id: ${SESSION_ID}" \
  --header 'content-type: application/json' \
  --header 'accept: application/json, text/event-stream' \
  --data '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/call",
    "params": {
      "name": "gateway-one-say_hello",
      "arguments": {}
    }
  }'
```

The response content should include the backend tool result plus an additional text part:

```text
[cpex:payload-marker]
```

## Performance Tests

As above and then run:
```bash
cargo run --release --bin contextforge-load-test -- --host 'http://127.0.0.1:8001' -r 40 -u 120 --run-time 120s --report-file report.html

```

[Performance reports](./reports)
