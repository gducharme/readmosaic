# Deploy Runtime Scaffold

This directory contains the deploy-oriented Go module for the Mosaic terminal runtime.

## Entrypoints

- **Run locally**: `make run`
- **Build binary**: `make build`
- **Run tests**: `make test`
- **Offline CI checks**: `make ci-offline`

## Startup flow contract

1. Load configuration from environment.
2. Build Wish SSH server runtime.
3. Attach middleware chain in strict order:
   - `rate-limit` (per-IP token bucket; defaults: 30 attempts per 1m, burst 10)
   - `username-routing`
   - `session-metadata`
4. Listen on internal port `2222` by default.

This flow is protected by runtime and boot tests in `internal/server/runtime_test.go`.

## Configuration

- `MOSAIC_SSH_HOST` (default `0.0.0.0`, must not be empty)
- `MOSAIC_SSH_PORT` (default `2222`, integer in `[1,65535]`)
- `MOSAIC_SSH_HOST_KEY_PATH` (default `.data/host_ed25519`, must not be empty or resolve to `.`)
- `MOSAIC_SSH_IDLE_TIMEOUT` (default `120s`, must be `> 0`)
- `MOSAIC_SSH_MAX_SESSIONS` (default `32`, must be `> 0`)
- `MOSAIC_SSH_RATE_LIMIT_PER_SECOND` (default `20`, must be `> 0`)
- `RATE_LIMIT_MAX_ATTEMPTS` (default `30`, attempts replenished across `RATE_LIMIT_WINDOW`)
- `RATE_LIMIT_WINDOW` (default `1m`, must be `> 0`)
- `RATE_LIMIT_BURST` (default `10`, must be `> 0`)
- `RATE_LIMIT_BAN_DURATION` (default `0s`, temporary ban after exhaustion, `>= 0`)
- `RATE_LIMIT_MAX_TRACKED_IPS` (default `10000`, caps memory use)
- `RATE_LIMIT_ENABLED` (default `true`; set `false` to disable in dev/test)
- `RATE_LIMIT_TRUST_PROXY_HEADERS` (default `false`; only trusts a proxy IP injected via trusted middleware value `mosaic.proxy_ip`)

## Shim contract (explicit)

This scaffold currently uses local `replace` directives in `go.mod` to point at `third_party/charmbracelet/*` shims.

Intentionally fake behaviors in shim mode:
- no SSH handshake or auth
- placeholder host-key file contents
- TCP-backed pseudo sessions

Behavior expected to remain compatible with upstream integration points:
- option-based server construction
- middleware wrapping order
- `ListenAndServe`/`Shutdown` lifecycle with server-closed signaling

Startup logs include mode (`shim`) and enabled middleware.

## Offline strategy

This module uses **replace-only** offline strategy (no vendored dependencies). Use `make ci-offline` to validate builds with `GOPROXY=off` and `GOSUMDB=off`.

## Layout

- `cmd/server/main.go` — composition root + process lifecycle.
- `internal/config` — env parsing/validation.
- `internal/server` — runtime assembly and `Run(ctx)`.
- `internal/router` — middleware descriptors + middleware logic.
- `internal/{tui,theme,content,commands,store,rtl,model}` — feature package placeholders.

## Operational snippets

### systemd (example)

```ini
[Unit]
Description=Mosaic SSH Terminal
After=network.target

[Service]
WorkingDirectory=/opt/mosaic/deploy
Environment=MOSAIC_SSH_PORT=2222
ExecStart=/opt/mosaic/deploy/bin/mosaic-server
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

### Container run (example)

```bash
docker run --rm -p 2222:2222 \
  -e MOSAIC_SSH_HOST=0.0.0.0 \
  -e MOSAIC_SSH_PORT=2222 \
  mosaic-terminal:latest
```

## Docker deployment assets

- `Dockerfile` provides a **multi-stage build** that compiles a static Linux binary in a Go builder image, then copies only the binary, CA bundle, and minimal passwd/group metadata into a `scratch` runtime.
- `docker-compose.yml` provides:
  - `app` service for the SSH runtime.
  - Optional `neo4j` service (enable with `--profile neo4j`) for local integration.
  - Host key bind mount:
    - `./data/ssh/host_ed25519:/run/keys/ssh_host_ed25519:ro`
  - Persistent named volume: `neo4j-data:/data`.
  - Port publication via `MOSAIC_SSH_PUBLISH_PORT`:
    - Default: `22:2222`
    - Fallback when host 22 is unavailable: `2222:2222` (set `MOSAIC_SSH_PUBLISH_PORT=2222`).

### Compose quick start

```bash
cd deploy
mkdir -p data/ssh
# Provide an existing ed25519 private host key at ./data/ssh/host_ed25519
# chmod 600 ./data/ssh/host_ed25519

# default publish strategy (host 22 -> container 2222)
docker compose up --build -d

# fallback publish strategy (host 2222 -> container 2222)
MOSAIC_SSH_PUBLISH_PORT=2222 docker compose up --build -d

# enable optional Neo4j integration service
MOSAIC_SSH_PUBLISH_PORT=2222 docker compose --profile neo4j up --build -d
```

## Healthcheck strategy

- **App service (`scratch`)**: there is no shell in the container, so define health from the outside using a TCP probe against the published SSH port.
  - Example host probe: `nc -zv 127.0.0.1 ${MOSAIC_SSH_PUBLISH_PORT:-22}`
  - In orchestrators, use TCP socket probes against container port `2222`.
- **Neo4j service**: compose includes an in-container `cypher-shell` healthcheck (`RETURN 1;`).
- Inspect status with `docker compose ps` and only treat the stack as healthy when app TCP and (if enabled) Neo4j healthcheck both pass.

## Logging guidance

- Both services default to Docker `json-file` logging with rotation (`max-size=10m`, `max-file=3`) in `docker-compose.yml`.
- Follow runtime logs with:
  - `docker compose logs -f app`
  - `docker compose --profile neo4j logs -f neo4j`
- The app emits startup metadata (runtime mode + middleware chain) and rate-limit events (`remote_ip`, counters, reason). Route these logs to your collector in production.

## Using real upstream dependencies

To switch to upstream modules:

1. Remove `replace github.com/charmbracelet/...` lines from `go.mod`.
2. Set real versions for `github.com/charmbracelet/wish` and `github.com/charmbracelet/ssh`.
3. Run `go mod tidy` (with internet access).
4. Delete `third_party/charmbracelet` if no longer needed.


## Rate-limiting behavior contract

### Client experience

- A limited client receives: `rate limit exceeded\n` and the session is ended before TUI middleware runs.
- Rate limiting is applied per normalized remote IP (IPv4 and IPv6-mapped IPv4 normalize to the same key).
- Clients behind the same NAT share a bucket by design.

### Operational safety

- Limiter state is **in-memory only**; process restarts clear buckets and counters.
- A hard cap (`RATE_LIMIT_MAX_TRACKED_IPS`) prevents unbounded map growth.
- Stale IP entries are evicted with TTL cleanup to reduce long-lived memory pressure.
- When capacity is full, unseen IPs are throttled (protects memory at the cost of possible false positives under attack).
- In clustered deployments, each node enforces limits independently (no cross-node coordination in this scaffold).

### Tuning guidance

- Raise `RATE_LIMIT_MAX_ATTEMPTS` and/or `RATE_LIMIT_BURST` for trusted private networks.
- Increase `RATE_LIMIT_BAN_DURATION` if abusive clients reconnect aggressively.
- Keep `RATE_LIMIT_TRUST_PROXY_HEADERS=false` unless a trusted SSH gateway middleware sets `mosaic.proxy_ip`.

### Observability

Throttled attempts emit structured logs with:

- `remote_ip`
- `timestamp`
- `reason`
- `rate_limit_hits`
- `total_blocked_connections`
- `active_tracked_ips`
