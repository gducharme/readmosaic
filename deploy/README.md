# Deploy Runtime Scaffold

This directory contains the deploy-oriented Go module and container assets for the Mosaic SSH runtime.

## What changed and why

The deploy setup now includes:

- A pinned multi-stage Docker build for reproducible, smaller runtime images.
- A human-readable Compose stack with:
  - `app` service (SSH runtime + gateway API)
  - `web` service (browser terminal UI)
  - optional `neo4j` service (`--profile neo4j`)
  - named volumes and explicit network
  - healthchecks, restart policy, and logging rotation
- A canonical `.env.example` aligned with `internal/config.LoadFromEnv()`.
- CI workflow improvements for container build/test/scan.

## File map

- `Dockerfile` — multi-stage build (`golang` builder + minimal Alpine runtime).
- `docker-compose.yml` — base deployment defaults with immutable `app` and `web` images (host web port default `3000`).
- `docker-compose.override.yml` — dev overrides (host SSH port default `2222`, rate-limit disabled, web bind-mount/live install flow).
- `.env.example` — copy to `.env` and customize.
- `Makefile` — helper commands for local build/run/test.

## Runtime startup contract

Startup sequence:

1. Load env config.
2. Build SSH runtime.
3. Attach middleware in this order:
   - `rate-limit`
   - `username-routing`
   - `session-metadata`
4. Listen on SSH port (`MOSAIC_SSH_PORT`, default `2222`).

Behavior is covered by tests in `internal/server/runtime_test.go`.

## Environment variables (canonical)

These are read by `internal/config.LoadFromEnv()`:

### Core SSH

- `MOSAIC_SSH_HOST` (default `0.0.0.0`)
- `MOSAIC_SSH_PORT` (default `2222`)
- `MOSAIC_SSH_HOST_KEY_PATH` (default `/run/keys/ssh_host_ed25519` in container deployments)
- `MOSAIC_SSH_IDLE_TIMEOUT` (default `120s`)
- `MOSAIC_SSH_MAX_SESSIONS` (default `32`)
- `MOSAIC_SSH_RATE_LIMIT_PER_SECOND` (default `20`)
- `MOSAIC_ARCHIVE_ROOT` (default `/archive`)
- `MOSAIC_ARCHIVE_HOST_DIR` (default `./data/archive`, bind-mounted in compose)

### Required content/indexing

- `ARWEAVE_TXID_MANIFESTO_EN`
- `ARWEAVE_TXID_MANIFESTO_AR`
- `ARWEAVE_TXID_MANIFESTO_ZH`
- `ARWEAVE_TXID_GENESIS`
- `BTC_ANCHOR_HEIGHT`
- `LISTEN_ADDR` (default `0.0.0.0:8080`)

### Rate limiting (canonical, non-legacy)

- `RATE_LIMIT_MAX_ATTEMPTS` (default `30`)
- `RATE_LIMIT_WINDOW` (default `1m`)
- `RATE_LIMIT_BURST` (default `10`)
- `RATE_LIMIT_BAN_DURATION` (default `0s`)
- `RATE_LIMIT_MAX_TRACKED_IPS` (default `10000`)
- `RATE_LIMIT_ENABLED` (default `true`)
- `RATE_LIMIT_TRUST_PROXY_HEADERS` (default `false`)

### Gateway SSH verification controls

- `GATEWAY_SSH_STRICT_HOST_KEY_CHECKING` (default `accept-new` for local dev; set `yes` in production)
- `GATEWAY_SSH_KNOWN_HOSTS` (default `/tmp/gateway_known_hosts`; mount a managed file in production)

### Optional app-side Neo4j client config

Set all 3 together or leave unset:

- `NEO4J_URI`
- `NEO4J_USER`
- `NEO4J_PASSWORD`

### Optional Neo4j compose service auth

- `NEO4J_AUTH` (default `neo4j/localdevpassword`)

## Ports and defaults

- Base compose (`docker-compose.yml`) publishes `${MOSAIC_SSH_PUBLISH_PORT:-22}:2222`, `${MOSAIC_GATEWAY_PUBLISH_PORT:-8080}:8080`, and `${MOSAIC_WEB_BIND_HOST:-0.0.0.0}:${MOSAIC_WEB_PUBLISH_PORT:-3000}:3000`.
- Dev override (`docker-compose.override.yml`) publishes `${MOSAIC_SSH_PUBLISH_PORT:-2222}:2222` and `${MOSAIC_GATEWAY_PUBLISH_PORT:-8080}:8080` for the `app` service and enables bind-mounted web development workflow with cached `node_modules` volume state.

This is intentional:

- **Deploy/default base** can target host SSH port 22.
- **Local dev** avoids privileged ports by defaulting SSH to 2222 and browser UI to 3000 while still exposing the HTTP gateway on 8080.
- **Web bind host** defaults to `127.0.0.1` in `.env.example` to reduce accidental external exposure; override `MOSAIC_WEB_BIND_HOST` when you intentionally need remote access.
- Archive storage is bind-mounted from `MOSAIC_ARCHIVE_HOST_DIR` into `MOSAIC_ARCHIVE_ROOT`; point `MOSAIC_ARCHIVE_HOST_DIR` at shared/host-persistent storage if you run multiple container instances.


## Web UI exposure and security

The `web` service is a browser terminal surface. Do not expose it directly to the public internet without additional controls (TLS termination, authentication, and network restrictions such as allowlists or private ingress).

The service sets both `WEB_PORT` and `PORT` to `3000` in Compose for compatibility with common Node server conventions.

## Local dev flow

```bash
cd deploy
cp .env.example .env
mkdir -p data/ssh data/archive
# provide an existing ed25519 private key:
# data/ssh/host_ed25519
chmod 600 data/ssh/host_ed25519
# archive documents are persisted in data/archive and mounted at /archive in the container

# uses docker-compose.yml + docker-compose.override.yml by default
docker compose up --build -d
```

## Production-style flow

```bash
cd deploy
cp .env.example .env
# set MOSAIC_SSH_PUBLISH_PORT=22 for host port 22 if desired
# configure real ARWEAVE_TXID_* / BTC_ANCHOR_HEIGHT / secrets

# use base compose only (skip override)
docker compose -f docker-compose.yml up --build -d
# optional neo4j service
docker compose -f docker-compose.yml --profile neo4j up --build -d
```

## Make targets

```bash
make run          # go run ./cmd/server
make build        # go build binary
make test         # go test ./...
make vet          # go vet ./...
make build-image  # docker build image
make up           # docker compose up --build -d
make down         # docker compose down
make logs         # tail app logs
make docker-test  # deterministic image smoke check with required key/env
```

## Git pre-commit hook (gofmt check)

A repo-managed pre-commit hook is available at `../.githooks/pre-commit` to enforce formatting in this Go module.

Enable it once per clone:

```bash
git config core.hooksPath .githooks
```

The hook runs:

```bash
cd deploy && gofmt -l ./cmd ./internal ./third_party
```

and blocks commits when formatting is needed.

## Healthcheck and logging strategy

- App healthcheck: TCP probe (`nc -z 127.0.0.1 2222`) in container.
- Neo4j healthcheck: `cypher-shell 'RETURN 1;'`.
- Logs: Docker `json-file` with rotation (`10m`, `3 files`).

## CI workflow intent

`.github/workflows/deploy-go.yml` runs `go test ./...` on the runner toolchain for fast validation.

It also performs:

- Buildx image build
- main-branch multi-arch push (`linux/amd64`, `linux/arm64`)
- Trivy filesystem scan
- Trivy image scan (PR image tag)

## Trivy tag alignment note

On PRs, image build uses tag:

- `${IMAGE_NAME}:pr-${GITHUB_SHA}`

The Trivy image scan references the same tag, so scan/build tags stay aligned.


## Compose compatibility note

The `app` service does not declare `depends_on` for `neo4j`. This keeps Neo4j truly optional across Compose versions and lets the app start independently unless Neo4j client env vars are explicitly configured.

## How to test (copy/paste)

```bash
cd deploy
cp .env.example .env
mkdir -p data/ssh data/archive
# place a valid key at data/ssh/host_ed25519
chmod 600 data/ssh/host_ed25519
# archive documents are persisted in data/archive and mounted at /archive in the container

make test
make vet

# render compose with dev override
MOSAIC_SSH_PUBLISH_PORT=2222 docker compose -f docker-compose.yml -f docker-compose.override.yml config

# app only
MOSAIC_SSH_PUBLISH_PORT=2222 docker compose up --build -d app
docker compose ps
nc -zv 127.0.0.1 2222

# optional neo4j
MOSAIC_SSH_PUBLISH_PORT=2222 docker compose --profile neo4j up --build -d
docker compose --profile neo4j ps

# cleanup
docker compose --profile neo4j down -v
```

## Troubleshooting

### Gateway logs from Docker

When `GATEWAY_HMAC_SECRET` is configured, the gateway now emits one structured log line per HTTP request with method/path/status/duration. Tail those logs from the container with:

```bash
docker logs --tail=100 -f mosaic-terminal
```

If you run through Compose service naming instead of an explicit `container_name`, use:

```bash
docker compose logs --tail=100 -f app
```

- **Port bind denied on 22**: set `MOSAIC_SSH_PUBLISH_PORT=2222`.
- **Port bind denied on 3000**: set `MOSAIC_WEB_PUBLISH_PORT` to any available host port.
- **Need remote web access**: set `MOSAIC_WEB_BIND_HOST=0.0.0.0` (or a specific interface IP), then confirm firewall/TLS/auth controls.
- **Host key mount errors**: verify file exists and `chmod 600`.
- **Neo4j unhealthy**: check `docker compose logs neo4j` and auth env.
- **App unhealthy**: inspect `docker compose logs app` for startup/env errors.
- **Gateway ECONNREFUSED from web terminal**: verify Compose publishes `8080` (or your configured `MOSAIC_GATEWAY_PUBLISH_PORT`) and that `GATEWAY_BASE_URL` points to that host/port.
