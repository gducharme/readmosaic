# Deploy Runtime Scaffold

This directory contains the deploy-oriented Go module and container assets for the Mosaic SSH runtime.

## What changed and why

The deploy setup now includes:

- A pinned multi-stage Docker build for reproducible, smaller runtime images.
- A human-readable Compose stack with:
  - `app` service (SSH runtime)
  - optional `neo4j` service (`--profile neo4j`)
  - named volumes and explicit network
  - healthchecks, restart policy, and logging rotation
- A canonical `.env.example` aligned with `internal/config.LoadFromEnv()`.
- CI workflow improvements for container build/test/scan.

## File map

- `Dockerfile` — multi-stage build (`golang` builder + minimal Alpine runtime).
- `docker-compose.yml` — base deployment defaults (host port default `22`).
- `docker-compose.override.yml` — dev defaults (host port default `2222`, rate-limit disabled).
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
- `MOSAIC_SSH_HOST_KEY_PATH` (default `.data/host_ed25519`)
- `MOSAIC_SSH_IDLE_TIMEOUT` (default `120s`)
- `MOSAIC_SSH_MAX_SESSIONS` (default `32`)
- `MOSAIC_SSH_RATE_LIMIT_PER_SECOND` (default `20`)

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

### Optional app-side Neo4j client config

Set all 3 together or leave unset:

- `NEO4J_URI`
- `NEO4J_USER`
- `NEO4J_PASSWORD`

### Optional Neo4j compose service auth

- `NEO4J_AUTH` (default `neo4j/localdevpassword`)

## Ports and defaults

- Base compose (`docker-compose.yml`) publishes `${MOSAIC_SSH_PUBLISH_PORT:-22}:2222`.
- Dev override (`docker-compose.override.yml`) publishes `${MOSAIC_SSH_PUBLISH_PORT:-2222}:2222`.

This is intentional:

- **Deploy/default base** can target host SSH port 22.
- **Local dev** avoids privileged port surprises by defaulting to 2222.

## Local dev flow

```bash
cd deploy
cp .env.example .env
mkdir -p data/ssh
# provide an existing ed25519 private key:
# data/ssh/host_ed25519
chmod 600 data/ssh/host_ed25519

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

## Healthcheck and logging strategy

- App healthcheck: TCP probe (`nc -z 127.0.0.1 2222`) in container.
- Neo4j healthcheck: `cypher-shell 'RETURN 1;'`.
- Logs: Docker `json-file` with rotation (`10m`, `3 files`).

## CI workflow intent

`.github/workflows/deploy-go.yml` keeps two test styles intentionally:

1. `go test ./...` on runner toolchain (fast host validation).
2. `go test ./...` inside pinned Go container (toolchain parity with image build environment).

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

`depends_on` is limited to `condition: service_healthy` (no `required: false`) for broader compatibility with older Compose implementations. The app should tolerate Neo4j being absent unless Neo4j env vars are explicitly configured.

## How to test (copy/paste)

```bash
cd deploy
cp .env.example .env
mkdir -p data/ssh
# place a valid key at data/ssh/host_ed25519
chmod 600 data/ssh/host_ed25519

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

- **Port bind denied on 22**: set `MOSAIC_SSH_PUBLISH_PORT=2222`.
- **Host key mount errors**: verify file exists and `chmod 600`.
- **Neo4j unhealthy**: check `docker compose logs neo4j` and auth env.
- **App unhealthy**: inspect `docker compose logs app` for startup/env errors.
