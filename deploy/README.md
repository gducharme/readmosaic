# Deploy Runtime Scaffold

This directory contains the deploy-oriented Go module for the Mosaic terminal runtime.

## Entrypoints

- **Run locally**: `make run`
- **Build binary**: `make build`
- **Run tests**: `make test`
- **Offline CI checks**: `make ci-offline`
- **Build image**: `make build-image`
- **Start stack**: `make up`
- **Stop stack**: `make down`

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

## Docker deployment assets

- `Dockerfile` is a multi-stage build:
  - builder: `golang:1.22.8-alpine3.20`
  - runtime: pinned minimal `alpine:3.20.6`
  - static Go build (`CGO_ENABLED=0`) with reproducible flags (`-trimpath -buildvcs=false -ldflags='-s -w -buildid='`)
  - non-root runtime user (`uid/gid 65532`)
- `docker-compose.yml` provides:
  - `app` (SSH runtime)
  - optional `neo4j` (`--profile neo4j`)
  - host key bind mount `./data/ssh/host_ed25519:/run/keys/ssh_host_ed25519:ro`
  - named persistence volumes (`neo4j-data`, `neo4j-logs`)
  - explicit `mosaic-app` network
  - healthchecks for app (TCP via `nc`) and Neo4j (`cypher-shell RETURN 1`)
  - restart policy + log rotation
- `docker-compose.override.yml` is the default dev override:
  - host port defaults to `2222:2222`
  - sets `RATE_LIMIT_ENABLED=false`

## Required env vars / secrets

1. Copy `.env.example` to `.env`.
2. Required secret material:
   - SSH host key at `./data/ssh/host_ed25519` (private key, mode `600`)
3. Optional Neo4j credentials:
   - `NEO4J_AUTH` and `NEO4J_PASSWORD` (used only when neo4j profile is enabled)

## Local development flow

```bash
cd deploy
cp .env.example .env
mkdir -p data/ssh
# provide an existing ed25519 private key file
chmod 600 data/ssh/host_ed25519

# dev defaults from docker-compose.override.yml (2222:2222)
docker compose up --build -d
```

## Production deploy flow

```bash
cd deploy
cp .env.example .env
# set MOSAIC_SSH_PUBLISH_PORT=22 in .env for host port 22 mapping
# provide production SSH host key at data/ssh/host_ed25519

docker compose -f docker-compose.yml up --build -d
# optional with graph integration
docker compose -f docker-compose.yml --profile neo4j up --build -d
```

## Healthcheck strategy

- App health uses a TCP probe (`nc -z 127.0.0.1 2222`) inside the runtime image.
- Neo4j health uses `cypher-shell`.
- Validate status with `docker compose ps`.

## Logging guidance

- Both services use Docker `json-file` driver with rotation (`10m`, `3 files`).
- Stream logs:
  - `docker compose logs -f app`
  - `docker compose --profile neo4j logs -f neo4j`

## How to build and run in CI

- Workflow `.github/workflows/deploy-go.yml` now:
  - runs Go format/tests/vet
  - builds multi-arch container image via buildx (`linux/amd64,linux/arm64`)
  - runs containerized smoke test (`go test ./...` in container)
  - runs Trivy filesystem and image scans
  - can push images on `main`

## Image size comparison

- `docker` CLI is not available in this execution environment, so an actual local image size diff could not be produced here.
- Run this locally to compare before/after image sizes:

```bash
cd deploy
# previous image tag (replace with your baseline build/tag)
docker image inspect mosaic-terminal:previous --format='{{.Size}}'
# current image tag
docker build -t mosaic-terminal:local .
docker image inspect mosaic-terminal:local --format='{{.Size}}'
```

## Troubleshooting

- **Port 22 unavailable**: set `MOSAIC_SSH_PUBLISH_PORT=2222` in `.env`.
- **Host key permission error**: ensure `chmod 600 data/ssh/host_ed25519`.
- **Neo4j not starting**: verify `NEO4J_AUTH`, inspect `docker compose logs neo4j`.
- **Healthcheck unhealthy**: verify container port 2222 binds and that SSH server started (`docker compose logs app`).

## How to test

```bash
cd deploy
cp .env.example .env
mkdir -p data/ssh
# add a valid key before starting stack

# Go checks
make test
make vet

# compose syntax
MOSAIC_SSH_PUBLISH_PORT=2222 docker compose config

# build and run app only
MOSAIC_SSH_PUBLISH_PORT=2222 docker compose up --build -d app
docker compose ps
docker compose logs --no-color app | tail -n 50

# app health probe from host
nc -zv 127.0.0.1 2222

# optional: neo4j profile
MOSAIC_SSH_PUBLISH_PORT=2222 docker compose --profile neo4j up --build -d
docker compose --profile neo4j ps

# cleanup
docker compose --profile neo4j down -v
```

## Using real upstream dependencies

To switch to upstream modules:

1. Remove `replace github.com/charmbracelet/...` lines from `go.mod`.
2. Set real versions for `github.com/charmbracelet/wish` and `github.com/charmbracelet/ssh`.
3. Run `go mod tidy` (with internet access).
4. Delete `third_party/charmbracelet` if no longer needed.
