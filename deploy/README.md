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
   - `rate-limit` (per-second connection rate window)
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

## Healthcheck strategy

SSH transport has no HTTP health endpoint in this scaffold. Use a TCP healthcheck on `${MOSAIC_SSH_HOST}:${MOSAIC_SSH_PORT}` and watch startup/session lifecycle logs.

## Using real upstream dependencies

To switch to upstream modules:

1. Remove `replace github.com/charmbracelet/...` lines from `go.mod`.
2. Set real versions for `github.com/charmbracelet/wish` and `github.com/charmbracelet/ssh`.
3. Run `go mod tidy` (with internet access).
4. Delete `third_party/charmbracelet` if no longer needed.
