# Deploy Runtime Scaffold

This directory contains the initial deploy-oriented Go module for the Mosaic terminal runtime.

## Entrypoints

- **Run locally**: `go run ./cmd/server`
- **Build binary**: `go build -o bin/mosaic-server ./cmd/server`
- **Container/deploy entrypoint**: use the built `cmd/server` binary as your process entry.

## Startup flow

1. Load configuration from environment (`MOSAIC_SSH_HOST`, `MOSAIC_SSH_PORT`).
2. Construct a Wish SSH server instance.
3. Attach middleware chain (rate limiting, username routing, session context).
4. Listen on internal port `2222` by default.

## Layout

- `cmd/server/main.go` — standard startup entrypoint.
- `internal/config` — environment config loading.
- `internal/server` — Wish server builder + launcher.
- `internal/router` — middleware chain registration.
- `internal/{tui,theme,content,commands,store,rtl,model}` — feature package placeholders.
