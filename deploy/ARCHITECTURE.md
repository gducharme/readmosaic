# Deploy Architecture

## Layer diagram

`cmd/server` -> `internal/config` + `internal/router` + `internal/server` -> `third_party/charmbracelet/*`

Future feature packages (`internal/tui`, `internal/theme`, `internal/content`, `internal/commands`, `internal/store`, `internal/rtl`, `internal/model`) hang off routing/session concerns, not transport wiring.

## Import rules

- `internal/tui` MUST NOT import `internal/server` or `internal/config`.
- `internal/router` should depend only on transport/session abstractions and model/session structures.
- `internal/server` owns transport lifecycle and MUST NOT import TUI internals.
- `cmd/server` is the composition root (env -> config -> middleware descriptor chain -> server run).

These constraints keep Wish transport concerns from leaking into UI/content modules.
