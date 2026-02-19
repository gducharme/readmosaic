# Web SSH Terminal

Ultra lightweight web interface for connecting to the Mosaic SSH runtime from a browser.

## What it does

- Renders one page with:
  - a small username dialog box
  - a full terminal pane underneath
- Opens a gateway session through `deploy/internal/gateway/http.go` before starting SSH.
- Uses `ssh <username>@<host> -p <port>` under the hood for interactive terminal IO.

## Run

```bash
cd web
npm install
npm start
```

Then open `http://localhost:3000`.

## Configuration

- `WEB_PORT` (default `3000`)
- `SSH_HOST` (default `127.0.0.1`)
- `SSH_PORT` (default `2222`)
- `GATEWAY_BASE_URL` (default `http://127.0.0.1:8080`)
- `GATEWAY_TARGET_HOST` (default follows `SSH_HOST`)
- `GATEWAY_TARGET_PORT` (default follows `SSH_PORT`)

Example for local deploy stack:

```bash
SSH_HOST=127.0.0.1 SSH_PORT=2222 npm start
```

Example using both the SSH runtime and gateway API:

```bash
SSH_HOST=127.0.0.1 SSH_PORT=2222 GATEWAY_BASE_URL=http://127.0.0.1:8080 npm start
```
