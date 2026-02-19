# Web SSH Terminal

Ultra lightweight web interface for connecting to the Mosaic SSH runtime from a browser.

## What it does

- Renders one page with:
  - a small username dialog box
  - a full terminal pane underneath
- Uses `ssh <username>@<host> -p <port>` under the hood.

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

Example for local deploy stack:

```bash
SSH_HOST=127.0.0.1 SSH_PORT=2222 npm start
```
