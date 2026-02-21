# Mosaic Terminal

## Run locally

```bash
cd web
npm install
npm start
```

Open http://localhost:3000

## Run with Docker Compose

```bash
cd web
docker compose up --build
```

Data is read/written from `./data` on the host and mounted to `/data` in the container.

## Access codes

The backend enforces server-side auth on all `/api/*` endpoints via the `x-access-code` header.

Defaults:

- `root` → Reader mode (read-only API access)
- `archivist` → Editor mode (read + write API access)

The login screen validates whatever code is configured server-side, so these defaults can be safely overridden.

You can override with environment variables:

- `ROOT_CODE`
- `ARCHIVIST_CODE`

## Security note

If you expose this service beyond localhost, run it behind HTTPS (for example via a reverse proxy). The access code is sent in an HTTP header and must not traverse plaintext HTTP.
