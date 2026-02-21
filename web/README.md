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

You can override with environment variables:

- `ROOT_CODE`
- `ARCHIVIST_CODE`
