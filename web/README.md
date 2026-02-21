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

- `root` → Reader mode
- `archivist` → Editor mode
