#!/usr/bin/env bash
set -euo pipefail

REMOTE_HOST="hell"
REMOTE_DIR="~/code"
CONTAINER_NAME="mosaic-terminal"
IMAGE_NAME="web-mosaic-terminal"
ARCHIVE_NAME="web-deploy.zip"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WEB_DIR="$ROOT_DIR/web"
ARCHIVE_PATH="/tmp/$ARCHIVE_NAME"

if ! command -v zip >/dev/null 2>&1; then
  echo "error: 'zip' is required but not installed." >&2
  exit 1
fi

if ! command -v ssh >/dev/null 2>&1; then
  echo "error: 'ssh' is required but not installed." >&2
  exit 1
fi

if ! command -v scp >/dev/null 2>&1; then
  echo "error: 'scp' is required but not installed." >&2
  exit 1
fi

if [[ ! -d "$WEB_DIR" ]]; then
  echo "error: web directory not found at $WEB_DIR" >&2
  exit 1
fi

rm -f "$ARCHIVE_PATH"

echo "[1/4] Creating archive: $ARCHIVE_PATH"
(
  cd "$ROOT_DIR"
  zip -qr "$ARCHIVE_PATH" web
)

echo "[2/4] Copying archive to $REMOTE_HOST:$REMOTE_DIR"
scp "$ARCHIVE_PATH" "$REMOTE_HOST:$REMOTE_DIR/$ARCHIVE_NAME"

echo "[3/4] Updating remote docker artifacts"
ssh "$REMOTE_HOST" "bash -lc '
set -euo pipefail
cd $REMOTE_DIR

if docker ps -a --format \"{{.Names}}\" | grep -Fxq \"$CONTAINER_NAME\"; then
  docker stop \"$CONTAINER_NAME\" >/dev/null 2>&1 || true
  docker rm \"$CONTAINER_NAME\" >/dev/null 2>&1 || true
fi

if docker image inspect \"$IMAGE_NAME\" >/dev/null 2>&1; then
  docker image rm \"$IMAGE_NAME\" >/dev/null 2>&1 || true
fi

SIGNUP_FILE=\"$REMOTE_DIR/web/data/more_email_signups.csv\"
SIGNUP_BAK=\"$REMOTE_DIR/.more_email_signups.csv.bak\"
if [ -f \"$SIGNUP_FILE\" ]; then
  cp -p \"$SIGNUP_FILE\" \"$SIGNUP_BAK\"
  echo \"[deploy] backed up more_email_signups.csv\"
else
  echo \"[deploy] no signup CSV to back up\"
fi

unzip -oq \"$ARCHIVE_NAME\"

mkdir -p \"$REMOTE_DIR/web/data\"
if [ -f \"$SIGNUP_BAK\" ]; then
  mv -f \"$SIGNUP_BAK\" \"$SIGNUP_FILE\"
  echo \"[deploy] restored more_email_signups.csv\"
fi

cd web
docker compose up --build
'"

echo "[4/4] Complete"
