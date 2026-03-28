#!/bin/sh
set -eu

PORT_VALUE="${WEB_PORT:-${PORT:-8000}}"

case "$PORT_VALUE" in
  ''|*[!0-9]*)
    PORT_VALUE="8000"
    ;;
esac

cd /workspace/app/backend 2>/dev/null || cd "$(dirname "$0")"
exec uvicorn app.main:app --host 0.0.0.0 --port "$PORT_VALUE"
