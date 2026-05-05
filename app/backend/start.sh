#!/bin/sh
set -eu
cd /workspace/app/backend 2>/dev/null || cd "$(dirname "$0")"
exec python startup_probe.py
