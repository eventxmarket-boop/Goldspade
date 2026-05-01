#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if ! pgrep -x redis-server >/dev/null 2>&1; then
  redis-server --daemonize yes
fi

if [ -f "$ROOT_DIR/go/executor/bin/executor" ]; then
  "$ROOT_DIR/go/executor/bin/executor" &
fi

if [ -f "$ROOT_DIR/python/orchestrator/worker.py" ]; then
  python3 "$ROOT_DIR/python/orchestrator/worker.py" &
fi
