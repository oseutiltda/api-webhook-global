#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_DIR="$ROOT_DIR/.dev/pids"

stop_pid_file() {
  local pid_file="$1"
  local name="$2"
  if [ -f "$pid_file" ]; then
    local pid
    pid="$(cat "$pid_file" 2>/dev/null || true)"
    if [ -n "${pid:-}" ] && kill -0 "$pid" 2>/dev/null; then
      echo "[dev-stop] Parando $name (pid=$pid)..."
      kill "$pid" || true
    else
      echo "[dev-stop] $name não está rodando."
    fi
    rm -f "$pid_file"
  else
    echo "[dev-stop] PID de $name não encontrado."
  fi
}

stop_pid_file "$PID_DIR/backend.pid" "backend"
stop_pid_file "$PID_DIR/frontend.pid" "frontend"

echo "[dev-stop] Finalizado."
