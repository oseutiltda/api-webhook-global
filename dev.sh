#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

LOG_DIR="$ROOT_DIR/.dev/logs"
PID_DIR="$ROOT_DIR/.dev/pids"
mkdir -p "$LOG_DIR" "$PID_DIR"

BACKEND_PID_FILE="$PID_DIR/backend.pid"
FRONTEND_PID_FILE="$PID_DIR/frontend.pid"
BACKEND_PORT="${BACKEND_PORT:-3000}"
FRONTEND_PORT="${FRONTEND_PORT:-3001}"
DEV_HOST="${DEV_HOST:-127.0.0.1}"

port_in_use() {
  local port="$1"
  if command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1
    return $?
  fi
  if command -v ss >/dev/null 2>&1; then
    ss -ltn 2>/dev/null | awk '{print $4}' | grep -Eq "[:.]${port}$"
    return $?
  fi
  return 1
}

show_port_owner() {
  local port="$1"
  if command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"$port" -sTCP:LISTEN || true
    return
  fi
  if command -v fuser >/dev/null 2>&1; then
    fuser -n tcp "$port" || true
  fi
}

kill_user_listener_on_port() {
  local port="$1"
  local pids=""

  if command -v lsof >/dev/null 2>&1; then
    pids="$(lsof -t -nP -iTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)"
  fi

  if [ -z "${pids:-}" ] && command -v fuser >/dev/null 2>&1; then
    pids="$(fuser -n tcp "$port" 2>/dev/null | tr ' ' '\n' | sed '/^$/d' || true)"
  fi

  if [ -z "${pids:-}" ]; then
    return
  fi

  for pid in $pids; do
    local owner
    owner="$(ps -o user= -p "$pid" 2>/dev/null | xargs || true)"
    if [ "$owner" = "$USER" ]; then
      echo "[dev] Matando processo do usuário na porta $port (pid=$pid)..."
      kill "$pid" || true
    fi
  done
  sleep 1
}

cleanup_project_orphans_on_port() {
  local port="$1"
  if ! command -v lsof >/dev/null 2>&1; then
    return
  fi

  local pids
  pids="$(lsof -t -nP -iTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)"
  if [ -z "${pids:-}" ]; then
    return
  fi

  local killed=0
  for pid in $pids; do
    local cmd
    cmd="$(ps -p "$pid" -o args= 2>/dev/null || true)"
    if [[ "$cmd" == *"$ROOT_DIR/backend"* ]] || [[ "$cmd" == *"$ROOT_DIR/frontend"* ]]; then
      echo "[dev] Encerrando processo órfão do projeto na porta $port (pid=$pid)..."
      kill "$pid" || true
      killed=1
    fi
  done

  if [ "$killed" -eq 1 ]; then
    sleep 1
  fi
}

cleanup_project_orphans_by_cmd() {
  local pids
  pids="$(ps -eo pid=,args= | awk -v root="$ROOT_DIR" '
    index($0, root "/backend") || index($0, root "/frontend") { print $1 }
  ' || true)"

  if [ -z "${pids:-}" ]; then
    return
  fi

  for pid in $pids; do
    if [ "$pid" = "$$" ]; then
      continue
    fi
    if kill -0 "$pid" 2>/dev/null; then
      echo "[dev] Encerrando processo órfão detectado por comando (pid=$pid)..."
      kill "$pid" || true
    fi
  done
  sleep 1
}

stop_if_running() {
  local pid_file="$1"
  local name="$2"
  if [ -f "$pid_file" ]; then
    local pid
    pid="$(cat "$pid_file" 2>/dev/null || true)"
    if [ -n "${pid:-}" ] && kill -0 "$pid" 2>/dev/null; then
      echo "[dev] Parando $name (pid=$pid)..."
      kill "$pid" || true
      sleep 1
    fi
    rm -f "$pid_file"
  fi
}

echo "[dev] Preparando ambiente dev local (backend + frontend)..."

if [ ! -f ".env" ]; then
  echo "[dev][erro] .env não encontrado na raiz."
  echo "[dev][dica] cp .env.example .env"
  exit 1
fi

if [ ! -d "backend/node_modules" ] || [ ! -d "frontend/node_modules" ]; then
  echo "[dev][erro] Dependências não instaladas."
  echo "[dev][dica] Rode: npm install --prefix backend && npm install --prefix frontend"
  exit 1
fi

stop_if_running "$BACKEND_PID_FILE" "backend"
stop_if_running "$FRONTEND_PID_FILE" "frontend"
cleanup_project_orphans_by_cmd
cleanup_project_orphans_on_port "$BACKEND_PORT"
cleanup_project_orphans_on_port "$FRONTEND_PORT"
kill_user_listener_on_port "$BACKEND_PORT"
kill_user_listener_on_port "$FRONTEND_PORT"

if port_in_use "$BACKEND_PORT"; then
  echo "[dev][erro] Porta $BACKEND_PORT já está em uso."
  show_port_owner "$BACKEND_PORT"
  echo "[dev][dica] Pare o processo da porta ou ajuste BACKEND_PORT."
  exit 1
fi

if port_in_use "$FRONTEND_PORT"; then
  echo "[dev][erro] Porta $FRONTEND_PORT já está em uso."
  show_port_owner "$FRONTEND_PORT"
  echo "[dev][dica] Pare o processo da porta ou ajuste FRONTEND_PORT."
  exit 1
fi

echo "[dev] Subindo backend em http://$DEV_HOST:$BACKEND_PORT ..."
(
  cd backend
  HOST="$DEV_HOST" \
  PORT="$BACKEND_PORT" \
  DOTENV_CONFIG_PATH=../.env \
    node -r dotenv/config -r ts-node/register src/server.ts \
    > "$LOG_DIR/backend.log" 2>&1
) &
BACKEND_PID=$!
echo "$BACKEND_PID" > "$BACKEND_PID_FILE"

echo "[dev] Subindo frontend em http://$DEV_HOST:$FRONTEND_PORT ..."
(
  cd frontend
  NEXT_PUBLIC_API_BASE_URL="http://$DEV_HOST:$BACKEND_PORT" \
    npm run dev -- --port "$FRONTEND_PORT" --hostname "$DEV_HOST" > "$LOG_DIR/frontend.log" 2>&1
) &
FRONTEND_PID=$!
echo "$FRONTEND_PID" > "$FRONTEND_PID_FILE"

sleep 3

if ! kill -0 "$BACKEND_PID" 2>/dev/null; then
  echo "[dev][erro] Backend encerrou ao iniciar. Veja: $LOG_DIR/backend.log"
  tail -n 80 "$LOG_DIR/backend.log" || true
  exit 1
fi

if ! kill -0 "$FRONTEND_PID" 2>/dev/null; then
  echo "[dev][erro] Frontend encerrou ao iniciar. Veja: $LOG_DIR/frontend.log"
  tail -n 80 "$LOG_DIR/frontend.log" || true
  exit 1
fi

echo "[dev] Backend PID:  $BACKEND_PID"
echo "[dev] Frontend PID: $FRONTEND_PID"
echo "[dev] Logs:"
echo "  - $LOG_DIR/backend.log"
echo "  - $LOG_DIR/frontend.log"
echo "[dev] URLs:"
echo "  - Backend:  http://$DEV_HOST:$BACKEND_PORT/health"
echo "  - Swagger:  http://$DEV_HOST:$BACKEND_PORT/docs"
echo "  - Frontend: http://$DEV_HOST:$FRONTEND_PORT"
echo
echo "[dev] Para encerrar use: ./scripts/dev/dev-stop.sh"
