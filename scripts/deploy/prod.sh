#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

echo "[prod] Iniciando deploy local em modo produção via Docker..."

if ! command -v docker >/dev/null 2>&1; then
  echo "[prod][erro] Docker não encontrado no PATH."
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "[prod][erro] Docker daemon não está disponível."
  exit 1
fi

if [ ! -f ".env" ]; then
  echo "[prod][erro] Arquivo .env não encontrado na raiz do projeto."
  echo "[prod][dica] Copie .env.example para .env e ajuste as variáveis antes de continuar."
  exit 1
fi

echo "[prod] Derrubando stack anterior (se existir)..."
docker compose down --remove-orphans

echo "[prod] Subindo containers (postgres, backend, frontend, worker)..."
docker compose up -d --build

echo "[prod] Aguardando healthcheck do banco..."
for i in {1..30}; do
  STATUS="$(docker compose ps --format json 2>/dev/null | grep -E '"Service":"postgres".*"Health":"healthy"' || true)"
  if [ -n "$STATUS" ]; then
    break
  fi
  sleep 2
done

echo "[prod] Status dos containers:"
docker compose ps

echo "[prod] Endpoints esperados:"
echo "  - Backend:  http://localhost:3000/health"
echo "  - Swagger:  http://localhost:3000/docs"
echo "  - Frontend: http://localhost:3001"

echo "[prod] Deploy concluído."
