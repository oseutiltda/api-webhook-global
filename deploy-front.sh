#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

SERVICE="frontend"

echo "[deploy-front] Deploy do frontend iniciado..."

if ! command -v docker >/dev/null 2>&1; then
  echo "[deploy-front][erro] Docker nao encontrado no PATH."
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "[deploy-front][erro] Docker daemon indisponivel."
  exit 1
fi

if ! command -v git >/dev/null 2>&1; then
  echo "[deploy-front][erro] Git nao encontrado no PATH."
  exit 1
fi

echo "[deploy-front] docker compose down..."
docker compose down

echo "[deploy-front] git pull --ff-only..."
git pull --ff-only

echo "[deploy-front] docker compose build ${SERVICE}..."
docker compose build "$SERVICE"

echo "[deploy-front] docker compose up -d ${SERVICE}..."
docker compose up -d "$SERVICE"

echo "[deploy-front] logs (tail 100) do ${SERVICE}..."
docker compose logs --tail 100 -f "$SERVICE"
