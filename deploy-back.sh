#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

SERVICE="backend"

echo "[deploy-back] Deploy do backend iniciado..."

if ! command -v docker >/dev/null 2>&1; then
  echo "[deploy-back][erro] Docker nao encontrado no PATH."
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "[deploy-back][erro] Docker daemon indisponivel."
  exit 1
fi

if ! command -v git >/dev/null 2>&1; then
  echo "[deploy-back][erro] Git nao encontrado no PATH."
  exit 1
fi

echo "[deploy-back] docker compose down..."
docker compose down

echo "[deploy-back] git pull --ff-only..."
git pull --ff-only

echo "[deploy-back] docker compose build ${SERVICE}..."
docker compose build "$SERVICE"

echo "[deploy-back] docker compose up -d ${SERVICE}..."
docker compose up -d "$SERVICE"

echo "[deploy-back] logs (tail 100) do ${SERVICE}..."
docker compose logs --tail 100 -f "$SERVICE"
