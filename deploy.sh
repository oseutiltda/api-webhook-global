#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

echo "[deploy] Deploy completo iniciado..."

if ! command -v docker >/dev/null 2>&1; then
  echo "[deploy][erro] Docker nao encontrado no PATH."
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "[deploy][erro] Docker daemon indisponivel."
  exit 1
fi

if ! command -v git >/dev/null 2>&1; then
  echo "[deploy][erro] Git nao encontrado no PATH."
  exit 1
fi

echo "[deploy] docker compose down..."
docker compose down

echo "[deploy] git pull --ff-only..."
git pull --ff-only

echo "[deploy] docker compose build..."
docker compose build

echo "[deploy] docker compose up -d..."
docker compose up -d

echo "[deploy] logs (tail 100) da stack..."
docker compose logs --tail 100 -f
