#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:3000}"
WEBHOOK_SECRET="${WEBHOOK_SECRET:-}"
API_FIXED_TOKEN="${API_FIXED_TOKEN:-}"

if [[ -z "$WEBHOOK_SECRET" || -z "$API_FIXED_TOKEN" ]]; then
  echo "ERRO: defina WEBHOOK_SECRET e API_FIXED_TOKEN antes de rodar."
  echo 'Exemplo: WEBHOOK_SECRET="..." API_FIXED_TOKEN="..." BASE_URL="http://localhost:3000" ./smoke-prod.sh'
  exit 1
fi

pass=0
fail=0

run_test() {
  local name="$1"
  local expected_regex="$2"
  local method="$3"
  local url="$4"
  local data="${5:-}"
  shift 5 || true
  local headers=("$@")

  local tmp_body tmp_hdr code
  tmp_body="$(mktemp)"
  tmp_hdr="$(mktemp)"

  if [[ "$method" == "GET" ]]; then
    code="$(curl -sS --connect-timeout 5 --max-time 20 -o "$tmp_body" -D "$tmp_hdr" -w "%{http_code}" "$url")"
  else
    code="$(curl -sS --connect-timeout 5 --max-time 20 -o "$tmp_body" -D "$tmp_hdr" -w "%{http_code}" -X "$method" "${headers[@]}" "$url" -d "$data")"
  fi

  if [[ "$code" =~ ^($expected_regex)$ ]]; then
    echo "[PASS] $name -> HTTP $code"
    pass=$((pass + 1))
  else
    echo "[FAIL] $name -> HTTP $code (esperado regex: $expected_regex)"
    echo "----- response body -----"
    cat "$tmp_body"
    echo
    echo "----- response headers -----"
    cat "$tmp_hdr"
    echo
    fail=$((fail + 1))
  fi

  rm -f "$tmp_body" "$tmp_hdr"
}

echo "BASE_URL=$BASE_URL"
echo "Iniciando smoke tests..."

# 1) Health simples (liveness)
run_test "GET /health" "200" "GET" "$BASE_URL/health" ""

# 2) Health completo (200 ou 503 quando worker/tabela não estão ativos)
run_test "GET /api/health" "200|503" "GET" "$BASE_URL/api/health" ""

# 3) Webhook CT-e autorizado (segredo)
run_test \
  "POST /webhooks/cte/autorizado" \
  "200|201|202|400|500" \
  "POST" \
  "$BASE_URL/webhooks/cte/autorizado" \
  '{"id":"evt-smoke-001","authorization_number":7001002,"status":"AUTORIZADO","xml":"<cte/>","event_xml":null}' \
  -H "Content-Type: application/json" \
  -H "x-webhook-secret: $WEBHOOK_SECRET" \
  -H "x-event-id: evt-smoke-001"

# 4) Idempotência webhook (mesmo x-event-id deve retornar 200)
run_test \
  "POST /webhooks/cte/autorizado (duplicate)" \
  "200" \
  "POST" \
  "$BASE_URL/webhooks/cte/autorizado" \
  '{"id":"evt-smoke-001","authorization_number":7001002,"status":"AUTORIZADO","xml":"<cte/>","event_xml":null}' \
  -H "Content-Type: application/json" \
  -H "x-webhook-secret: $WEBHOOK_SECRET" \
  -H "x-event-id: evt-smoke-001"

# 5) API CTe com token fixo
run_test \
  "POST /api/CTe/InserirCte?token=..." \
  "200|201|400|500" \
  "POST" \
  "$BASE_URL/api/CTe/InserirCte?token=$API_FIXED_TOKEN" \
  '{"id":9901003,"authorization_number":7001003,"status":"AUTORIZADO","xml":"<cte/>","event_xml":null}' \
  -H "Content-Type: application/json"

echo
echo "Resumo: PASS=$pass FAIL=$fail"

if [[ "$fail" -gt 0 ]]; then
  echo "Smoke com falhas."
  exit 2
fi

echo "Smoke OK."
