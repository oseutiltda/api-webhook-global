#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:3000}"
API_FIXED_TOKEN="${API_FIXED_TOKEN:-}"
WEBHOOK_SECRET="${WEBHOOK_SECRET:-}"

if [[ -z "$API_FIXED_TOKEN" || -z "$WEBHOOK_SECRET" ]]; then
  echo "ERRO: defina API_FIXED_TOKEN e WEBHOOK_SECRET."
  echo 'Exemplo: API_FIXED_TOKEN="..." WEBHOOK_SECRET="..." BASE_URL="http://localhost:3000" ./smoke-contas-pagar.sh'
  exit 1
fi

RUN_ID="$(date +%s)"
API_ID="$((9910000 + (RUN_ID % 100000)))"
WEBHOOK_ID="$((9920000 + (RUN_ID % 100000)))"
EVENT_ID="evt-cp-criar-${WEBHOOK_ID}-${RUN_ID}"

API_DOCUMENT="CP-TESTE-${API_ID}"
WEBHOOK_DOCUMENT="CP-WEBHOOK-${WEBHOOK_ID}"

TMP_API_JSON="/tmp/cp-api-${RUN_ID}.json"
TMP_WEBHOOK_JSON="/tmp/cp-webhook-${RUN_ID}.json"

cleanup() {
  rm -f "$TMP_API_JSON" "$TMP_WEBHOOK_JSON" /tmp/cp_smoke_body.txt /tmp/cp_smoke_hdr.txt
}
trap cleanup EXIT

cat > "$TMP_API_JSON" <<EOF
{
  "installment_count": 1,
  "data": {
    "id": ${API_ID},
    "type": "Accounting::Debit::SupplierBilling",
    "document": "${API_DOCUMENT}",
    "issue_date": "2026-03-03",
    "due_date": "2026-03-20",
    "value": 1500.75,
    "comments": "Teste contas a pagar API",
    "corporation": {
      "id": 1,
      "person_id": 1001,
      "nickname": "Filial Teste",
      "cnpj": "12345678000199"
    },
    "receiver": {
      "id": 2001,
      "name": "Fornecedor Teste",
      "type": "Supplier",
      "cnpj": "99887766000155"
    },
    "installments": [
      {
        "id": 1,
        "position": 1,
        "due_date": "2026-03-20",
        "value": 1500.75
      }
    ]
  }
}
EOF

cat > "$TMP_WEBHOOK_JSON" <<EOF
{
  "data": {
    "id": ${WEBHOOK_ID},
    "type": "Accounting::Debit::SupplierBilling",
    "document": "${WEBHOOK_DOCUMENT}",
    "issue_date": "2026-03-03",
    "due_date": "2026-03-25",
    "value": "890.10",
    "comments": "Teste webhook CP",
    "corporation": {
      "id": 1,
      "person_id": 1001,
      "nickname": "Filial Teste",
      "cnpj": "12345678000199"
    },
    "receiver": {
      "id": 2001,
      "name": "Fornecedor Teste",
      "type": "Supplier",
      "cnpj": "99887766000155"
    },
    "installments": [
      {
        "id": 10,
        "position": 1,
        "due_date": "2026-03-25",
        "value": "890.10"
      }
    ]
  }
}
EOF

pass=0
fail=0

run_test() {
  local name="$1"
  local expected="$2"
  shift 2

  local code
  code="$(curl -sS --connect-timeout 5 --max-time 20 -o /tmp/cp_smoke_body.txt -D /tmp/cp_smoke_hdr.txt -w "%{http_code}" "$@")"

  if [[ "$code" =~ ^($expected)$ ]]; then
    echo "[PASS] $name -> HTTP $code"
    pass=$((pass + 1))
  else
    echo "[FAIL] $name -> HTTP $code (esperado: $expected)"
    echo "----- response body -----"
    cat /tmp/cp_smoke_body.txt
    echo
    echo "----- response headers -----"
    cat /tmp/cp_smoke_hdr.txt
    echo
    fail=$((fail + 1))
  fi
}

echo "BASE_URL=$BASE_URL"
echo "API_ID=$API_ID WEBHOOK_ID=$WEBHOOK_ID EVENT_ID=$EVENT_ID"
echo "Iniciando smoke de Contas a Pagar..."

run_test \
  "POST /api/ContasPagar/InserirContasPagar" \
  "202" \
  -X POST "$BASE_URL/api/ContasPagar/InserirContasPagar?token=$API_FIXED_TOKEN" \
  -H "Content-Type: application/json" \
  --data-binary "@$TMP_API_JSON"

run_test \
  "POST /webhooks/faturas/pagar/criar" \
  "202" \
  -X POST "$BASE_URL/webhooks/faturas/pagar/criar" \
  -H "Content-Type: application/json" \
  -H "x-webhook-secret: $WEBHOOK_SECRET" \
  -H "x-event-id: $EVENT_ID" \
  --data-binary "@$TMP_WEBHOOK_JSON"

run_test \
  "POST /webhooks/faturas/pagar/criar (duplicate)" \
  "200" \
  -X POST "$BASE_URL/webhooks/faturas/pagar/criar" \
  -H "Content-Type: application/json" \
  -H "x-webhook-secret: $WEBHOOK_SECRET" \
  -H "x-event-id: $EVENT_ID" \
  --data-binary "@$TMP_WEBHOOK_JSON"

echo
echo "Resumo: PASS=$pass FAIL=$fail"
echo "Documentos gerados: API=${API_DOCUMENT}, WEBHOOK=${WEBHOOK_DOCUMENT}"

if [[ "$fail" -gt 0 ]]; then
  echo "Smoke com falhas."
  exit 2
fi

echo "Smoke OK."
