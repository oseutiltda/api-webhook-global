#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:3000}"
API_FIXED_TOKEN="${API_FIXED_TOKEN:-}"
WEBHOOK_SECRET="${WEBHOOK_SECRET:-}"

if [[ -z "$API_FIXED_TOKEN" || -z "$WEBHOOK_SECRET" ]]; then
  echo "ERRO: defina API_FIXED_TOKEN e WEBHOOK_SECRET."
  echo 'Exemplo: API_FIXED_TOKEN="..." WEBHOOK_SECRET="..." BASE_URL="http://localhost:3000" ./scripts/smoke/smoke-contas-receber-baixa.sh'
  exit 1
fi

RUN_ID="$(date +%s)"
FATURA_ID="$((9930000 + (RUN_ID % 100000)))"
INSTALLMENT_ID="$((9940000 + (RUN_ID % 100000)))"
BAIXA_EVENT_ID="evt-cr-baixa-${INSTALLMENT_ID}-${RUN_ID}"

FATURA_DOCUMENT="CR-TESTE-${FATURA_ID}"

TMP_RECEBER_JSON="/tmp/cr-receber-${RUN_ID}.json"
TMP_BAIXA_JSON="/tmp/cr-baixa-${RUN_ID}.json"

cleanup() {
  rm -f "$TMP_RECEBER_JSON" "$TMP_BAIXA_JSON" /tmp/cr_smoke_body.txt /tmp/cr_smoke_hdr.txt
}
trap cleanup EXIT

cat > "$TMP_RECEBER_JSON" <<EOF2
{
  "installment_count": 1,
  "data": {
    "id": ${FATURA_ID},
    "type": "Accounting::Credit::CustomerBilling",
    "document": "${FATURA_DOCUMENT}",
    "issue_date": "2026-03-03",
    "due_date": "2026-03-25",
    "value": 980.55,
    "comments": "Teste contas a receber para baixa",
    "corporation": {
      "id": 1,
      "person_id": 1001,
      "nickname": "Filial Teste",
      "cnpj": "12345678000199"
    },
    "customer": {
      "id": 3001,
      "name": "Cliente Teste",
      "type": "Customer",
      "cnpj": "11222333000144"
    },
    "installments": [
      {
        "id": ${INSTALLMENT_ID},
        "position": 1,
        "due_date": "2026-03-25",
        "value": 980.55
      }
    ],
    "invoice_items": [
      {
        "id": 1,
        "type": "CTE",
        "total": 980.55
      }
    ]
  }
}
EOF2

cat > "$TMP_BAIXA_JSON" <<EOF2
{
  "installment_id": ${INSTALLMENT_ID},
  "payment_date": "2026-03-26T10:30:00",
  "payment_value": 980.55,
  "discount_value": 0,
  "interest_value": 0,
  "payment_method": "PIX",
  "bank_account": "CC-12345",
  "bankname": "BANCO TESTE",
  "accountnumber": "12345",
  "accountdigit": "0",
  "comments": "Baixa de teste"
}
EOF2

pass=0
fail=0

run_test() {
  local name="$1"
  local expected="$2"
  shift 2

  local code
  code="$(curl -sS --connect-timeout 5 --max-time 20 -o /tmp/cr_smoke_body.txt -D /tmp/cr_smoke_hdr.txt -w "%{http_code}" "$@")"

  if [[ "$code" =~ ^($expected)$ ]]; then
    echo "[PASS] $name -> HTTP $code"
    pass=$((pass + 1))
  else
    echo "[FAIL] $name -> HTTP $code (esperado: $expected)"
    echo "----- response body -----"
    cat /tmp/cr_smoke_body.txt
    echo
    echo "----- response headers -----"
    cat /tmp/cr_smoke_hdr.txt
    echo
    fail=$((fail + 1))
  fi
}

echo "BASE_URL=$BASE_URL"
echo "FATURA_ID=$FATURA_ID INSTALLMENT_ID=$INSTALLMENT_ID EVENT_ID=$BAIXA_EVENT_ID"
echo "Iniciando smoke de Contas a Receber (Baixa)..."

run_test \
  "POST /api/ContasReceber/InserirContasReceber" \
  "202" \
  -X POST "$BASE_URL/api/ContasReceber/InserirContasReceber?token=$API_FIXED_TOKEN" \
  -H "Content-Type: application/json" \
  --data-binary "@$TMP_RECEBER_JSON"

run_test \
  "POST /api/ContasReceber/InserirContasReceberBaixa" \
  "202" \
  -X POST "$BASE_URL/api/ContasReceber/InserirContasReceberBaixa?token=$API_FIXED_TOKEN" \
  -H "Content-Type: application/json" \
  --data-binary "@$TMP_BAIXA_JSON"

run_test \
  "POST /webhooks/faturas/receber/baixar" \
  "202" \
  -X POST "$BASE_URL/webhooks/faturas/receber/baixar" \
  -H "Content-Type: application/json" \
  -H "x-webhook-secret: $WEBHOOK_SECRET" \
  -H "x-event-id: $BAIXA_EVENT_ID" \
  --data-binary "@$TMP_BAIXA_JSON"

run_test \
  "POST /webhooks/faturas/receber/baixar (duplicate)" \
  "200" \
  -X POST "$BASE_URL/webhooks/faturas/receber/baixar" \
  -H "Content-Type: application/json" \
  -H "x-webhook-secret: $WEBHOOK_SECRET" \
  -H "x-event-id: $BAIXA_EVENT_ID" \
  --data-binary "@$TMP_BAIXA_JSON"

echo
echo "Resumo: PASS=$pass FAIL=$fail"
echo "Documento gerado: ${FATURA_DOCUMENT}"

echo "IDs usados: fatura=${FATURA_ID}, installment=${INSTALLMENT_ID}"

if [[ "$fail" -gt 0 ]]; then
  echo "Smoke com falhas."
  exit 2
fi

echo "Smoke OK."
