#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:3000}"
API_FIXED_TOKEN="${API_FIXED_TOKEN:-}"

if [[ -z "$API_FIXED_TOKEN" ]]; then
  echo "ERRO: defina API_FIXED_TOKEN."
  echo 'Exemplo: API_FIXED_TOKEN="..." BASE_URL="http://localhost:3000" ./scripts/smoke/smoke-ciot.sh'
  exit 1
fi

RUN_ID="$(date +%s)"
SUFFIX="$((RUN_ID % 100000))"
NRCIOT="CIOT-TESTE-${SUFFIX}"
MANIFEST_EXTERNAL_ID="${RUN_ID}"

TMP_INSERT_JSON="/tmp/ciot-insert-${RUN_ID}.json"
TMP_UPDATE_JSON="/tmp/ciot-update-${RUN_ID}.json"
TMP_CANCEL_JSON="/tmp/ciot-cancel-${RUN_ID}.json"

cleanup() {
  rm -f "$TMP_INSERT_JSON" "$TMP_UPDATE_JSON" "$TMP_CANCEL_JSON" /tmp/ciot_smoke_body.txt /tmp/ciot_smoke_hdr.txt
}
trap cleanup EXIT

cat > "$TMP_INSERT_JSON" <<EOF_INSERT
{
  "cancelado": 0,
  "Manifest": {
    "id": "${MANIFEST_EXTERNAL_ID}",
    "nrciot": "${NRCIOT}",
    "cdempresa": "300",
    "cdcartafrete": "CF-${SUFFIX}",
    "nrcgccpfprop": "12345678000199",
    "nrcgccpfmot": "99887766000155",
    "dtemissao": "2026-03-03",
    "nrmanifesto": "MNF-${SUFFIX}",
    "nrplaca": "ABC1D23",
    "nrceporigem": "79002000",
    "nrcepdestino": "01001000",
    "insituacao": 0,
    "cdcondicaovencto": 0,
    "cdremetente": "REM-${SUFFIX}",
    "cddestinatario": "DES-${SUFFIX}",
    "cdnaturezacarga": "NAT-${SUFFIX}",
    "cdespeciecarga": "ESP-${SUFFIX}",
    "nrnotafiscal": "NF-${SUFFIX}",
    "dsusuarioinc": "api",
    "dtinclusao": "2026-03-03",
    "intipoorigem": "API",
    "nrplacareboque1": "REB1D23",
    "cdtarifa": 0,
    "vlfrete": 1000,
    "inveiculoproprio": 0,
    "dtprazomaxentrega": "2026-03-05",
    "serie": 1,
    "inoperacaodistribuicao": 0,
    "inveiculo": 0,
    "cdrota": "01",
    "inoperadorapagtoctrb": "N",
    "inrespostaquesttacagreg": 0,
    "vlmanifesto": 1000,
    "vlcombustivel": 0,
    "vlpedagio": 0,
    "vlnotacreditodebito": 0,
    "vldesconto": 0,
    "vlcsll": 0,
    "vlpis": 0,
    "vlirff": 0,
    "vlinss": 0,
    "vltotalmanifesto": 1000,
    "vlabastecimento": 0,
    "vladiantamento": 0,
    "vlir": 0,
    "vlsaldoapagar": 1000,
    "vlsaldofrete": 1000,
    "vlcofins": 0,
    "vlsestsenat": 0,
    "vliss": 0,
    "vlcsl": 0,
    "parcelas": [
      {
        "ID": "PARC-${SUFFIX}-1",
        "idparcela": "1",
        "nrciotsistema": "SYS-${SUFFIX}",
        "nrciot": "${NRCIOT}",
        "dstipo": "SALDO",
        "dsstatus": "PENDENTE",
        "cdfavorecido": "FAV-${SUFFIX}",
        "cdcartafrete": "CF-${SUFFIX}",
        "cdevento": "EVT",
        "dtpagto": "2026-03-20",
        "dtinclusao": "2026-03-03",
        "hrinclusao": "12:00:00",
        "dsusuarioinc": "api",
        "dtreferenciacalculo": "2026-03-03"
      }
    ],
    "dadosFaturamento": {
      "ID": "FAT-${SUFFIX}",
      "cdempresa": "300",
      "cdcartafrete": "CF-${SUFFIX}",
      "cdempresaFV": "300",
      "nrficha": "FCH-${SUFFIX}"
    }
  }
}
EOF_INSERT

cat > "$TMP_UPDATE_JSON" <<EOF_UPDATE
{
  "cancelado": 0,
  "Manifest": {
    "id": "${MANIFEST_EXTERNAL_ID}",
    "nrciot": "${NRCIOT}",
    "cdempresa": "300",
    "cdcartafrete": "CF-${SUFFIX}",
    "nrcgccpfprop": "12345678000199",
    "nrcgccpfmot": "99887766000155",
    "dtemissao": "2026-03-03",
    "nrmanifesto": "MNF-${SUFFIX}",
    "nrplaca": "ABC1D23",
    "nrceporigem": "79002000",
    "nrcepdestino": "01001000",
    "insituacao": 0,
    "cdcondicaovencto": 0,
    "cdremetente": "REM-${SUFFIX}",
    "cddestinatario": "DES-${SUFFIX}",
    "cdnaturezacarga": "NAT-${SUFFIX}",
    "cdespeciecarga": "ESP-${SUFFIX}",
    "nrnotafiscal": "NF-${SUFFIX}",
    "dsusuarioinc": "api",
    "dtinclusao": "2026-03-03",
    "intipoorigem": "API",
    "nrplacareboque1": "REB1D23",
    "cdtarifa": 0,
    "vlfrete": 1200,
    "inveiculoproprio": 0,
    "dtprazomaxentrega": "2026-03-05",
    "serie": 1,
    "inoperacaodistribuicao": 0,
    "inveiculo": 0,
    "cdrota": "01",
    "inoperadorapagtoctrb": "N",
    "inrespostaquesttacagreg": 0,
    "vlmanifesto": 1200,
    "vlcombustivel": 0,
    "vlpedagio": 0,
    "vlnotacreditodebito": 0,
    "vldesconto": 0,
    "vlcsll": 0,
    "vlpis": 0,
    "vlirff": 0,
    "vlinss": 0,
    "vltotalmanifesto": 1200,
    "vlabastecimento": 0,
    "vladiantamento": 0,
    "vlir": 0,
    "vlsaldoapagar": 1200,
    "vlsaldofrete": 1200,
    "vlcofins": 0,
    "vlsestsenat": 0,
    "vliss": 0,
    "vlcsl": 0,
    "parcelas": [
      {
        "ID": "PARC-${SUFFIX}-1",
        "idparcela": "1",
        "nrciotsistema": "SYS-${SUFFIX}",
        "nrciot": "${NRCIOT}",
        "dstipo": "SALDO",
        "dsstatus": "ATUALIZADO",
        "cdfavorecido": "FAV-${SUFFIX}",
        "cdcartafrete": "CF-${SUFFIX}",
        "cdevento": "EVT",
        "dtpagto": "2026-03-20",
        "dtinclusao": "2026-03-03",
        "hrinclusao": "12:00:00",
        "dsusuarioinc": "api",
        "dtreferenciacalculo": "2026-03-03"
      }
    ],
    "dadosFaturamento": {
      "ID": "FAT-${SUFFIX}",
      "cdempresa": "300",
      "cdcartafrete": "CF-${SUFFIX}",
      "cdempresaFV": "300",
      "nrficha": "FCH-${SUFFIX}-UPD"
    }
  }
}
EOF_UPDATE

cat > "$TMP_CANCEL_JSON" <<EOF_CANCEL
{
  "Manifest": {
    "nrciot": "${NRCIOT}"
  },
  "Obscancelado": "Cancelamento de teste smoke",
  "DsUsuarioCan": "smoke"
}
EOF_CANCEL

pass=0
fail=0

run_test() {
  local name="$1"
  local expected="$2"
  shift 2

  local code
  code="$(curl -sS --connect-timeout 5 --max-time 20 -o /tmp/ciot_smoke_body.txt -D /tmp/ciot_smoke_hdr.txt -w "%{http_code}" "$@")"

  if [[ "$code" =~ ^($expected)$ ]]; then
    echo "[PASS] $name -> HTTP $code"
    pass=$((pass + 1))
  else
    echo "[FAIL] $name -> HTTP $code (esperado: $expected)"
    echo "----- response body -----"
    cat /tmp/ciot_smoke_body.txt
    echo
    echo "----- response headers -----"
    cat /tmp/ciot_smoke_hdr.txt
    echo
    fail=$((fail + 1))
  fi
}

echo "BASE_URL=$BASE_URL"
echo "NRCIOT=$NRCIOT MANIFEST_EXTERNAL_ID=$MANIFEST_EXTERNAL_ID"
echo "Iniciando smoke de CIOT..."

run_test \
  "POST /api/CIOT/InserirContasPagarCIOT (create)" \
  "200|201" \
  -X POST "$BASE_URL/api/CIOT/InserirContasPagarCIOT?token=$API_FIXED_TOKEN" \
  -H "Content-Type: application/json" \
  --data-binary "@$TMP_INSERT_JSON"

run_test \
  "POST /api/CIOT/InserirContasPagarCIOT (update)" \
  "200" \
  -X POST "$BASE_URL/api/CIOT/InserirContasPagarCIOT?token=$API_FIXED_TOKEN" \
  -H "Content-Type: application/json" \
  --data-binary "@$TMP_UPDATE_JSON"

run_test \
  "POST /api/CIOT/CancelarContasPagarCIOT" \
  "200" \
  -X POST "$BASE_URL/api/CIOT/CancelarContasPagarCIOT?token=$API_FIXED_TOKEN" \
  -H "Content-Type: application/json" \
  --data-binary "@$TMP_CANCEL_JSON"

echo
echo "Resumo: PASS=$pass FAIL=$fail"

if [[ "$fail" -gt 0 ]]; then
  echo "Smoke com falhas."
  exit 2
fi

echo "Smoke OK."
