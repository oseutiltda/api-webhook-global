# Guia Rápido - Teste de Webhook CT-e (Produção)

## 1) Variáveis (ajuste antes de testar)

```bash
export BASE_URL="https://SEU_DOMINIO_API"
export API_TOKEN="SEU_CTE_FIXED_TOKEN_OU_API_FIXED_TOKEN"
export WEBHOOK_SECRET="SEU_WEBHOOK_SECRET"
```

## 2) Health check

```bash
curl -sS "$BASE_URL/api/health"
```

## 3) Teste CT-e via rota com token fixo (recomendado)

Endpoint:

- `POST /api/CTe/InserirCte?token=...`

```bash
curl -sS -X POST "$BASE_URL/api/CTe/InserirCte?token=$API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "id": 9901001,
    "authorization_number": 7001001,
    "status": "AUTORIZADO",
    "xml": "<cte><id>9901001</id><status>AUTORIZADO</status></cte>",
    "event_xml": null
  }'
```

Resposta esperada:

- `201` (novo) ou `200` (atualização)
- body com `"Status": true`

## 4) Teste webhook CT-e autorizado (fluxo com segredo)

Endpoint:

- `POST /webhooks/cte/autorizado`

Obs.: essa rota exige header `x-webhook-secret` e idempotência (`id` no body ou `x-event-id`).

```bash
curl -sS -X POST "$BASE_URL/webhooks/cte/autorizado" \
  -H "Content-Type: application/json" \
  -H "x-webhook-secret: $WEBHOOK_SECRET" \
  -H "x-event-id: evt-cte-aut-001" \
  -d '{
    "id": 9901002,
    "authorization_number": 7001002,
    "status": "AUTORIZADO",
    "xml": "<cte><id>9901002</id><status>AUTORIZADO</status></cte>",
    "event_xml": null
  }'
```

## 5) Teste webhook CT-e cancelado (fluxo com segredo)

Endpoint:

- `POST /webhooks/cte/cancelado`

```bash
curl -sS -X POST "$BASE_URL/webhooks/cte/cancelado" \
  -H "Content-Type: application/json" \
  -H "x-webhook-secret: $WEBHOOK_SECRET" \
  -H "x-event-id: evt-cte-can-001" \
  -d '{
    "id": 9901003,
    "authorization_number": 7001003,
    "status": "CANCELADO",
    "xml": "<cte><id>9901003</id><status>CANCELADO</status></cte>",
    "event_xml": "<evento><tipo>CANCELAMENTO</tipo></evento>"
  }'
```

## 6) Validar se o frontend vai enxergar

As telas do dashboard consomem `WebhookEvent`:

```bash
curl -sS "$BASE_URL/api/worker/stats"
curl -sS "$BASE_URL/api/worker/events?limit=20&page=1"
```

Se esses endpoints retornarem eventos de CT-e, o frontend deve refletir em:

- `/`
- `/worker`
- `/worker1`
