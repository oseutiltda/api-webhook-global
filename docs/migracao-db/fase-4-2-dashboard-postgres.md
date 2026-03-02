# Fase 4.2 - Dashboard PostgreSQL

Data: 2026-03-02
Status: Concluida

## Objetivo

Remover dependencias de SQL Server no modulo de dashboard para operar com PostgreSQL sem erro de sintaxe.

## Alteracoes realizadas

Arquivo:

- `backend/src/routes/dashboard.ts`

Mudancas principais:

- removidas queries raw com:
  - `GETDATE`, `DATEADD`
  - `DATEPART`, `CAST(receivedAt AS DATE)`
  - `ISJSON`, `JSON_VALUE`
  - `dbo.WebhookEvent`
- rota `/api/worker/stats`:
  - calculo de `uniqueRecords` migrado para agregacao em TypeScript (parse seguro de `metadata` JSON).
- rota `/api/worker/productivity`:
  - agregacoes por periodo/tipo/tipoIntegracao migradas para Prisma + agregacao em memoria.
  - mantido contrato de resposta da API com contadores e estruturas existentes.
- padronizacao de tratamento `P2021`:
  - helper `isMissingWebhookEventTable` para funcionar sem acoplamento ao nome `dbo.WebhookEvent`.

## Validacoes

- `backend`: `npm run typecheck` -> OK
- `backend`: `npm run lint` -> OK (sem erros; warnings preexistentes de debito tecnico global)
- runtime no container backend:
  - `GET /api/worker/stats` -> `200` com payload valido
  - `GET /api/worker/productivity?period=mensal` -> `200` com payload valido

## Evidencias

- respostas observadas:
  - `/api/worker/stats` retornando estrutura zerada valida (dashboard vazia)
  - `/api/worker/productivity?period=mensal` retornando `summary` e listas vazias sem erro

## Proximo passo

- `F4.3-remover-sqlserver-remanescente`:
  - eliminar checks `dbo.WebhookEvent` remanescentes no worker
  - mapear e priorizar `TOP 1`/`dbo.*` ativos por dominio (Pessoa/CIOT/Contas/NFSe/CTe)
  - preparar backlog executavel da Fase 5 por lotes menores
