# Fase 5.3 - CIOT (Primeiro Lote)

Data: 2026-03-02
Status: Em andamento (sub-lote 5.3.1 aplicado)

## Objetivo do lote

Aplicar hardening inicial no dominio CIOT para impedir execucao de SQL Server legado no worker quando o ambiente estiver em PostgreSQL.

## Atualizacao - F5.3.1 concluido

Arquivo:

- `worker/src/services/ciotSync.ts`

Mudancas:

- adicionado gate de bypass:
  - `IS_POSTGRES` + `shouldBypassCiotLegacyFlow()`;
- `processPendingCiot`:
  - quando `DATABASE_URL` for PostgreSQL e `ENABLE_SQLSERVER_LEGACY=false`, o worker nao executa fluxo legado CIOT e registra log estruturado de desativacao.

Validacao:

- `worker`: `./worker/node_modules/.bin/prettier --write worker/src/services/ciotSync.ts` -> OK
- `worker`: `npm run typecheck` -> OK
- `worker`: `npm run lint` -> OK (sem erros; warnings tecnicos preexistentes)

## Proximo sublote

- `F5.3.2`: aplicar bypass equivalente em pontos de integracao CIOT no backend (`backend/src/services/ciotService.ts`) para manter consistencia do modo PostgreSQL sem legado.
