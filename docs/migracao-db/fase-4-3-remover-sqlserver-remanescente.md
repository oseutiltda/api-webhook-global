# Fase 4.3 - Remover SQL Server Remanescente

Data: 2026-03-02
Status: Concluida

## Objetivo

Reduzir acoplamentos remanescentes de SQL Server antes da migracao por dominio (Fase 5).

## Avanco aplicado neste lote

Arquivo:

- `worker/src/index.ts`

Alteracao:

- removido acoplamento exato a `dbo.WebhookEvent` nos checks de erro `P2021`;
- criado helper `isMissingWebhookEventTable` para detectar ausencia da tabela sem depender de schema SQL Server.

## Validacao

- `worker`: `npm run typecheck` -> OK
- `worker`: `npm run lint` -> OK (sem erros; warnings preexistentes de debito tecnico)

## Evidencia de fechamento do checkpoint

- busca em `worker/src` para `dbo.WebhookEvent` sem ocorrencias;
- backlog tecnico priorizado criado:
  - `docs/migracao-db/fase-5-backlog-priorizado.md`

## Proximos passos

- iniciar Fase 5 pelo Lote 5.1 (Pessoa), seguindo backlog em `docs/migracao-db/fase-5-backlog-priorizado.md`.
