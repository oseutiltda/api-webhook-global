# Fase 5 - Backlog Priorizado por Dominio

Data: 2026-03-02
Status: Planejado e priorizado

## Objetivo

Transformar a migracao de SQL Server para PostgreSQL em lotes pequenos, com ordem estrategica de execucao.

## Base do diagnostico

Padrao pesquisado:

- `TOP 1`
- `dbo.`
- `[dbo]`
- `EXEC dbo.`
- `GETDATE()`
- `DATEADD()`
- `ISJSON/JSON_VALUE`

Resultado consolidado em `backend/src/services` + `worker/src/services`:

- total de ocorrencias: `355`

Recorte dos principais arquivos-alvo:

- `backend/src/services/pessoaService.ts`: `77`
- `worker/src/services/nfseSync.ts`: `64`
- `backend/src/services/ciotService.ts`: `33`
- `worker/src/services/ciotSync.ts`: `32`
- `backend/src/services/contasReceberService.ts`: `18`
- `backend/src/services/contasPagarService.ts`: `17`
- `worker/src/services/contasPagarSync.ts`: `12`
- `worker/src/services/contasReceberBaixaSync.ts`: `11`
- `worker/src/services/contasReceberSync.ts`: `10`
- `worker/src/services/cteSync.ts`: `3`

## Ordem de execucao recomendada (Fase 5)

1. Pessoa (backend) - alto volume e dominio base para cadastros
2. CTe (worker/backend) - menor volume, bom para consolidar padrao de migracao
3. NFSe (worker) - alto volume, forte uso de SQL Server raw
4. CIOT (backend/worker) - volume medio/alto com varias procedures
5. Contas a Pagar (backend/worker)
6. Contas a Receber (backend/worker)

## Lotes tecnicos por dominio

### Pessoa - Lote 5.1

- alvo principal: `backend/src/services/pessoaService.ts`
- foco:
  - substituir `SELECT TOP 1` por Prisma/`LIMIT 1`
  - isolar/flaggear `EXEC dbo.*` de integracao externa
  - remover dependencias de `[AFS_INTEGRADOR].[dbo].*` do caminho principal
- criterio de aceite:
  - sem SQL Server no caminho principal de insercao/atualizacao de Pessoa
  - fallback controlado por flag quando integracao externa estiver desligada

### CTe - Lote 5.2

- alvos:
  - `worker/src/services/cteSync.ts`
  - `worker/src/services/cteIntegration.ts`
- foco:
  - retirar `EXEC dbo.*` e leituras com `TOP 1` dos fluxos ativos no PostgreSQL
  - manter modo seguro com flags quando depender de Senior

### NFSe - Lote 5.3

- alvo principal: `worker/src/services/nfseSync.ts`
- foco:
  - remover uso de `[db].dbo.*`, `GETDATE()`, `TOP 1`, hints SQL Server
  - separar claramente staging local PostgreSQL x integracao externa

### CIOT - Lote 5.4

- alvos:
  - `backend/src/services/ciotService.ts`
  - `worker/src/services/ciotSync.ts`
  - `worker/src/services/ciotIntegration.ts`
- foco:
  - migrar operacoes diretas em `dbo.manifests*`
  - neutralizar procedures com fallback por flag

### Contas - Lote 5.5

- alvos:
  - `backend/src/services/contasPagarService.ts`
  - `backend/src/services/contasReceberService.ts`
  - `backend/src/services/contasReceberBaixaService.ts`
  - `worker/src/services/contasPagarSync.ts`
  - `worker/src/services/contasReceberSync.ts`
  - `worker/src/services/contasReceberBaixaSync.ts`
- foco:
  - migrar comandos SQL Server de staging
  - preservar comportamento com flags e logs de bypass

## Regra de execucao para cada lote

1. Refatorar somente um dominio por vez.
2. Rodar `eslint`, `typecheck` e `prettier`.
3. Atualizar `docs/migracao-db/progress-tracking.md`.
4. Registrar evidencias em arquivo da fase/lote.
