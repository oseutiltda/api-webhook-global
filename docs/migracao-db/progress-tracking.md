# Progress Tracking - Migracao DB

## Como usar

- Este arquivo e a fonte oficial de retomada.
- Sempre atualizar ao finalizar qualquer etapa relevante.
- Objetivo: saber exatamente onde parou e qual o proximo passo.

## Estado atual

- Data da ultima atualizacao: 2026-03-02
- Fase atual: Fase 5 (migracao por dominio) - etapa 5.2 em andamento
- Status geral: em andamento

## Ultimo checkpoint concluido

- Checkpoint: `F5.2.8-modo-recebimento-sem-worker`
- Resultado:
  - dominio Pessoa com sub-lotes 5.1.1/5.1.2/5.1.3 aplicados (bypass + leituras locais + gravacao minima)
  - iniciado hardening de CTe no worker com guard explicito para bloquear fluxo SQL Server legado em PostgreSQL
  - leituras de CTe em `cteSync` migradas para Prisma no modo PostgreSQL (pendentes/cancelados)
  - marcacoes de `processed` em `cteSync` migradas para Prisma no modo PostgreSQL
  - `cteIntegration` protegido com bypass global no modo PostgreSQL sem legado:
    - `inserirContasReceberCTe` marca `processed` localmente e nao executa integracao Senior
    - `alterarXMLProcessado` usa `prisma.cte.updateMany` no modo PostgreSQL
    - `cancelarCte` e `verificarCteExistente` retornam fallback controlado com log
  - `cteSync` consolidado para PostgreSQL-first:
    - ao processar CT-e com bypass ativo, nao consulta tabelas Senior para verificacao
    - atualiza `WebhookEvent` como integrado em modo local (`postgres_local_sem_legacy`)
    - evita mensagens e validacoes de GTCCONCE/GTCCONHE/GTCCONSF no caminho PostgreSQL
  - orquestracao do worker ajustada para priorizar CTe em PostgreSQL sem legado:
    - removido bloqueio global que impedia execucao dos processadores adaptados
    - `processPendingCte` e `processPendingCteCancelados` liberados para executar no modo local
  - fluxo completo de CTe validado para testes:
    - ingestao via `/api/CTe/InserirCte`
    - persistencia em `ctes`
    - processamento pelo worker com `processed=true`
    - eventos `worker/cte` com `integrationStatus=integrated`
  - aplicado modo operacional temporario somente recebimento:
    - `ENABLE_WORKER=false` no `.env`
    - servico `worker` parado via `docker compose stop worker`
    - backend segue recebendo CTe normalmente, mantendo `processed=false` em staging
  - Swagger integrado no backend:
    - endpoint de especificacao OpenAPI em `/docs.json`
    - interface Swagger UI em `/docs`
    - CSP ajustada na rota `/docs` para permitir carregamento dos assets do Swagger UI
- Evidencias:
  - `docs/migracao-db/fase-5-1-pessoa-primeiro-lote.md`
  - `docs/migracao-db/fase-5-2-cte-primeiro-lote.md`
  - `docs/migracao-db/fase-5-3-ciot-primeiro-lote.md`
  - `docs/migracao-db/guia-fluxo-cte-inicial.md`
  - `docs/migracao-db/guia-swagger-api.md`
  - `backend`: `http://localhost:3000/docs` -> 200
  - `backend`: `http://localhost:3000/docs.json` -> 200
  - `worker`: `./worker/node_modules/.bin/prettier --write worker/src/index.ts worker/src/services/cteSync.ts` OK
  - `worker`: `./worker/node_modules/.bin/prettier --write worker/src/services/cteIntegration.ts worker/src/services/cteSync.ts` OK
  - `worker`: `./worker/node_modules/.bin/prettier --write worker/src/services/ciotSync.ts` OK
  - `worker`: `npm run typecheck` OK
  - `worker`: `npm run lint` OK (sem erros)
  - `docker compose ps`: backend/postgres ativos e worker parado
  - insercao teste `external_id=910001` recebida e persistida com `processed=false`

## Proximo checkpoint

- Checkpoint alvo: `F5.2.9-reativar-worker-controlado-quando-autorizado`
- Objetivo:
  - manter ambiente em modo somente recebimento ate autorizacao de reativacao do worker
  - preservar backlog de CTe pendente para processamento controlado posterior
- Criterio de aceite:
  - backend segue recebendo webhooks sem impactar processamento em background
  - estado operacional documentado para retomada segura

## Checkpoints concluídos (historico resumido)

- `F0-inventario-sql-raw` (concluido)
- `F1-baseline-postgres-local` (concluido)
- `F2-prisma-schema-postgresql` (concluido)
- `F3-migrate-generate-backend` (concluido parcial com delta worker mapeado)
- `F4.1-worker-gate-legacy-postgres` (concluido)
- `F4.2-dashboard-postgres` (concluido)
- `F4.3-remover-sqlserver-remanescente` (concluido)
- `F5.1-pessoa-primeiro-lote` (sub-lotes 5.1.1, 5.1.2 e 5.1.3 aplicados)
- `F5.2-cte-primeiro-lote` (sub-lotes 5.2.1, 5.2.2, 5.2.3-parcial, 5.2.4 e 5.2.5-parcial aplicados)
- `F5.3-ciot-primeiro-lote` (sub-lote 5.3.1 aplicado)

## Bloqueios abertos

- Divergencia de schema Prisma entre backend e worker no mesmo banco (ownership de migrations).
- Debito tecnico de lint/format preexistente em varios modulos.
