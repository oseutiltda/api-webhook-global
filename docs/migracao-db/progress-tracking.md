# Progress Tracking - Migracao DB

## Como usar

- Este arquivo e a fonte oficial de retomada.
- Sempre atualizar ao finalizar qualquer etapa relevante.
- Objetivo: saber exatamente onde parou e qual o proximo passo.

## Estado atual

- Data da ultima atualizacao: 2026-03-02
- Fase atual: Fase 5 (migracao por dominio) - etapa 5.2/5.3 em andamento
- Status geral: em andamento

## Ultimo checkpoint concluido

- Checkpoint: `F5.2.10-seed-admin-docker-e-validacao-front-back`
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
  - seed de admin adicionado no backend:
    - `backend/prisma/seed.js` cria/atualiza `admin_users` com upsert idempotente
    - credenciais vindas de `ADMIN_SEED_*` com fallback para `NEXT_PUBLIC_ADMIN1_*`
    - adicionado retry de conexao para evitar race de startup do Postgres
  - backend em Docker producao executa seed no startup:
    - `backend/Dockerfile` executa `node prisma/seed.js` antes de `node dist/server.js`
  - frontend Docker recebeu build args para credenciais de admin:
    - `NEXT_PUBLIC_ADMIN1_USER`, `NEXT_PUBLIC_ADMIN1_PASSWORD`
    - `NEXT_PUBLIC_ADMIN2_USER`, `NEXT_PUBLIC_ADMIN2_PASSWORD`
  - validacao de stack em Docker:
    - `postgres`, `backend` e `frontend` ativos
    - `worker` parado intencionalmente para manter modo somente recebimento (`ENABLE_WORKER=false`)
  - conectividade validada em rede Docker:
    - `bmx-frontend -> http://backend:3000/health` retornando `200`
    - frontend respondendo `200` internamente em `http://localhost:3000`
  - seed confirmado no banco:
    - `admin_users` contem `admin@admin` com role `ADMIN` e `is_active=true`
  - credencial de admin de testes padronizada para dev/producao docker:
    - `ADMIN_SEED_LOGIN=afs@afs`
    - `ADMIN_SEED_PASSWORD=afs123`
    - `NEXT_PUBLIC_ADMIN1_USER=afs@afs`
    - `NEXT_PUBLIC_ADMIN1_PASSWORD=afs123`
  - seed do backend ajustado para carregar `.env` da raiz ao executar em `backend/`:
    - `backend/prisma/seed.js` agora usa `dotenv.config()` + fallback `../../.env`
    - evita falha de ambiente ao rodar `npm run seed` dentro da pasta `backend`
  - login frontend recebeu fallback de contingencia para testes:
    - `frontend/src/components/LoginForm.tsx` inclui `afs@afs / afs123` na lista de credenciais aceitas
    - evita bloqueio por cache/env antigo do Next durante validacao local
  - diagnostico de credencial invalida no dev concluido:
    - origem: `frontend/.env.local` sobrescrevendo com `admin@admin`
    - correcao aplicada: `NEXT_PUBLIC_ADMIN1_USER=afs@afs` e `NEXT_PUBLIC_ADMIN1_PASSWORD=afs123`
    - acao operacional: manter frontend dev reiniciado sempre que `.env.local` mudar
  - seed dedicado de CT-e criado para validacao local:
    - arquivo `backend/prisma/seed-ctes.js`
    - script `npm run seed:ctes` adicionado no `backend/package.json`
    - comportamento idempotente: remove lote de teste (`external_id` 990001..990006) e reinsere 6 CT-es
  - seed de eventos CT-e (dashboard) criado:
    - arquivo `backend/prisma/seed-ctes-events.js`
    - script `npm run seed:ctes-events` adicionado no `backend/package.json`
    - comportamento idempotente: remove e reinsere 8 registros em `WebhookEvent` com fontes CT-e (`/webhooks/cte/*` e `/api/CTe/InserirCte`)
  - guia rápido de teste de CT-e em produção criado na raiz:
    - `GUIA-TESTE-WEBHOOK-CTE-PROD.md`
    - inclui exemplos `curl` para `/api/CTe/InserirCte`, `/webhooks/cte/autorizado` e `/webhooks/cte/cancelado`
  - hardening do ambiente dev local aplicado:
    - `dev.sh` agora valida portas antes de subir (`3000`/`3001`) e mostra dono da porta quando ocupada
    - `dev.sh` sobe backend/frontend com bind explicito em `127.0.0.1` por padrao (`DEV_HOST`)
    - `dev.sh` injeta `NEXT_PUBLIC_API_BASE_URL` coerente com backend local no startup do frontend
    - `dev.sh` faz fail-fast com diagnostico (tail de logs) se backend ou frontend cair na inicializacao
    - `dev.sh` encerra automaticamente processos orfaos do proprio projeto que estejam prendendo `3000/3001`
    - `dev.sh` ganhou limpeza adicional por comando (`ps`) para casos em que deteccao por porta nao encontra PID (ex.: Next órfão em 3001)
    - `dev.sh` agora tenta matar automaticamente listeners em `3000/3001` do mesmo usuario antes de abortar por porta ocupada
    - `backend/src/server.ts` atualizado para respeitar `HOST` (fallback `0.0.0.0`)
- Evidencias:
  - `docs/migracao-db/fase-5-1-pessoa-primeiro-lote.md`
  - `docs/migracao-db/fase-5-2-cte-primeiro-lote.md`
  - `docs/migracao-db/fase-5-3-ciot-primeiro-lote.md`
  - `docs/migracao-db/guia-fluxo-cte-inicial.md`
  - `docs/migracao-db/guia-swagger-api.md`
  - `backend/prisma/seed.js`
  - `backend/Dockerfile`
  - `frontend/Dockerfile`
  - `docker-compose.yml`
  - `backend`: `http://localhost:3000/docs` -> 200
  - `backend`: `http://localhost:3000/docs.json` -> 200
  - `worker`: `./worker/node_modules/.bin/prettier --write worker/src/index.ts worker/src/services/cteSync.ts` OK
  - `worker`: `./worker/node_modules/.bin/prettier --write worker/src/services/cteIntegration.ts worker/src/services/cteSync.ts` OK
  - `worker`: `./worker/node_modules/.bin/prettier --write worker/src/services/ciotSync.ts` OK
  - `worker`: `npm run typecheck` OK
  - `worker`: `npm run lint` OK (sem erros)
  - `docker compose ps -a`: backend/frontend/postgres ativos e worker parado (exit 0)
  - `docker exec bmx-frontend node -e "fetch('http://backend:3000/health')..."` -> `status=200`
  - `docker exec -i global-postgres psql -U global -d global_integrador -c "SELECT login, role, is_active FROM admin_users;"` -> `admin@admin | ADMIN | t`
  - insercao teste `external_id=910001` recebida e persistida com `processed=false`

## Proximo checkpoint

- Checkpoint alvo: `F5.3.2-ciot-backend-bypass-postgres`
- Objetivo:
  - manter CTe em modo somente recebimento para testes
  - avancar CIOT no backend para remover dependencia SQL Server no caminho principal
- Criterio de aceite:
  - endpoints/backlog de CIOT operam sem query SQL Server no modo PostgreSQL
  - logs de bypass/fallback padronizados para CIOT

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
