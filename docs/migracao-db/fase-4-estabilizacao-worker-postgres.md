# Fase 4 - Estabilizacao Worker no PostgreSQL (gate legado)

Data: 2026-03-02
Status: Concluida (etapa 4.1)

## Objetivo

Eliminar erros recorrentes do worker ao executar SQL Server raw/procedures enquanto o ambiente principal usa PostgreSQL.

## Alteracoes realizadas

Arquivos:

- `worker/src/config/env.ts`
- `worker/src/index.ts`
- `.env`

### 1) Nova flag de controle

- Adicionada variavel `ENABLE_SQLSERVER_LEGACY` (boolean parseado de string).
- Default: `false`.

### 2) Gate no processamento legacy

- No `worker/src/index.ts`, quando:
  - `DATABASE_URL` for PostgreSQL, e
  - `ENABLE_SQLSERVER_LEGACY=false`

  entao as rotinas legadas (`processPendingNfse`, `processPendingCiot`, `processPendingCte`, `processPendingContas*`) sao puladas.

### 3) Configuracao ativa

- `.env` atualizado com:
  - `ENABLE_SQLSERVER_LEGACY=false`

## Validacoes

- `worker`: `npm run build` -> OK
- `prettier` aplicado nos arquivos alterados:
  - `worker/src/index.ts`
  - `worker/src/config/env.ts`
- Container worker rebuildado e iniciado com sucesso.
- Logs apos multiplos ciclos sem erros SQL Server.

## Observacao de qualidade (atualizada)

- Configuracao local de ESLint foi criada para backend e worker (`eslint.config.mjs`).
- Frontend ja possuia `eslint.config.mjs` e foi ajustado para modo de transicao (warnings para debito tecnico atual).
- Scripts padronizados adicionados em backend/worker/frontend:
  - `lint`
  - `typecheck`
  - `format`
  - `format:check`
- Base de Prettier adicionada na raiz:
  - `.prettierrc.json`
  - `.prettierignore`

Status atual da sequencia de qualidade:

- `typecheck`: executa nos 3 projetos.
- `eslint`: executa nos 3 projetos (ainda com warnings/erros de debito tecnico preexistente).
- `prettier`: executa com configuracao central.

## Proximo passo (atualizado)

- Fase 4.2 concluida em `docs/migracao-db/fase-4-2-dashboard-postgres.md`.
- Seguir para `F4.3-remover-sqlserver-remanescente` antes da Fase 5 por dominio.
