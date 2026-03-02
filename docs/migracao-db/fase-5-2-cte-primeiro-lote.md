# Fase 5.2 - CTe (Primeiro Lote)

Data: 2026-03-02
Status: Em andamento (sub-lotes 5.2.1, 5.2.2, 5.2.3-parcial, 5.2.4, 5.2.5-parcial e 5.2.6 aplicados)

## Objetivo do lote

Aplicar hardening no dominio CTe para evitar execucao de SQL Server legado no worker quando o ambiente estiver em PostgreSQL.

## Alteracoes aplicadas

Arquivo:

- `worker/src/services/cteSync.ts`

Mudancas:

- adicionado guard explicito:
  - `shouldBypassCteLegacyFlow()`
  - condicao: `DATABASE_URL` PostgreSQL e `ENABLE_SQLSERVER_LEGACY=false`
- aplicado bypass nos fluxos:
  - `processPendingCte`
  - `processPendingCteCancelados`
- quando bypass ativo:
  - nao executa procedures SQL Server de CTe;
  - registra log estruturado de desativacao.

## Atualizacao - F5.2.2 concluido

Mudancas:

- em `cteSync`, quando PostgreSQL esta ativo:
  - `listarCtesNaoIntegrados` agora usa `prisma.cte.findMany` (sem `EXEC dbo.*`);
  - `listarCtesCanceladosNaoProcessados` agora usa `prisma.cte.findMany` (sem `SELECT TOP ... FROM dbo...`);
  - marcacao de processado por `id` passou a usar `prisma.cte.update` em vez de `UPDATE dbo...`.
- `alterarXMLProcessado` passou a usar `prisma.cte.updateMany` no modo PostgreSQL.

## Validacao

- `worker`: `npm run typecheck` -> OK
- `worker`: `npm run lint` -> OK (sem erros; warnings tecnicos preexistentes)
- container:
  - `docker compose up -d --build worker` executado com sucesso

## Proximo sublote

- iniciar `F5.2.3` em `cteIntegration.ts` para reduzir leituras simples (`TOP 1` / `EXEC dbo.*`) com fallback controlado por flags.

## Atualizacao - F5.2.3 (parcial)

Arquivo:

- `worker/src/services/cteIntegration.ts`

Mudancas:

- `buildCdEmpresaFromCnpj`:
  - em PostgreSQL com `ENABLE_SQLSERVER_LEGACY=false`, evita `EXEC dbo.P_EMPRESA_SENIOR_POR_CNPJ_LISTAR` e usa fallback controlado (`300`) com log.
- `obterCdTpDoctoFiscalPorEmpresa`:
  - em PostgreSQL com `ENABLE_SQLSERVER_LEGACY=false`, evita query `SELECT TOP 1 ... EmpresaSenior` e aplica regra direta por `cdEmpresa`.

Validacao:

- `worker`: `npm run typecheck` -> OK
- `worker`: `npm run lint` -> OK (sem erros; warnings tecnicos preexistentes)

## Atualizacao - F5.2.4 concluido

Arquivo:

- `worker/src/services/cteIntegration.ts`

Mudancas:

- adicionado gate global de bypass para PostgreSQL sem legado:
  - `shouldBypassCteSeniorLegacy()`;
- `alterarXMLProcessado`:
  - em PostgreSQL, passou a usar `prisma.cte.updateMany` por `external_id` (sem `EXEC dbo.P_ALTERAR_XML_PROCESSADO_CR_SENIOR`);
- `inserirContasReceberCTe`:
  - em PostgreSQL sem legado, faz skip da integracao Senior e marca CT-e como processado no banco local com log estruturado;
- `cancelarCte`:
  - em PostgreSQL sem legado, retorna sucesso controlado com log de bypass;
- `verificarCteExistente`:
  - em PostgreSQL sem legado, retorna fallback local sem consultas `TOP 1` no banco Senior.

Validacao:

- `worker`: `./worker/node_modules/.bin/prettier --write worker/src/services/cteIntegration.ts` -> OK
- `worker`: `npm run typecheck` -> OK
- `worker`: `npm run lint` -> OK (sem erros; warnings tecnicos preexistentes)

## Proximo sublote

- `F5.2.5`: reduzir acoplamentos SQL Server remanescentes no codigo de CTe fora do caminho ativo em PostgreSQL e consolidar estrategia de limpeza incremental.

## Atualizacao - F5.2.5 (parcial)

Arquivos:

- `worker/src/services/cteIntegration.ts`
- `worker/src/services/cteSync.ts`

Mudancas:

- `cteIntegration` (modo PostgreSQL sem legado):
  - `inserirContasReceberCTe` passou a retornar `tabelasInseridas: ['LOCAL_POSTGRES']` para refletir processamento local;
- `cteSync`:
  - no sucesso de `processarCte`, quando `shouldBypassCteLegacyFlow()` esta ativo:
    - nao executa verificacao de tabelas Senior (`verificarCteExistente`);
    - finaliza `WebhookEvent` como `integrated` em modo local, com metadata `modo: 'postgres_local_sem_legacy'`;
    - evita consultas e mensagens baseadas em GTCCONCE/GTCCONHE/GTCCONSF no fluxo PostgreSQL.

Validacao:

- `worker`: `./worker/node_modules/.bin/prettier --write worker/src/services/cteIntegration.ts worker/src/services/cteSync.ts` -> OK
- `worker`: `npm run typecheck` -> OK
- `worker`: `npm run lint` -> OK (sem erros; warnings tecnicos preexistentes)

## Atualizacao - F5.2.6 concluido (prioridade CTe para testes)

Arquivos:

- `worker/src/index.ts`
- `worker/src/services/cteSync.ts`

Mudancas:

- removido bloqueio global que encerrava o `processBatch` em PostgreSQL sem legado antes de processar CTe;
- em `worker/src/index.ts`, modo PostgreSQL sem legado passa a executar explicitamente os fluxos adaptados:
  - `processPendingCte`
  - `processPendingCteCancelados`
  - `processPendingCiot` (com bypass interno);
- removidos retornos antecipados em `processPendingCte` e `processPendingCteCancelados` que impediam o processamento local por Prisma.

Validacao funcional ponta a ponta:

- envio real de CT-e para `POST /api/CTe/InserirCte?token=...` retornando `201`;
- registros em `ctes` marcados com `processed = true` para `external_id` de teste (`900001`, `900002`, `900003`);
- eventos de worker criados e concluídos:
  - `cte-1`, `cte-2`, `cte-3`
  - `source = worker/cte`
  - `integrationStatus = integrated`
  - metadata com `modo = postgres_local_sem_legacy`.

Validacao tecnica:

- `worker`: `./worker/node_modules/.bin/prettier --write worker/src/index.ts worker/src/services/cteSync.ts` -> OK
- `worker`: `npm run typecheck` -> OK
- `worker`: `npm run lint` -> OK (sem erros; warnings tecnicos preexistentes)

## Observacao de prioridade

- `ciotService` no backend permanece pendente por decisao de prioridade.
- foco atual: manter fluxo completo de recebimento/processamento de CTe estavel para testes.
