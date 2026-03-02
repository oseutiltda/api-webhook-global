# Inventario SQL Raw - Fase 0

Data: 2026-03-02
Escopo: backend + worker

## Resumo executivo
- Arquivos com SQL raw mapeados: 18
- Chamadas SQL raw totais (`$queryRaw*`, `$executeRaw*`): 287
- Ocorrencias de padrao SQL Server (TOP/GETDATE/DATEADD/ISJSON/JSON_VALUE/[dbo]/EXEC): 299
- Hotspot principal: `backend/src/services/pessoaService.ts`

## Compatibilidade de schema Prisma
- `provider = "sqlserver"` em 2 schemas
- Tipos SQL Server nativos encontrados (backend + worker):
- `@db.NVarChar`: 56
- `@db.NText`: 7
- `@db.DateTime`: 12

Conclusao: migracao para PostgreSQL exige conversao de schema Prisma + adaptacao de SQL raw.

## Matriz de risco por dominio
| Dominio | Arquivos | Raw calls | Padroes SQL Server | Risco | Motivo |
|---|---:|---:|---:|---|---|
| Pessoa | 1 | 67 | 72 | Critico | Alto volume de SQL raw + procedures + joins complexos |
| NFSe | 1 | 28 | 38 | Critico | SQL extenso com regras de negocio e funcoes SQL Server |
| CIOT | 4 | 58 | 46 | Alto | Integracao + sync com muita procedure e SQL dinamico |
| CTe | 2 | 33 | 28 | Alto | Integracao com tabelas e regras de conciliacao |
| ContasPagar | 3 | 43 | 38 | Alto | Procedures e fluxo financeiro multi-etapa |
| ContasReceber | 3 | 39 | 35 | Alto | Procedures e validacoes cruzadas |
| ContasReceberBaixa | 2 | 12 | 12 | Medio | Menor volume, mas com procedures |
| Dashboard | 1 | 6 | 30 | Medio | Forte dependencia de funcoes SQL Server para metricas |
| CoreSQL | 1 | 1 | 0 | Baixo | Ponto isolado de utilitario |

## Inventario por arquivo
| Arquivo | Dominio | Raw calls | Padroes SQL Server | Complexidade | Estrategia recomendada |
|---|---|---:|---:|---|---|
| `backend/src/services/pessoaService.ts` | Pessoa | 67 | 72 | Critica | Fase dedicada; quebrar em subservicos e migrar por blocos |
| `worker/src/services/nfseSync.ts` | NFSe | 28 | 38 | Critica | Priorizar compat layer + migracao progressiva |
| `worker/src/services/cteIntegration.ts` | CTe | 27 | 24 | Alta | Migrar SQL para PostgreSQL e reduzir raw unsafe |
| `worker/src/services/ciotIntegration.ts` | CIOT | 20 | 20 | Alta | Isolar procedures legadas e criar caminhos por provider |
| `backend/src/services/ciotService.ts` | CIOT | 19 | 10 | Alta | Trocar SQL raw por Prisma API quando possivel |
| `worker/src/services/ciotSync.ts` | CIOT | 18 | 16 | Alta | Revisar `TOP`, `GETDATE`, schema naming |
| `worker/src/services/contasPagarIntegration.ts` | ContasPagar | 16 | 15 | Alta | Migrar procedures para camada compativel |
| `backend/src/services/contasReceberService.ts` | ContasReceber | 15 | 10 | Alta | Migracao por funcoes de escrita/leitura |
| `backend/src/services/contasPagarService.ts` | ContasPagar | 15 | 11 | Alta | Reescrever queries com `LIMIT`/`NOW` e schema PG |
| `worker/src/services/contasReceberIntegration.ts` | ContasReceber | 14 | 15 | Alta | Adapter SQL por provider + reduc. de raw unsafe |
| `worker/src/services/contasPagarSync.ts` | ContasPagar | 12 | 12 | Media | Migrar procedures e queries batch |
| `worker/src/services/contasReceberBaixaSync.ts` | ContasReceberBaixa | 11 | 11 | Media | Migrar procedures e rotina de status |
| `worker/src/services/contasReceberSync.ts` | ContasReceber | 10 | 10 | Media | Migrar batch/sync e controle de robo |
| `worker/src/services/cteSync.ts` | CTe | 6 | 4 | Media | Ajuste de consultas e update processado |
| `backend/src/routes/dashboard.ts` | Dashboard | 6 | 30 | Media | Reescrever queries agregadas para Postgres |
| `backend/src/services/contasReceberBaixaService.ts` | ContasReceberBaixa | 1 | 1 | Baixa | Ajuste simples apos adapter financeiro |
| `worker/src/services/ciotRulesHelper.ts` | CIOT | 1 | 0 | Baixa | Revisao pontual |
| `worker/src/utils/nrSeqControle.ts` | CoreSQL | 1 | 0 | Baixa | Revisao pontual |

## Padroes SQL Server detectados (prioridade de substituicao)
1. `EXEC dbo.<procedure>` (alta prioridade)
2. `SELECT TOP ...` (alta prioridade)
3. `GETDATE()` e `DATEADD(...)` (alta prioridade)
4. `ISJSON(...)` e `JSON_VALUE(...)` (alta prioridade)
5. referencia de schema com colchetes `[db].[dbo].[tabela]` (alta prioridade)

## Fila estrategica para Fase 1+
1. Baseline Postgres local + `DATABASE_URL` (backend/worker)
2. Conversao de `schema.prisma` (provider e tipos nativos)
3. Dashboard (para manter observabilidade minima)
4. Dominios financeiros (ContasPagar/Receber/Baixa)
5. CTe + CIOT
6. NFSe
7. Pessoa (ultimo por risco e volume)

## Entregas da Fase 0
- [x] Pasta `docs/migracao-db/` criada
- [x] Inventario SQL raw consolidado
- [x] Matriz de risco por dominio definida
- [ ] Branch dedicada `feat/db-postgres-migration` (pendente de comando explicito)

