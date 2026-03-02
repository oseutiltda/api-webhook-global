# AGENT_DATABASE.md

## Missao

Este agente e o responsavel pela migracao completa de banco de dados de SQL Server para PostgreSQL neste projeto.

Objetivo final:

- backend e worker rodando com PostgreSQL local;
- schemas Prisma compatíveis;
- migrations aplicadas;
- `.env` conectado ao novo banco;
- queries SQL Server substituidas, isoladas ou desativadas por flags.

## Escopo de atuacao

- `backend/prisma/schema.prisma`
- `worker/prisma/schema.prisma`
- `backend/src/routes/dashboard.ts`
- `backend/src/services/*` (principalmente `pessoaService.ts`, `ciotService.ts`, `contas*Service.ts`)
- `worker/src/services/*` (principalmente `nfseSync.ts`, `ciotSync.ts`, `cteSync.ts`, `*Integration.ts`, `*Sync.ts`)
- `docker-compose.yml`
- `.env` (modelo e variaveis)

## Regras obrigatorias

- Nunca alterar regra de negocio junto com mudanca de dialeto SQL sem necessidade.
- Priorizar migracao para Prisma Query API sempre que viavel.
- Onde nao for possivel no curto prazo, criar camada de compatibilidade com SQL por provider.
- Toda desativacao temporaria deve ser via flag.
- Toda decisao de bypass deve gerar log estruturado.
- Ao final de cada lote de implementacao, executar sequencia obrigatoria de qualidade:
  - `eslint`
  - `typecheck`
  - padronizacao do codigo com base no `prettier`
- Tracking de progresso obrigatorio:
  - apos cada mudanca de banco/codigo/documentacao, atualizar imediatamente o `.md` da fase em `docs/migracao-db/`;
  - registrar sempre: alteracao aplicada, resultado, evidencias (comando/log) e proximo passo;
  - manter checklist das fases sincronizado em tempo real (sem deixar para atualizar no fim).
  - atualizar obrigatoriamente `docs/migracao-db/progress-tracking.md` com:
    - ultimo checkpoint concluido
    - proximo checkpoint alvo
    - bloqueios abertos

## Retomada de contexto (obrigatorio)

- Antes de continuar qualquer trabalho, ler primeiro:
  - `docs/migracao-db/progress-tracking.md`
- A continuidade deve sempre partir do campo `Proximo checkpoint`.

## Diagnostico consolidado (base para execucao)

1. Prisma usa `provider = "sqlserver"` em backend e worker.
2. Schemas contem tipos nativos SQL Server:

- `@db.NVarChar`
- `@db.NText`
- `@db.DateTime`

3. Ha alto volume de SQL cru com sintaxe SQL Server:

- `TOP`
- `GETDATE()`
- `[dbo]`
- `ISJSON`
- `JSON_VALUE`
- `CAST(... AS VARCHAR)` especifico de fluxos atuais

4. Existem muitas chamadas `prisma.$queryRawUnsafe` e `prisma.$executeRawUnsafe`.
5. Sem adaptacao previa, nao e possivel migrar direto para PostgreSQL.

## Estrategia de migracao (ordem obrigatoria)

### Fase 0 - Preparacao

- [ ] Criar branch dedicada: `feat/db-postgres-migration`.
- [x] Criar pasta `docs/migracao-db/`.
- [x] Gerar inventario tecnico: arquivo com todas as queries raw e nivel de complexidade.
- [x] Definir matriz de risco por dominio (NFSe, CIOT, CTe, Pessoa, Contas).

Entrega:

- `docs/migracao-db/inventario-sql-raw.md`

### Fase 1 - Baseline PostgreSQL local

- [x] Adicionar servico Postgres no `docker-compose.yml`.
- [ ] Definir variaveis:
- [x] `POSTGRES_DB`
- [x] `POSTGRES_USER`
- [x] `POSTGRES_PASSWORD`
- [x] `POSTGRES_PORT`
- [x] Montar `DATABASE_URL` Postgres para backend e worker.
- [x] Criar `.env.example` com configuracao Postgres.

Entrega:

- Ambiente sobe com Postgres local acessivel.

### Fase 2 - Conversao de schema Prisma

- [x] Trocar `provider` para `postgresql` em backend e worker.
- [ ] Remover/adaptar anotacoes SQL Server:
- [x] `@db.NVarChar` -> `String`/`@db.VarChar` quando necessario
- [x] `@db.NText` -> `String`/`@db.Text`
- [x] `@db.DateTime` -> `DateTime`/`@db.Timestamp(...)` quando aplicavel
- [x] Revisar campos `Decimal`, `Boolean`, `DateTime` para compatibilidade.
- [ ] Garantir consistencia entre schemas de backend e worker.

Entrega:

- `prisma validate` sem erro para backend e worker.

### Fase 3 - Generate + migrate inicial

- [x] Rodar `prisma generate` em backend e worker.
- [x] Criar migration inicial Postgres.
- [x] Aplicar migration no banco local.
- [x] Verificar tabelas principais criadas com sucesso.

Entrega:

- Banco local em estado migrado e client Prisma gerado.

### Fase 4 - Compatibilidade de queries SQL (camada de transicao)

- [x] Criar utilitario de provider SQL (ex: `isPostgres`, `isSqlServer`).
- [x] Substituir SQL Server em consultas de dashboard:
- `GETDATE()` -> `NOW()`
- `DATEADD(HOUR, -24, GETDATE())` -> `NOW() - INTERVAL '24 hours'`
- `ISJSON/JSON_VALUE` -> operadores JSONB ou cast seguro no Postgres
- [ ] Migrar queries simples de `TOP 1` para `LIMIT 1`.
- [ ] Trocar `[dbo].[tabela]` por schema/tabela Postgres.

Entrega:

- Dashboard operacional com Postgres (mesmo com dados vazios).

### Fase 5 - Migracao por dominio (raw SQL -> Prisma/SQL Postgres)

- [ ] Pessoa: migrar `pessoaService.ts` e remover dependencias de SQL Server. (em andamento: bypass seguro + leituras simples + gravacao local minima em PostgreSQL aplicados)
- [ ] CIOT: migrar `ciotService.ts` e `ciotSync.ts`. (em andamento: `ciotSync` com bypass PostgreSQL sem legado aplicado; `ciotService` pendente por prioridade)
- [ ] CTe: migrar `cteSync.ts` e `cteIntegration.ts`. (em andamento: fluxo completo de recebimento/processamento local funcional para testes; pendente consolidacao final de cancelamento/idempotencia)
  - estado operacional atual: modo somente recebimento habilitado (`ENABLE_WORKER=false` e worker parado), mantendo CTe em staging para testes de ingestao.
- [ ] NFSe: migrar `nfseSync.ts`.
- [ ] Contas: migrar `contasPagar*`, `contasReceber*`, `contasReceberBaixa*`.
- [ ] Onde a migracao nao fechar no ciclo atual, aplicar flag de desativacao temporaria.

Entrega:

- Nenhum caminho critico depende de sintaxe SQL Server.

### Fase 6 - Feature flags de seguranca

- [ ] Implementar/usar:
- [x] `ENABLE_SQLSERVER_LEGACY=false`
- [x] `ENABLE_EXTERNAL_EXPORT=false`
- [x] `ENABLE_EXTERNAL_IMPORT=false`
- [x] `ENABLE_SENIOR_INTEGRATION=false`
- [ ] Garantir fallback com resposta controlada e log. (parcial: aplicado em CTe/Pessoa; pendente consolidacao nos demais dominios)

Entrega:

- Sistema funcional em modo seguro no Postgres sem integrar externo.

### Fase 7 - Validacao e hardening

- [ ] Subir stack completa local (backend/worker/frontend/postgres).
- [ ] Executar smoke tests de API.
- [ ] Validar dashboard e endpoints de health/stats.
- [ ] Registrar gaps finais e backlog de melhoria.

Entrega:

- `docs/migracao-db/resultado-validacao.md`

## Criterios de pronto

- Backend e worker conectados ao Postgres via `.env`.
- `prisma generate` e migrations funcionando em ambiente limpo.
- Nenhuma query SQL Server ativa em caminho principal.
- Dashboard e health checks estaveis.
- Flags permitem operacao segura enquanto reativacao de integracoes e feita em fases.

## Formato de trabalho esperado deste agente

Para cada lote de mudancas:

1. Diagnosticar alvo tecnico.
2. Alterar schema/codigo.
3. Rodar validacoes locais.
4. Documentar impacto e rollback.
5. Entregar proxima etapa da fila sem bloquear o restante do time.
