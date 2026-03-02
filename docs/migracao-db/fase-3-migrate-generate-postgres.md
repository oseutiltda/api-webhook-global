# Fase 3 - Migrate + Generate (PostgreSQL)

Data: 2026-03-02
Status: Concluida parcialmente (backend aplicado, worker mapeado)

## Objetivo
Aplicar migration inicial no PostgreSQL local e garantir geração de client Prisma.

## Resultado
### Backend
- Migration aplicada com sucesso:
  - `backend/prisma/migrations/20260302151740_init_postgres_backend/migration.sql`
- `prisma migrate dev` executado com sucesso.
- `prisma generate` executado com sucesso.

### Worker
- `prisma validate` e `prisma generate` executados com sucesso.
- `prisma migrate dev` NAO foi aplicado no mesmo banco neste momento para evitar conflito de historico de migration no `_prisma_migrations`, pois o schema do worker diverge do backend.

## Delta mapeado do worker vs banco atual
Principais diferencas detectadas com `prisma migrate diff`:
- Tabelas a adicionar (worker): `Final*` (14 tabelas)
- Tabelas removidas do ponto de vista do worker: `CiotBase`, `CteCancelado`, `FaturaPagarCancelamento`, `Nfse`

Conclusao tecnica:
- Backend e worker usam datamodels diferentes para o mesmo banco.
- Antes de aplicar migration do worker, e necessario definir estrategia unica:
  1. unificar schema Prisma, ou
  2. separar ownership de tabelas por estrategia de migration controlada.

## Acao corretiva aplicada para migrate funcionar
- O usuario `global` recebeu permissao `CREATEDB` para permitir shadow database do Prisma Migrate.

## Proximo passo (Fase 4)
- Criar estrategia de unificacao de schema backend/worker e regras de ownership de tabelas.
- Iniciar adaptacao de queries SQL Server -> PostgreSQL nos hotspots definidos no inventario.
