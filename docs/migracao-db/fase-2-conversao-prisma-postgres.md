# Fase 2 - Conversao Prisma para PostgreSQL

Data: 2026-03-02
Status: Concluida

## Objetivo
Converter os schemas Prisma de backend e worker de SQL Server para PostgreSQL.

## Alteracoes realizadas
Arquivos alterados:
- `backend/prisma/schema.prisma`
- `worker/prisma/schema.prisma`

Conversoes aplicadas:
1. Provider
- `provider = "sqlserver"` -> `provider = "postgresql"`

2. Tipos nativos
- `@db.NVarChar(n)` -> `@db.VarChar(n)`
- `@db.NText` -> `@db.Text`
- `@db.DateTime` -> `@db.Timestamp(3)`

## Evidencias
- Nao ha mais ocorrencia de:
  - `provider = "sqlserver"`
  - `@db.NVarChar`
  - `@db.NText`
  - `@db.DateTime`

- Ha ocorrencias de:
  - `provider = "postgresql"`
  - `@db.VarChar(...)`
  - `@db.Text`
  - `@db.Timestamp(3)`

## Validacao tecnica
Validacoes executadas com sucesso:
- backend:
  - `npx prisma validate`
  - `npx prisma generate`
- worker:
  - `npx prisma validate`
  - `npx prisma generate`

## Causa raiz dos erros anteriores (resolvida)
- `npx` falhava com `EAI_AGAIN` por restricao de rede no sandbox.
- `backend/worker` nao tinham `node_modules` instalados, entao nao havia binario Prisma local.

Acao aplicada:
- instalacao de dependencias com `npm ci` em `backend` e `worker`.
- execucao do Prisma com `DATABASE_URL` vindo do `.env` raiz.

## Proximo passo (Fase 3)
- Instalar/viabilizar Prisma CLI local e executar:
  - `prisma validate`
  - `prisma generate`
  - `prisma migrate`
