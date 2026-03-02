# Fase 5.1 - Pessoa (Primeiro Lote)

Data: 2026-03-02
Status: Em andamento (sub-lotes 5.1.1, 5.1.2 e 5.1.3 aplicados)

## Objetivo do lote

Estabilizar o dominio Pessoa no ambiente PostgreSQL em modo seguro, sem executar SQL Server legado enquanto a migracao completa nao termina.

## Alteracoes aplicadas

Arquivos:

- `backend/src/config/env.ts`
- `backend/src/services/pessoaService.ts`
- `.env`

Mudancas:

- adicionadas flags no backend:
  - `ENABLE_EXTERNAL_EXPORT` (bool)
  - `ENABLE_EXTERNAL_IMPORT` (bool)
  - `ENABLE_SENIOR_INTEGRATION` (bool)
- adicionado bypass controlado no `inserirPessoa`:
  - quando `DATABASE_URL` e PostgreSQL e `ENABLE_SENIOR_INTEGRATION=false`, o fluxo legado SQL Server/Senior e pulado com log estruturado;
  - retorno controlado de sucesso em modo migracao (sem executar procedures/queries SQL Server no caminho principal).
- `.env` atualizado com:
  - `ENABLE_EXTERNAL_EXPORT=false`
  - `ENABLE_EXTERNAL_IMPORT=false`
  - `ENABLE_SENIOR_INTEGRATION=false`

## Validacao

- `backend`: `npm run typecheck` -> OK
- `backend`: `npm run lint` -> OK (sem erros; warnings tecnicos preexistentes)
- `prettier` aplicado nos arquivos alterados.

## Proximo sublote (F5.1.2)

- iniciar substituicao de pontos de leitura `TOP 1`/`[AFS_INTEGRADOR].[dbo].*` em Pessoa:
  - `verificaExistenciaPessoa`
  - `obterCodigoPessoa`
  - queries de verificacao de integracao
- mover consultas de apoio para Prisma/SQL PostgreSQL ou gate por flag quando depender de externo.

## Atualizacao - F5.1.2 concluido

Mudancas:

- `verificaExistenciaPessoa` em PostgreSQL agora usa consulta local (`prisma.pessoa`) em vez de SQL Server.
- `obterCodigoPessoa` em PostgreSQL agora usa consulta local (`prisma.pessoa`) e extracao de `codPessoa` pelo payload quando disponivel.
- fallback SQL Server permanece apenas quando nao estiver em PostgreSQL.

Impacto:

- removida dependencia de `SELECT TOP 1`/`EXEC dbo.*` para leituras simples de Pessoa no modo PostgreSQL.
- preparado caminho para continuar a migracao de gravacoes/operacoes adicionais por lotes.

## Atualizacao - F5.1.3 concluido

Mudancas:

- bypass de Pessoa em modo migracao passou a persistir dados minimos localmente no PostgreSQL via `prisma.pessoa.upsert`.
- campos minimos persistidos: `id`, `nomeRazaoSocial`, `nomeFantasia`, `cpf`, `cnpj`, inscricoes, metadados e `payload`.

Validacao de runtime:

- `POST /api/Pessoa/InserirPessoa` retornando `201` com mensagem de modo migracao.
- consulta SQL no Postgres confirmou registro em `public."Pessoa"`:
  - `id = MIGRACAO-PESSOA-001`
  - `nomeRazaoSocial = Teste Migracao Pessoa`
