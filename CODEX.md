# CODEX.md

## Objetivo

Este projeto será migrado de **BMX (Empresa X)** para **Global**, preservando o core técnico e reduzindo risco por fases.

Ponto crucial desta migração (diretriz principal):

- quando o `worker` estiver ativado, ele deve continuar lendo os dados do banco local (PostgreSQL) e processando os domínios atuais;
- o destino de integração final deve ser **SAP**;
- o fluxo legado para **SQL Server/Senior** deve ser tratado como transitório e progressivamente substituído, sem quebrar a ingestão atual.

Meta inicial:

- rodar tudo localmente com banco próprio;
- aplicar migrações de schema;
- desativar integrações de exportação/importação para operar em modo neutro;
- deixar a dashboard funcional, porém sem dados sensíveis (estado "vazio").

## Stack e estrutura atual

- `backend`: Node.js + TypeScript + Express + Prisma (`sqlserver`)
- `worker`: Node.js + TypeScript + Prisma (`sqlserver`)
- `frontend`: Next.js
- Orquestração: `docker-compose.yml`

Pastas principais:

- `backend/src`
- `worker/src`
- `frontend/src`
- `backend/prisma/schema.prisma`
- `worker/prisma/schema.prisma`
- `backend/migrations`

## Princípios de migração

- Não quebrar ingestão local: webhooks e APIs podem continuar recebendo payload.
- Desacoplar integração externa primeiro, reativar depois por feature flag.
- Toda mudança de schema deve entrar por migration versionada.
- Evitar hardcode de empresa (BMX/Global) no domínio da aplicação.
- Fazer rollout em fases pequenas e reversíveis.

## Estratégia técnica (alto nível)

1. Renomeação e neutralização de branding

- Remover textos e assets específicos de BMX no frontend.
- Atualizar nomes de containers/serviços para padrão neutro/global.

2. Banco local + migrations

- Definir `DATABASE_URL` local (SQL Server local).
- Garantir `prisma generate` e `prisma migrate` em backend/worker.
- Consolidar scripts de migration em fluxo único e reproduzível.

3. Congelar integrações externas

- Desabilitar caminhos que escrevem em bases/tabelas externas (Senior, exportações).
- Desabilitar entradas de importação externa que não sejam necessárias nesta fase.
- Substituir execução por no-op controlado por flag e log explícito.

  3.1 Direção alvo da integração do worker

- Manter o desenho atual de processamento por domínio no worker.
- Trocar o adaptador de saída legado (SQL Server/Senior) por adaptador de saída para SAP.
- Garantir que a troca seja feita por camada de integração (adapter/service), preservando contratos internos e idempotência.
- Ativação deve ser progressiva por domínio e controlada por flags.

4. Dashboard vazia

- Frontend deve abrir sem erro mesmo sem dados.
- Endpoints de métricas retornam estrutura vazia/default.
- Mensagens claras de "integração desativada".

5. Reativação gradual

- Cada domínio (NFSe, CIOT, CTe, Pessoa, Contas) reativado separadamente por flag.
- Validar em ambiente local/homologação antes de produção.

## Variáveis de ambiente sugeridas (novas)

Adicionar no `.env` (backend e worker):

- `COMPANY_CODE=GLOBAL`
- `ENABLE_EXTERNAL_EXPORT=false`
- `ENABLE_EXTERNAL_IMPORT=false`
- `ENABLE_SENIOR_INTEGRATION=false`
- `ENABLE_SAP_INTEGRATION=false` (novo alvo; ligar por domínio quando aplicável)
- `ENABLE_DASHBOARD_DATA=false`
- `ENABLED_WORKER_SERVICES=` (vazio para controle manual; ou lista específica)

## Padrões de implementação

- Toda integração externa deve passar por um "gate" de feature flag.
- Em modo desativado:
  - não chama procedure externa;
  - não persiste em tabela final externa;
  - registra log com motivo do bypass.
- Retornos HTTP devem continuar estáveis para não quebrar clientes existentes.

## Checklist de execução

### Fase 1: Base local

- [ ] Criar banco SQL Server local para Global.
- [ ] Ajustar `DATABASE_URL` para local.
- [ ] Executar `backend`/`worker` com Prisma client gerado.
- [ ] Rodar migrations e validar tabelas essenciais.

### Fase 2: Neutralização de integrações

- [ ] Mapear funções de exportação/importação em backend/worker.
- [ ] Colocar flags e no-op em integrações Senior e similares.
- [ ] Garantir que fluxos aceitam payload mas não exportam.
- [ ] Garantir logs de bypass padronizados.

### Fase 3: Dashboard vazia

- [ ] Ajustar frontend para estado inicial sem dados.
- [ ] Ajustar endpoints para respostas default quando `ENABLE_DASHBOARD_DATA=false`.
- [ ] Remover métricas dependentes de integração ativa nesta fase.

### Fase 4: Branding Global

- [ ] Trocar nome BMX por Global em UI, metadata e textos.
- [ ] Revisar logos/imagens públicas.
- [ ] Padronizar nomes de containers e documentação.

### Fase 5: Reativação controlada

- [ ] Definir ordem de reativação por domínio (ex: Pessoa -> CTe -> NFSe -> CIOT -> Contas).
- [ ] Criar testes mínimos por domínio antes de ativar flag.
- [ ] Ligar uma flag por vez e monitorar dashboard/logs.
- [ ] Direcionar integrações reativadas para SAP (não para SQL/Senior legado).

## Comandos úteis

Backend:

- `cd backend && npm install`
- `cd backend && npm run prisma:generate`
- `cd backend && npm run prisma:migrate`
- `cd backend && npm run dev`

Worker:

- `cd worker && npm install`
- `cd worker && npm run prisma:generate`
- `cd worker && npm run prisma:migrate`
- `cd worker && npm run dev`

Frontend:

- `cd frontend && npm install`
- `cd frontend && npm run dev`

Docker:

- `docker compose up -d --build`
- `docker compose logs -f backend worker frontend`

## Critérios de pronto (MVP da migração)

- Projeto sobe localmente sem dependências de BMX.
- Banco local Global funcional com migrations aplicadas.
- Exportações/importações externas desativadas e rastreáveis por log.
- Dashboard acessível e estável em modo vazio.
- Backlog de reativação por domínio definido.
