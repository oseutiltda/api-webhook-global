# AGENTS.md

## Finalidade

Este documento orienta agentes técnicos a transformar o projeto originalmente da **BMX (Empresa X)** em uma base reutilizável para a **Global**.

Diretriz principal:

- primeiro estabilizar execução local e neutralizar integrações externas;
- depois reativar capacidades aos poucos, com controle por flags e validação por domínio.

## Agentes Especializados

- Agente principal: este `AGENTS.md` coordena o plano macro da migracao.
- Agente de banco: usar **obrigatoriamente** [AGENT_DATABASE.md](/home/afsgroup/programacao/api-webhook-global/AGENT_DATABASE.md) para toda atividade de banco de dados e dialeto SQL.

## Regra de delegacao obrigatoria (database)

Sempre que a tarefa envolver qualquer um dos itens abaixo, o agente principal deve delegar ao `AGENT_DATABASE.md`:

- trocar SQL Server por PostgreSQL;
- alterar `schema.prisma` de backend/worker;
- criar ou aplicar migrations;
- ajustar `DATABASE_URL` e variaveis de banco no `.env`;
- migrar queries raw SQL (`$queryRawUnsafe`, `$executeRawUnsafe`);
- revisar compatibilidade de dashboard/servicos com PostgreSQL.

## Resultado esperado desta iniciativa

1. Ambiente local funcional (backend, worker, frontend) com banco local.
2. Migrações organizadas e reproduzíveis.
3. Exportações/importações externas comentadas ou desligadas por configuração.
4. Dashboard em estado vazio e estável.
5. Plano de reativação incremental por domínio de negócio.

## Regras obrigatórias para os agentes

- Não remover rotas públicas existentes sem decisão explícita.
- Não fazer chamadas de integração externa quando `ENABLE_EXTERNAL_EXPORT=false`.
- Não processar importação externa quando `ENABLE_EXTERNAL_IMPORT=false`.
- Toda desativação deve ser observável por log estruturado.
- Toda alteração estrutural deve ter migration correspondente.
- Nunca misturar rebranding visual com mudança de regra de negócio no mesmo PR, quando possível.
- Ao final de toda implementação, executar a sequência obrigatória de qualidade:
  - `eslint`
  - `typecheck`
  - padronização de código com base no `prettier`
- Tracking de progresso é obrigatório e contínuo:
  - após **cada mudança relevante**, atualizar imediatamente os arquivos `.md` de progresso da fase;
  - registrar: o que foi alterado, status (`feito`/`pendente`), evidências de validação e próximo passo;
  - não acumular atualizações para o final: o tracking deve ser incremental a cada etapa.
  - manter `docs/migracao-db/progress-tracking.md` sempre atualizado para retomada exata do ponto de parada.

## Retomada obrigatoria

- Antes de iniciar uma nova etapa, consultar:
  - `docs/migracao-db/progress-tracking.md`
- A execução deve continuar a partir do `Proximo checkpoint` registrado.

## Escopo técnico inicial

- Backend (`backend/src`)
  - rotas `/webhooks/*` e `/api/*`
  - controladores e serviços de integração
- Worker (`worker/src`)
  - processamento de pendências e integrações com procedimentos/tabelas finais
- Frontend (`frontend/src`)
  - dashboard principal e seleção
  - limpeza de branding BMX

## Estratégia operacional por fases

### Fase A - Inventário e congelamento

Objetivo: saber exatamente o que integra com sistemas externos.

Tarefas:

- Mapear funções de exportação/importação em:
  - `backend/src/services/*`
  - `backend/src/controllers/*`
  - `worker/src/services/*`
- Catalogar cada integração por domínio:
  - NFSe
  - CIOT
  - CTe
  - Pessoa
  - Contas a Pagar
  - Contas a Receber
- Marcar cada ponto com status:
  - `ativo`
  - `desativado_por_flag`
  - `pendente_migracao`

Critério de aceite:

- Inventário salvo no repositório (ex: `docs/migracao/integracoes.md`).

### Fase B - Banco local Global

Objetivo: retirar dependência de banco legado para desenvolvimento.

Tarefas:

- Provisionar SQL Server local.
- Configurar `DATABASE_URL` local para backend e worker.
- Validar `prisma generate` em ambos.
- Executar migrações necessárias e corrigir conflitos.
- Padronizar processo de migration (ordem e comandos oficiais).

Critério de aceite:

- Ambiente sobe do zero com comandos documentados e banco inicializado.

Observacao de execucao:

- Esta fase e executada pelo agente de banco conforme `AGENT_DATABASE.md`.

### Fase C - Desativar exportações/importações

Objetivo: operar em modo "seguro" sem troca externa de dados.

Tarefas:

- Introduzir flags de controle:
  - `ENABLE_EXTERNAL_EXPORT`
  - `ENABLE_EXTERNAL_IMPORT`
  - `ENABLE_SENIOR_INTEGRATION`
- Encapsular chamadas externas em guard clauses.
- Quando desativado:
  - retornar sucesso controlado ou status de skip, conforme fluxo;
  - registrar log: serviço, motivo, payload-id.
- Comentário de código breve explicando que é bloqueio temporário da migração Global.

Critério de aceite:

- Nenhuma chamada externa executa com flags desligadas.

Observacao de dependencia:

- So iniciar a Fase C apos o baseline de PostgreSQL e migrations da Fase B concluido pelo agente de banco.

### Fase D - Dashboard vazia

Objetivo: interface operacional disponível sem dependência de dados integrados.

Tarefas:

- Definir modo vazio por `ENABLE_DASHBOARD_DATA=false`.
- Ajustar endpoints para retornar estruturas default:
  - listas vazias
  - contadores zerados
  - mensagens de "integração desativada"
- Remover/ocultar referências BMX na UI.

Critério de aceite:

- Dashboard abre sem erro e com dados vazios coerentes.

### Fase E - Rebranding para Global

Objetivo: eliminar identidade BMX do produto.

Tarefas:

- Revisar textos, títulos e metadados.
- Trocar logos/assets em `frontend/public`.
- Atualizar nomes de container e documentação técnica.

Critério de aceite:

- Nenhum texto/logo BMX em fluxos de uso comum.

### Fase F - Reativação gradual por domínio

Objetivo: voltar integrações em etapas seguras.

Tarefas:

- Definir ordem de ativação (recomendado):
  1. Pessoa
  2. CTe
  3. NFSe
  4. CIOT
  5. Contas a Pagar
  6. Contas a Receber
- Para cada domínio:
  - ligar flag específica;
  - executar testes de contrato e idempotência;
  - acompanhar logs e métricas por janela controlada;
  - documentar resultado antes do próximo domínio.

Critério de aceite:

- Cada domínio ativado com checklist aprovado e rollback definido.

## Lista de tarefas completa (backlog macro)

### 1) Preparação

- [ ] Criar `docs/migracao/` com documentação central da transição.
- [ ] Criar matriz de integrações ativas por domínio.
- [ ] Definir responsáveis técnicos por backend/worker/frontend.

### 2) Infra local

- [x] Subir PostgreSQL local dedicado à Global.
- [x] Revisar `.env` e variáveis obrigatórias.
- [x] Validar conexão Prisma backend/worker.
- [x] Criar baseline de migration para ambiente limpo.

Atualizacao estrategica:

- [ ] Executar o backlog tecnico do `AGENT_DATABASE.md` (Fases 0 a 7). (em andamento: Fases 0, 1, 2, 3, 4.1, 4.2 e 4.3 concluídas; F5.1 Pessoa em progresso; F5.2 CTe com fluxo funcional validado e modo temporario somente recebimento ativo (`ENABLE_WORKER=false`); F5.3 CIOT iniciado com 5.3.1 no worker, backend pendente por prioridade)

### 3) Segurança de integração

- [ ] Implementar flags globais de export/import.
- [ ] Proteger chamadas para Senior/stored procedures. (em andamento: CTe e Pessoa com bypass em PostgreSQL)
- [ ] Garantir fallback sem erro para rotas públicas. (em andamento: endpoints principais de dashboard/worker estabilizados)
- [ ] Padronizar logs de bypass. (em andamento: logs estruturados aplicados em lotes de CTe/Pessoa)

### 4) Ajuste funcional mínimo

- [ ] Manter ingestão de payload funcionando.
- [ ] Persistir apenas no banco local/staging quando integração externa desligada.
- [ ] Garantir idempotência nos principais endpoints.

### 5) Dashboard vazia

- [ ] Respostas default nos endpoints de dashboard.
- [ ] UI com estado vazio amigável.
- [ ] Remover indicadores que dependem de integração ativa.

### 6) Rebranding

- [ ] Trocar nomenclaturas BMX -> Global no frontend.
- [ ] Trocar assets e títulos.
- [ ] Renomear serviços/containers/documentação onde necessário.

### 7) Qualidade

- [ ] Criar smoke tests de API essenciais.
- [x] Criar teste de inicialização completa via docker compose.
- [x] Verificar logs sem erros críticos com integrações desligadas.

### 8) Reativação gradual

- [ ] Criar flag por domínio de integração.
- [ ] Definir critérios de aceite por domínio.
- [ ] Executar ativação progressiva com monitoramento.

### 9) Entrega

- [ ] Documentar arquitetura alvo Global.
- [ ] Entregar runbook operacional de manutenção.
- [ ] Registrar pendências e débito técnico remanescente.

## Definição de pronto final

A migração é considerada pronta quando:

- o sistema roda localmente de ponta a ponta;
- integrações externas ficam totalmente controladas por flags;
- dashboard funciona em modo vazio sem erros;
- branding Global está aplicado;
- existe plano executável para reativação incremental das integrações.

## Observação para execução incremental

Sempre abrir PRs pequenos, preferencialmente por fase.
Cada PR deve conter:

- objetivo da fase;
- mudanças técnicas;
- evidências (logs, testes, prints da dashboard vazia quando aplicável);
- riscos e rollback.
