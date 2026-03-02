# Guia Inicial - Fluxo de CTe (modo atual)

## Objetivo deste guia

Explicar, de forma simples, como o CTe entra no sistema e o que acontece ate ele ser marcado como processado.

Este guia e para quem ainda nao tem experiencia com o projeto.

## Contexto atual (importante)

Hoje o projeto esta em migracao para PostgreSQL.

No modo atual:

- `ENABLE_SQLSERVER_LEGACY=false`
- o fluxo de CTe roda em modo local (PostgreSQL)
- integracao com banco/procedures da Senior esta desativada

Ou seja: o CTe e recebido, salvo, processado e marcado como concluido localmente para testes.

## Modo somente recebimento (sem worker)

Se quiser manter apenas o recebimento por webhook/API:

- configurar `ENABLE_WORKER=false` no `.env`
- parar o servico `worker` (`docker compose stop worker`)

Neste modo:

- o backend continua recebendo e gravando em `ctes`
- os registros ficam com `processed=false` (pendentes)
- nenhuma rotina de processamento em background e executada

## Visao geral do fluxo

1. API recebe o CTe (`POST /api/CTe/InserirCte?token=...`)
2. Backend valida o payload
3. Backend grava/atualiza o registro na tabela `ctes` com `processed=false`
4. Worker busca CTes pendentes (`processed=false`)
5. Worker processa em modo local PostgreSQL
6. Worker marca `processed=true` na tabela `ctes`
7. Eventos sao registrados na tabela `WebhookEvent`

## Etapa a etapa

### 1) Entrada pela API

Endpoint:

- `POST /api/CTe/InserirCte?token=SEU_TOKEN_FIXO`

Campos minimos esperados no body:

- `id` (vira `external_id`)
- `authorization_number`
- `status`
- `xml`
- `event_xml` (opcional)

### 2) Backend persiste em staging (`ctes`)

Servico principal:

- `backend/src/services/cteService.ts`

Comportamento:

- se nao existir, cria registro
- se ja existir (mesmo `external_id` + `authorization_number`), atualiza
- sempre deixa `processed=false` para o worker processar

### 3) Worker processa pendentes

Servicos principais:

- `worker/src/index.ts`
- `worker/src/services/cteSync.ts`
- `worker/src/services/cteIntegration.ts`

No modo PostgreSQL sem legado:

- o worker executa o fluxo de CTe adaptado
- nao chama procedures SQL Server da Senior
- marca o CTe como processado localmente (`processed=true`)

## Como validar se funcionou

### A) Validar tabela `ctes`

Esperado apos alguns segundos:

- registro com `processed=true`

Exemplo de consulta:

```sql
SELECT id, external_id, authorization_number, status, processed, updated_at
FROM ctes
ORDER BY id DESC
LIMIT 10;
```

### B) Validar `WebhookEvent`

Voce vera dois tipos de evento:

- evento da API: `source=/api/CTe/InserirCte`
- evento do worker: `source=worker/cte`

No worker, o esperado e:

- `status=processed`
- `integrationStatus=integrated`
- metadata com `modo=postgres_local_sem_legacy`

Exemplo:

```sql
SELECT id, source, status, "integrationStatus", metadata, "processedAt"
FROM "WebhookEvent"
WHERE id LIKE 'cte-%'
ORDER BY "receivedAt" DESC
LIMIT 20;
```

## Como testar rapidamente (roteiro)

1. Subir stack:
- `docker compose up -d --build`

2. Enviar um CTe para API.

3. Aguardar 5-10 segundos (ciclo do worker).

4. Consultar `ctes` e confirmar `processed=true`.

5. Consultar `WebhookEvent` e confirmar evento `worker/cte` como `integrated`.

## Erros comuns e causa

- `processed` continua `false`:
  - worker nao esta rodando
  - worker sem a versao mais nova de codigo
  - erro no loop do worker

- API responde 401:
  - token ausente ou invalido

- API responde 400:
  - payload invalido (faltou campo obrigatorio)

## O que este fluxo ainda NAO faz

- nao integra em tabelas finais da Senior
- nao executa procedures SQL Server do legado

Isso e intencional neste estagio da migracao para permitir testes locais seguros.
