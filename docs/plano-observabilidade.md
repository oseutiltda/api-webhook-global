# Plano de Observabilidade — API Webhook Global

> **Data:** 2026-03-06  
> **Stack:** Grafana · Loki · Promtail · Sentry  
> **Escopo:** backend · worker · frontend · postgres

---

## 1. Visão Geral

Implementar observabilidade completa no projeto **api-webhook-global**, cobrindo três pilares:

| Pilar | Ferramenta | Objetivo |
|---|---|---|
| **Logs centralizados** | Loki + Promtail | Coletar, indexar e consultar logs de todos os containers |
| **Dashboards & Alertas** | Grafana | Visualizar métricas, logs e configurar alertas automáticos |
| **Error Tracking** | Sentry | Capturar exceções, rastrear performance e alertar em tempo real |

### Arquitetura de alto nível

```
┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│   backend    │   │   worker     │   │   frontend   │
│  (Pino JSON) │   │  (Pino JSON) │   │  (Next.js)   │
└──────┬───────┘   └──────┬───────┘   └──────┬───────┘
       │ stdout            │ stdout           │ stdout
       ▼                   ▼                  ▼
┌──────────────────────────────────────────────────────┐
│              Docker json-file log driver              │
│         /var/lib/docker/containers/*/*.log            │
└──────────────────────┬───────────────────────────────┘
                       │
                       ▼
              ┌────────────────┐
              │   Promtail     │  ← scrape logs de containers
              │  (container)   │
              └───────┬────────┘
                      │ push
                      ▼
              ┌────────────────┐
              │     Loki       │  ← armazena e indexa logs
              │  (container)   │
              └───────┬────────┘
                      │ datasource
                      ▼
              ┌────────────────┐
              │    Grafana     │  ← dashboards + alertas
              │  (container)   │
              └────────────────┘

  ┌──────────┐  ┌──────────┐  ┌──────────┐
  │ backend  │  │ worker   │  │ frontend │  ── @sentry/node / @sentry/nextjs ──▶ Sentry Cloud
  └──────────┘  └──────────┘  └──────────┘
```

---

## 2. Fases de Implementação

### Fase 1 — Infraestrutura de Logs (Loki + Promtail + Grafana)

#### 1.1 Adicionar serviços ao `docker-compose.yml`

Criar os containers do **Loki**, **Promtail** e **Grafana** na composição existente:

```yaml
# ── Observabilidade ──────────────────────────────────

  loki:
    image: grafana/loki:3.4.2
    container_name: global-loki
    restart: unless-stopped
    ports:
      - "3100:3100"
    volumes:
      - loki_data:/loki
      - ./observability/loki/loki-config.yml:/etc/loki/local-config.yaml
    command: -config.file=/etc/loki/local-config.yaml
    healthcheck:
      test: ["CMD-SHELL", "wget --no-verbose --tries=1 --spider http://localhost:3100/ready || exit 1"]
      interval: 15s
      timeout: 5s
      retries: 5

  promtail:
    image: grafana/promtail:3.4.2
    container_name: global-promtail
    restart: unless-stopped
    volumes:
      - ./observability/promtail/promtail-config.yml:/etc/promtail/config.yml
      - /var/lib/docker/containers:/var/lib/docker/containers:ro
      - /var/run/docker.sock:/var/run/docker.sock:ro
    command: -config.file=/etc/promtail/config.yml
    depends_on:
      loki:
        condition: service_healthy

  grafana:
    image: grafana/grafana:11.6.0
    container_name: global-grafana
    restart: unless-stopped
    ports:
      - "3002:3000"
    environment:
      GF_SECURITY_ADMIN_USER: ${GRAFANA_ADMIN_USER:-admin}
      GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_ADMIN_PASSWORD:-admin}
      GF_USERS_ALLOW_SIGN_UP: "false"
    volumes:
      - grafana_data:/var/lib/grafana
      - ./observability/grafana/provisioning:/etc/grafana/provisioning
    depends_on:
      loki:
        condition: service_healthy
```

Adicionar aos `volumes`:

```yaml
volumes:
  postgres_data:
  loki_data:
  grafana_data:
```

#### 1.2 Criar arquivos de configuração

Estrutura de diretórios:

```
observability/
├── grafana/
│   └── provisioning/
│       ├── datasources/
│       │   └── loki.yml          # Datasource Loki auto-provisionado
│       └── dashboards/
│           ├── dashboard.yml     # Provider de dashboards
│           └── api-overview.json # Dashboard pré-configurado
├── loki/
│   └── loki-config.yml           # Configuração do Loki
└── promtail/
    └── promtail-config.yml       # Configuração do Promtail
```

##### `observability/loki/loki-config.yml`

```yaml
auth_enabled: false

server:
  http_listen_port: 3100

common:
  path_prefix: /loki
  storage:
    filesystem:
      chunks_directory: /loki/chunks
      rules_directory: /loki/rules
  replication_factor: 1
  ring:
    kvstore:
      store: inmemory

schema_config:
  configs:
    - from: "2024-01-01"
      store: tsdb
      object_store: filesystem
      schema: v13
      index:
        prefix: index_
        period: 24h

limits_config:
  retention_period: 30d          # Reter logs por 30 dias
  max_query_length: 721h
  ingestion_rate_mb: 10
  ingestion_burst_size_mb: 20

compactor:
  working_directory: /loki/compactor
  compaction_interval: 10m
  retention_enabled: true
  retention_delete_delay: 2h
  retention_delete_worker_count: 150
```

##### `observability/promtail/promtail-config.yml`

```yaml
server:
  http_listen_port: 9080
  grpc_listen_port: 0

positions:
  filename: /tmp/positions.yaml

clients:
  - url: http://loki:3100/loki/api/v1/push

scrape_configs:
  - job_name: docker
    docker_sd_configs:
      - host: unix:///var/run/docker.sock
        refresh_interval: 5s
        filters:
          - name: name
            values:
              - "bmx-backend"
              - "bmx-worker"
              - "bmx-frontend"
              - "global-postgres"
    relabel_configs:
      - source_labels: ['__meta_docker_container_name']
        regex: '/(.*)'
        target_label: 'container'
      - source_labels: ['__meta_docker_container_log_stream']
        target_label: 'logstream'
      - source_labels: ['__meta_docker_container_label_com_docker_compose_service']
        target_label: 'service'
    pipeline_stages:
      # Parsear logs JSON do Pino (backend e worker)
      - match:
          selector: '{service=~"backend|worker"}'
          stages:
            - json:
                expressions:
                  level: level
                  msg: msg
                  pid: pid
                  hostname: hostname
                  req_method: req.method
                  req_url: req.url
                  res_statusCode: res.statusCode
                  responseTime: responseTime
                  err_message: err.message
            - labels:
                level:
                msg:
            - timestamp:
                source: time
                format: UnixMs
      # Logs do PostgreSQL
      - match:
          selector: '{service="postgres"}'
          stages:
            - regex:
                expression: '^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d+ \w+) \[(?P<pid>\d+)\] (?P<level>\w+):  (?P<message>.*)$'
            - labels:
                level:
```

##### `observability/grafana/provisioning/datasources/loki.yml`

```yaml
apiVersion: 1
datasources:
  - name: Loki
    type: loki
    access: proxy
    url: http://loki:3100
    isDefault: true
    editable: false
```

##### `observability/grafana/provisioning/dashboards/dashboard.yml`

```yaml
apiVersion: 1
providers:
  - name: 'default'
    orgId: 1
    folder: 'Global Webhook'
    type: file
    disableDeletion: false
    editable: true
    options:
      path: /etc/grafana/provisioning/dashboards
      foldersFromFilesStructure: false
```

#### 1.3 Normalizar logging no Backend e Worker

Garantir que o **Pino** emita JSON estruturado consistente com campos padronizados.

**Modificar `backend/src/utils/logger.ts`:**

```typescript
import pino from 'pino';

export const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  formatters: {
    level(label) {
      return { level: label };
    },
  },
  timestamp: pino.stdTimeFunctions.isoTime,
  base: {
    service: 'backend',
    env: process.env.NODE_ENV || 'development',
  },
});
```

**Modificar `worker/src/utils/logger.ts`:**

```typescript
import pino from 'pino';

export const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  formatters: {
    level(label) {
      return { level: label };
    },
  },
  timestamp: pino.stdTimeFunctions.isoTime,
  base: {
    service: 'worker',
    env: process.env.NODE_ENV || 'development',
  },
});
```

> **Por quê?** Por padrão, Pino emite `level` como número (30, 40, 50...). Com o formatter acima, emite como string (`info`, `warn`, `error`), facilitando a filtragem no Loki/Grafana. O campo `service` identifica a origem no dashboard.

---

### Fase 2 — Sentry (Error Tracking + Performance Monitoring)

#### 2.1 Instalar dependências

```bash
# Backend
cd backend && npm install @sentry/node

# Worker
cd worker && npm install @sentry/node

# Frontend (Next.js)
cd frontend && npx @sentry/wizard@latest -i nextjs
```

#### 2.2 Configurar Sentry no Backend

**Criar `backend/src/config/sentry.ts`:**

```typescript
import * as Sentry from '@sentry/node';
import { logger } from '../utils/logger';

export function initSentry() {
  const dsn = process.env.SENTRY_DSN;

  if (!dsn) {
    logger.warn('SENTRY_DSN não configurado — Sentry desativado');
    return;
  }

  Sentry.init({
    dsn,
    environment: process.env.NODE_ENV || 'development',
    release: process.env.SENTRY_RELEASE || 'api-webhook-global@1.0.0',
    tracesSampleRate: parseFloat(process.env.SENTRY_TRACES_SAMPLE_RATE || '0.2'),
    profilesSampleRate: parseFloat(process.env.SENTRY_PROFILES_SAMPLE_RATE || '0.1'),
    integrations: [
      Sentry.httpIntegration(),
      Sentry.expressIntegration(),
      Sentry.prismaIntegration(),
    ],
    beforeSend(event) {
      // Não enviar erros de validação (ZodError) ao Sentry
      if (event.exception?.values?.some(v => v.type === 'ZodError')) {
        return null;
      }
      return event;
    },
  });

  logger.info({ environment: process.env.NODE_ENV }, 'Sentry inicializado');
}
```

**Modificar `backend/src/app.ts`** — Adicionar setup do Sentry:

```typescript
// No topo do arquivo, antes de qualquer outro import
import { initSentry } from './config/sentry';
import * as Sentry from '@sentry/node';
initSentry();

// ... imports existentes ...

const app = express();

// Sentry request handler (ANTES de todas as rotas)
Sentry.setupExpressErrorHandler(app);

// ... middlewares e rotas existentes ...

// Error handler existente (DEPOIS de todas as rotas)
// Modificar para capturar no Sentry:
app.use(errorHandler);
```

**Modificar `backend/src/middleware/error.ts`** — Integrar Sentry:

```typescript
import * as Sentry from '@sentry/node';

export function errorHandler(err: any, _req: Request, res: Response, _next: NextFunction) {
  // Capturar no Sentry (exceto erros de validação)
  if (!(err instanceof ZodError) && !(err instanceof SyntaxError && 'body' in err)) {
    Sentry.captureException(err, {
      extra: {
        url: _req.url,
        method: _req.method,
        statusCode: err.status || 500,
      },
    });
  }

  // ... lógica existente de tratamento de erros ...
}
```

#### 2.3 Configurar Sentry no Worker

**Criar `worker/src/config/sentry.ts`:**

```typescript
import * as Sentry from '@sentry/node';
import { logger } from '../utils/logger';

export function initSentry() {
  const dsn = process.env.SENTRY_DSN;

  if (!dsn) {
    logger.warn('SENTRY_DSN não configurado — Sentry desativado');
    return;
  }

  Sentry.init({
    dsn,
    environment: process.env.NODE_ENV || 'development',
    release: process.env.SENTRY_RELEASE || 'api-webhook-global@1.0.0',
    tracesSampleRate: parseFloat(process.env.SENTRY_TRACES_SAMPLE_RATE || '0.2'),
    integrations: [
      Sentry.prismaIntegration(),
    ],
  });

  logger.info({ environment: process.env.NODE_ENV }, 'Sentry inicializado no Worker');
}
```

**Modificar `worker/src/index.ts`** — Adicionar Sentry:

```typescript
// No topo do arquivo
import { initSentry } from './config/sentry';
import * as Sentry from '@sentry/node';
initSentry();

// No catch genérico do processBatch:
catch (error: any) {
  Sentry.captureException(error, {
    tags: { component: 'worker', operation: 'processBatch' },
    extra: { eventId: event?.id },
  });
  // ... lógica existente ...
}

// Capturar exceções não tratadas:
process.on('uncaughtException', (error) => {
  Sentry.captureException(error);
  Sentry.flush(2000).then(() => process.exit(1));
});
```

#### 2.4 Configurar Sentry no Frontend (Next.js)

Usar o wizard oficial (`npx @sentry/wizard@latest -i nextjs`), que cria automaticamente:
- `sentry.client.config.ts`
- `sentry.server.config.ts`
- `sentry.edge.config.ts`
- Configura `next.config.js` com `withSentryConfig`

Depois, personalizar o `sentry.client.config.ts` com:

```typescript
Sentry.init({
  dsn: process.env.NEXT_PUBLIC_SENTRY_DSN,
  environment: process.env.NODE_ENV,
  tracesSampleRate: 0.2,
  replaysSessionSampleRate: 0.1,
  replaysOnErrorSampleRate: 1.0,
  integrations: [
    Sentry.replayIntegration(),
    Sentry.browserTracingIntegration(),
  ],
});
```

---

### Fase 3 — Variáveis de Ambiente

#### 3.1 Adicionar ao `.env.example`

```bash
# ==============================
# Observabilidade
# ==============================

# Sentry
SENTRY_DSN=
SENTRY_RELEASE=api-webhook-global@1.0.0
SENTRY_TRACES_SAMPLE_RATE=0.2
SENTRY_PROFILES_SAMPLE_RATE=0.1
NEXT_PUBLIC_SENTRY_DSN=

# Grafana
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=admin
```

#### 3.2 Passar variáveis para os containers

Adicionar `SENTRY_DSN` e `SENTRY_RELEASE` no `environment` dos serviços `backend` e `worker` do `docker-compose.yml`.

---

### Fase 4 — Dashboards Grafana

#### 4.1 Dashboard principal pré-configurado

Criar `observability/grafana/provisioning/dashboards/api-overview.json` com os seguintes painéis:

| Painel | Tipo | Query LogQL |
|---|---|---|
| **Logs por serviço (live tail)** | Logs | `{service=~"backend\|worker"}` |
| **Erros por serviço (últimas 24h)** | Time series | `count_over_time({service=~"backend\|worker"} \|= "error" [5m])` |
| **Erros HTTP 5xx** | Stat | `count_over_time({service="backend"} \| json \| res_statusCode >= 500 [24h])` |
| **Latência de requests (p99)** | Gauge | `quantile_over_time(0.99, {service="backend"} \| json \| unwrap responseTime [1h])` |
| **Worker — eventos processados** | Time series | `count_over_time({service="worker"} \|= "Evento processado com sucesso" [5m])` |
| **Worker — falhas** | Time series | `count_over_time({service="worker"} \|= "Falha ao processar evento" [5m])` |
| **Erros de conexão DB** | Stat | `count_over_time({service=~"backend\|worker"} \|= "Erro de conexão" [1h])` |
| **Logs do PostgreSQL** | Logs | `{service="postgres"}` |

#### 4.2 Alertas Grafana recomendados

| Alerta | Condição | Severidade |
|---|---|---|
| Taxa de erros 5xx alta | > 10 erros 5xx em 5 min | Critical |
| Worker parado | Nenhum log de "Worker" em 10 min | Warning |
| Falhas consecutivas no worker | > 5 "Falha ao processar evento" em 5 min | Critical |
| Erros de conexão com DB | > 3 erros de conexão em 5 min | Critical |
| Container reiniciando | Log de "Worker iniciado" > 3x em 1h | Warning |

---

### Fase 5 — Hardening e Boas Práticas

#### 5.1 Contexto estruturado nos logs

Utilizar child loggers do Pino para adicionar contexto por domínio:

```typescript
// Exemplo em um controller
const domainLogger = logger.child({ domain: 'cte', operation: 'inserir' });
domainLogger.info({ cteId: '12345' }, 'CTe recebido para processamento');
```

#### 5.2 Request ID (Correlation ID)

Adicionar middleware para gerar um `requestId` único por requisição e propagá-lo em todos os logs:

```typescript
// backend/src/middleware/requestId.ts
import { v4 as uuid } from 'uuid';
import { Request, Response, NextFunction } from 'express';

export function requestIdMiddleware(req: Request, _res: Response, next: NextFunction) {
  req.id = req.headers['x-request-id'] as string || uuid();
  next();
}
```

Configurar o `pino-http` para usar o `requestId`:

```typescript
app.use(pinoHttp({
  logger,
  genReqId: (req) => req.headers['x-request-id'] || uuid(),
  customProps: (req) => ({ requestId: req.id }),
} as any));
```

#### 5.3 Métricas de performance no Sentry

Adicionar spans customizados para operações críticas:

```typescript
// Exemplo no worker
const transaction = Sentry.startSpan(
  { name: 'worker.processBatch', op: 'worker' },
  async (span) => {
    // ... processamento do batch ...
    span.setAttributes({
      'batch.size': events.length,
      'batch.processed': processedCount,
    });
  }
);
```

#### 5.4 Source Maps no Sentry

Configurar upload de source maps no build do backend e worker:

```bash
# No Dockerfile (após o build)
RUN npx @sentry/cli sourcemaps inject ./dist
RUN npx @sentry/cli sourcemaps upload ./dist --release=$SENTRY_RELEASE
```

---

## 3. Checklist de Implementação

### Infraestrutura (Docker)
- [x] Criar diretório `observability/` com configs de Loki, Promtail e Grafana
- [x] Adicionar serviços Loki, Promtail e Grafana ao `docker-compose.yml`
- [x] Adicionar volumes `loki_data` e `grafana_data`
- [ ] Testar `docker compose up` com stack completa

### Logging Estruturado
- [x] Atualizar `backend/src/utils/logger.ts` com formatters e campo `service`
- [x] Atualizar `worker/src/utils/logger.ts` com formatters e campo `service`
- [x] Implementar middleware de `requestId` no backend
- [ ] Validar que logs aparecem no Grafana/Loki

### Sentry
- [ ] Criar projeto no Sentry (ou Sentry self-hosted) e obter DSN
- [ ] Instalar `@sentry/node` no backend e worker (bloqueado por indisponibilidade de rede no ambiente local)
- [x] Criar `backend/src/config/sentry.ts` e `worker/src/config/sentry.ts`
- [x] Integrar Sentry no `app.ts` (backend) e `index.ts` (worker)
- [x] Integrar Sentry no error handler middleware
- [ ] Configurar Sentry no frontend via wizard
- [x] Adicionar variáveis ao `.env.example`
- [ ] Testar captura de exceções no Sentry

### Dashboards & Alertas
- [x] Criar dashboard JSON pré-configurado do Grafana
- [ ] Configurar alertas de erros 5xx, worker parado e falhas de DB
- [ ] Validar painéis com dados reais

### Hardening
- [ ] Implementar child loggers por domínio (CTe, NFSe, CIOT, etc.)
- [ ] Configurar upload de source maps para Sentry
- [x] Documentar runbook de uso dos dashboards

---

## 4. Variáveis de Ambiente — Resumo

| Variável | Serviço | Descrição |
|---|---|---|
| `SENTRY_DSN` | backend, worker | DSN do projeto Sentry |
| `SENTRY_RELEASE` | backend, worker | Versão da release (ex: `1.0.0`) |
| `SENTRY_TRACES_SAMPLE_RATE` | backend, worker | Taxa de amostragem de traces (0.0–1.0) |
| `SENTRY_PROFILES_SAMPLE_RATE` | backend | Taxa de profiling (0.0–1.0) |
| `NEXT_PUBLIC_SENTRY_DSN` | frontend | DSN público para o Next.js |
| `GRAFANA_ADMIN_USER` | grafana | Usuário admin do Grafana |
| `GRAFANA_ADMIN_PASSWORD` | grafana | Senha admin do Grafana |
| `LOG_LEVEL` | backend, worker | Nível de log (debug/info/warn/error) |

---

## 5. Ordem de Execução Recomendada

```
1. Criar diretório observability/ e arquivos de config
         │
2. Adicionar Loki + Promtail + Grafana ao docker-compose.yml
         │
3. Normalizar loggers (backend + worker) com formatters Pino
         │
4. Subir stack e validar logs no Grafana
         │
5. Instalar e configurar Sentry (backend → worker → frontend)
         │
6. Implementar middleware requestId
         │
7. Criar dashboard JSON e alertas Grafana
         │
8. Hardening: child loggers, source maps, documentação
```

---

## 6. Referências

- [Grafana Loki — Getting Started](https://grafana.com/docs/loki/latest/getting-started/)
- [Promtail — Docker Service Discovery](https://grafana.com/docs/loki/latest/clients/promtail/scraping/#docker-service-discovery)
- [Sentry Node.js SDK](https://docs.sentry.io/platforms/javascript/guides/node/)
- [Sentry Next.js SDK](https://docs.sentry.io/platforms/javascript/guides/nextjs/)
- [Pino Logger](https://getpino.io/)
