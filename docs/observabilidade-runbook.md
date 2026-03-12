# Runbook de Observabilidade

## Objetivo
Operar logs centralizados (Loki/Promtail), dashboards e alertas (Grafana) e error tracking (Sentry) no ambiente local da API Webhook Global.

## 1. Subir stack
1. `docker compose up -d postgres backend frontend worker loki promtail grafana`
2. Validar health:
   - `curl -fsS http://localhost:3000/health`
   - `curl -fsS http://localhost:3100/ready`
   - `curl -fsS http://localhost:${GRAFANA_PORT:-3002}/api/health`

## 2. Acesso Grafana
- URL: `http://localhost:${GRAFANA_PORT:-3002}`
- Usuário: `${GRAFANA_ADMIN_USER}` (default `admin`)
- Senha: `${GRAFANA_ADMIN_PASSWORD}` (default `admin`)
- Datasource Loki provisionado automaticamente (`uid=loki`)
- Dashboard provisionado: `API Webhook Global - Overview`

## 3. Consultas úteis (LogQL)
- Logs backend/worker: `{service=~"backend|worker"}`
- Erros backend: `{service="backend"} |= "error"`
- Falhas worker: `{service="worker"} |= "Falha ao processar evento"`
- Logs Postgres: `{service="postgres"}`

## 4. Teste rápido de ponta a ponta
1. Gerar tráfego de API:
   - `./scripts/smoke/smoke-prod.sh` (ou smoke equivalente do domínio)
2. Abrir dashboard e confirmar:
   - logs aparecendo por serviço
   - contador de erros alterando quando houver falha real

## 5. Sentry
- Variáveis obrigatórias:
  - `SENTRY_DSN`
  - `SENTRY_RELEASE`
  - `SENTRY_TRACES_SAMPLE_RATE`
  - `SENTRY_PROFILES_SAMPLE_RATE` (backend)
- Com `SENTRY_DSN` vazio, Sentry fica desativado por fallback controlado em log.
- Após instalar dependências (`@sentry/node` e `@sentry/nextjs`), reiniciar containers.

## 6. Alertas recomendados
- Erros 5xx > 10 em 5 min (critical)
- Falhas de worker > 5 em 5 min (critical)
- Sem logs de worker em 10 min (warning)
- Erros de conexão DB > 3 em 5 min (critical)

## 7. Troubleshooting
- `promtail` sem logs:
  - conferir bind de `/var/run/docker.sock`
  - conferir nomes de container no `promtail-config.yml`
- `grafana` sem datasource:
  - validar volume `./observability/grafana/provisioning`
  - reiniciar grafana
- Sentry não envia erro:
  - validar `SENTRY_DSN`
  - checar log de inicialização do backend/worker para fallback
