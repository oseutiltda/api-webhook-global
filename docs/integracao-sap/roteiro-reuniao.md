# Roteiro de Reuniao - Integracao SAP (Worker)

## Objetivo

Definir a implementacao da integracao com SAP mantendo o fluxo atual:

- API recebe payload;
- backend persiste no PostgreSQL;
- worker processa pendencias e integra no SAP.

Diretriz principal:

- nao integrar mais no legado SQL Server/Senior quando o fluxo SAP estiver ativo;
- manter rollout por dominio, com flags e observabilidade.

## Mini plano tecnico

### 1) Arquitetura alvo

- backend continua responsavel por ingestao e persistencia local;
- worker vira responsavel exclusivo por enviar/atualizar/cancelar no SAP;
- integracao legada permanece desativada por flag.

### 2) Camada SAP no worker

- `SapClient`: autenticacao, timeout, retry, circuit breaker.
- `SapMapper`: mapeamento de payload interno para contrato SAP por dominio.
- `SapService`: operacoes por dominio (`create`, `update`, `cancel`, `settle`).

### 3) Padrao de eventos internos

- cada evento deve conter:
  - `domain` (Pessoa, NotaSaida, NotaEntrada, BaixaCP, BaixaCR, GNRE);
  - `action` (`create`, `update`, `cancel`, `settle`);
  - `eventId` e chave idempotente;
  - payload normalizado;
  - metadados de rastreio.
- status do processamento:
  - `pending`
  - `processing`
  - `integrated`
  - `failed`
  - `dead_letter`

### 4) Flags por dominio

- `ENABLE_SAP_INTEGRATION`
- `ENABLE_SAP_PESSOA`
- `ENABLE_SAP_NOTA_SAIDA`
- `ENABLE_SAP_NOTA_ENTRADA`
- `ENABLE_SAP_BAIXAS`
- `ENABLE_SAP_GNRE`

### 5) Ordem recomendada de rollout

1. Pessoa (`create/update`)
2. Nota de saida (equivalente ao CTe: `create/cancel/update`)
3. Nota de entrada (equivalente a Contas a Pagar: `create/cancel/update`)
4. Baixas de contas a pagar e contas a receber (`settle/reverse`)
5. GNRE / NF de entrada (`create/update`)

### 6) Operacao segura

- retry com backoff exponencial e limite por evento;
- dead letter para falhas permanentes;
- logs estruturados com `eventId`, `domain`, `action`, `sapDocumentId`, `errorCode`;
- metricas por dominio: sucesso, falha, latencia, fila.

## Fluxo de referencia (Pessoa)

1. API chama `POST /api/Pessoa/InserirPessoa`.
2. Backend salva no banco local e registra evento.
3. Worker busca evento pendente de Pessoa.
4. `SapMapper` monta payload conforme contrato SAP.
5. `SapClient` envia requisicao.
6. Em sucesso: grava referencia SAP e marca `integrated`.
7. Em erro: retry; ao exceder limite, `dead_letter`.

## Perguntas objetivas para reuniao SAP

### Plataforma e contrato

1. Qual tecnologia oficial devemos usar: OData, REST, RFC/BAPI, IDoc, CPI?
2. os endpoints oficiais para cada domínio?
3. Existe ambiente de teste homologacao?
4. Funcionamento autenticacao (OAuth, Basic, certificado) e renovacao?

### Regras por operacao

6. os campos obrigatorios para `create`, `update`, `cancel`, `settle`?
7. Quais campos nao podem ser alterados apos integracao?

### Operacao e confiabilidade

18. Qual estrategia recomendada para reprocessamento/rollback?

## Checklist de saida da reuniao

### Pessoa

- contrato de `create/update` confirmado;
- chave idempotente definida;
- identificador SAP de retorno confirmado.

### Nota de saida (CTe-like)

- contrato de `create/cancel/update` fechado;
- regras fiscais de cancelamento documentadas;
- comportamento de atualizacao apos autorizacao definido.

### Nota de entrada (Contas a Pagar-like)

- contrato de `create/cancel/update` fechado;
- regras contabeis/fiscais de alteracao e cancelamento definidas.

### Baixas CP/CR

- contrato de liquidacao (`settle`) definido;
- fluxo de estorno e reconciliacao confirmado.

### GNRE / NF entrada

- contrato de `create/update` definido;
- campos fiscais obrigatorios e validacoes confirmados.

## Entregaveis tecnicos apos reuniao

- matriz de mapeamento campo-a-campo por dominio (interno -> SAP);
- definicao de chaves idempotentes por operacao;
- catalogo de erros SAP com classificacao (retryavel vs nao retryavel);
- plano de rollout por dominio com criterio de aceite e rollback.
