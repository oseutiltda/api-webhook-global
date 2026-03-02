# Guia de Diagnóstico de Falhas - CIOT/NFSe

## Como Identificar a Origem da Falha

### 1. Verificar a Tabela WebhookEvent

A tabela `dbo.WebhookEvent` registra todos os eventos do sistema e ajuda a identificar onde ocorreu a falha:

```sql
SELECT 
    id,
    source,
    status,
    integrationStatus,
    errorMessage,
    processingTimeMs,
    integrationTimeMs,
    seniorId,
    metadata,
    receivedAt,
    processedAt
FROM dbo.WebhookEvent
WHERE status = 'failed'
ORDER BY receivedAt DESC;
```

### 2. Interpretar os Campos

#### Campo `source`
- **`/api/CIOT/InserirContasPagarCIOT`** → Falha no **WORKER** (processamento de integração)
- **`/webhooks/ctrb/ciot/parcelas`** → Falha no **BACKEND** (recebimento de webhook)
- **`worker/nfse/{id}`** → Falha no **WORKER** (processamento NFSe)

#### Campo `integrationStatus`
- **`failed`** → Falha na integração com Senior (WORKER)
- **`pending`** → Ainda não foi processado
- **`integrated`** → Integrado com sucesso
- **`skipped`** → Ignorado (não requer integração)

#### Campo `errorMessage`
- **`Invalid object name 'SOFTRAN_BRASILMAXI..FTRCFT'`** → Problema de **BANCO DE DADOS** (tabela não existe)
- **`Registro não inserido, favor verificar log!`** → Erro genérico, verificar logs do worker
- **`Manifesto sem parcelas vinculadas`** → Problema de **DADOS** (backend não inseriu parcelas)

#### Campo `metadata` (JSON)
Contém detalhes adicionais:
```json
{
  "manifestId": 4547,
  "nrciot": "21183",
  "error": "Invalid object name 'SOFTRAN_BRASILMAXI..FTRCFT'",
  "errorCode": "208",
  "errorName": "PrismaClientKnownRequestError",
  "step": "executarFTRCFT",
  "isDatabaseError": true,
  "databaseObject": "SOFTRAN_BRASILMAXI..FTRCFT"
}
```

### 3. Tipos de Falhas Comuns

#### Falha no BACKEND (API)
**Sintomas:**
- `source` começa com `/webhooks/` ou `/api/`
- `status = 'failed'` mas `integrationStatus = NULL`
- `errorMessage` menciona problemas de validação, dados faltando, etc.

**Exemplos:**
- "Manifesto não incluído, favor verificar lançamento!"
- "ID do evento ausente"
- "Token inválido ou ausente"

**Ação:** Verificar logs do `bmx-backend` no Docker

#### Falha no WORKER (Integração)
**Sintomas:**
- `source = '/api/CIOT/InserirContasPagarCIOT'` ou `worker/nfse/...`
- `integrationStatus = 'failed'`
- `errorMessage` menciona stored procedures, tabelas Senior, etc.

**Exemplos:**
- "Invalid object name 'SOFTRAN_BRASILMAXI..FTRCFT'"
- "Erro na integração Senior (executarFTRCFT): ..."
- "Registro não inserido, favor verificar log!"

**Ação:** Verificar logs do `bmx-worker` no Docker

#### Falha de BANCO DE DADOS
**Sintomas:**
- `errorCode = '208'` ou `'P2021'`
- `errorMessage` contém "Invalid object name"
- `isDatabaseError = true` no metadata
- `databaseObject` indica qual tabela/objeto está faltando

**Exemplos:**
- "Invalid object name 'SOFTRAN_BRASILMAXI..FTRCFT'"
- "Invalid object name 'SOFTRAN_BRASILMAXI..GFAFATUR'"

**Ação:** 
1. Verificar se o banco `SOFTRAN_BRASILMAXI` existe
2. Verificar se as tabelas/stored procedures existem
3. Verificar permissões de acesso

### 4. Consultas Úteis

#### Falhas do Worker (Integração)
```sql
SELECT 
    id,
    source,
    status,
    integrationStatus,
    errorMessage,
    processingTimeMs,
    integrationTimeMs,
    metadata,
    receivedAt
FROM dbo.WebhookEvent
WHERE integrationStatus = 'failed'
ORDER BY receivedAt DESC;
```

#### Falhas do Backend (API)
```sql
SELECT 
    id,
    source,
    status,
    errorMessage,
    receivedAt
FROM dbo.WebhookEvent
WHERE status = 'failed' 
  AND integrationStatus IS NULL
ORDER BY receivedAt DESC;
```

#### Falhas de Banco de Dados
```sql
SELECT 
    id,
    source,
    errorMessage,
    metadata,
    receivedAt
FROM dbo.WebhookEvent
WHERE errorMessage LIKE '%Invalid object name%'
   OR errorMessage LIKE '%208%'
ORDER BY receivedAt DESC;
```

### 5. Análise do Caso Atual

**Evento:** `ciot-4547-1764180603195`

**Análise:**
- ✅ **Backend funcionou corretamente**: Inseriu manifesto, parcelas e faturamento
- ❌ **Worker falhou**: Erro ao executar stored procedure `executarFTRCFT`
- ❌ **Problema de Banco**: Tabela `SOFTRAN_BRASILMAXI..FTRCFT` não existe

**Diagnóstico:**
```
Origem: WORKER
Tipo: BANCO DE DADOS
Problema: Tabela/objeto não encontrado no banco SOFTRAN_BRASILMAXI
Solução: Verificar configuração do banco de dados Senior
```

### 6. Próximos Passos

1. Verificar se o banco `SOFTRAN_BRASILMAXI` está acessível
2. Verificar se as stored procedures existem:
   - `P_CONTAS_PAGAR_CIOT_FTRCFT_ESL_EXCLUIR`
   - `P_CONTAS_PAGAR_CIOT_FTRCFT_ESL_INCLUIR`
3. Verificar se as tabelas existem:
   - `FTRCFT`
   - `GFAFATUR`
   - `GFATITU`
   - etc.
4. Verificar variáveis de ambiente do worker:
   - `DATABASE_URL`
   - `CIOT_DESTINATION_DATABASE` (se existir)

