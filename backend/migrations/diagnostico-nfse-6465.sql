-- Script de diagnóstico para NFSe 6465
-- Verifica o status da NFSe e se ela foi integrada nas tabelas Senior

-- 1. Verificar se a NFSe existe na tabela nfse e seu status
SELECT 
    id,
    NumeroNfse,
    CnpjIdentPrestador,
    processed,
    status,
    error_message,
    created_at,
    updated_at,
    cancelado
FROM AFS_INTEGRADOR.dbo.nfse
WHERE NumeroNfse = 6465
ORDER BY id DESC;

-- 2. Verificar se existe registro em GTCConhe (tabela de destino)
SELECT 
    CdEmpresa,
    NrSeqControle,
    NrDoctoFiscal,
    DtEmissao,
    DsUsuario,
    DtDigitacao
FROM SOFTRAN_BRASILMAXI_HML.dbo.GTCConhe
WHERE NrDoctoFiscal = 6465
ORDER BY NrSeqControle DESC;

-- 3. Verificar se existe registro em sisnfsne (tabela de destino)
SELECT 
    CdEmpresa,
    CdSequencia,
    NrSeqControle,
    NrDoctoFiscal,
    NrNFSe,
    CdVerifNFSe,
    DsUsuInclusao,
    DtHrInclusao
FROM SOFTRAN_BRASILMAXI_HML.dbo.sisnfsne
WHERE NrDoctoFiscal = 6465
ORDER BY CdSequencia DESC;

-- 4. Verificar eventos WebhookEvent relacionados a esta NFSe
SELECT 
    id,
    source,
    receivedAt,
    status,
    processedAt,
    errorMessage,
    retryCount,
    integrationStatus,
    processingTimeMs,
    integrationTimeMs,
    seniorId,
    metadata
FROM dbo.WebhookEvent
WHERE source LIKE '%nfse%' 
  AND (metadata LIKE '%6465%' OR source LIKE '%6465%')
ORDER BY receivedAt DESC;

-- 5. Verificar todas as NFSe com número 6465 (caso haja múltiplas)
SELECT 
    id,
    NumeroNfse,
    CnpjIdentPrestador,
    processed,
    status,
    error_message,
    created_at,
    updated_at
FROM AFS_INTEGRADOR.dbo.nfse
WHERE NumeroNfse = 6465
ORDER BY id DESC;

-- 6. Verificar se há registros pendentes (processed = 0) para este número
SELECT 
    id,
    NumeroNfse,
    CnpjIdentPrestador,
    processed,
    status,
    error_message,
    created_at,
    updated_at
FROM AFS_INTEGRADOR.dbo.nfse
WHERE NumeroNfse = 6465
  AND processed = 0
  AND cancelado = 0
ORDER BY id DESC;

