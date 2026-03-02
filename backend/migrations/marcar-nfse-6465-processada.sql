-- Script para marcar NFSe 6465 como processada
-- Este script deve ser executado APENAS se a NFSe já existe nas tabelas de destino (GTCConhe ou sisnfsne)

-- 1. Verificar se a NFSe 6465 existe nas tabelas de destino
SELECT 
    'GTCConhe' as Tabela,
    CdEmpresa,
    NrSeqControle,
    NrDoctoFiscal,
    DtEmissao,
    DtCancelamento
FROM SOFTRAN_BRASILMAXI_HML.dbo.GTCConhe
WHERE NrDoctoFiscal = 6465
ORDER BY CdEmpresa;

SELECT 
    'sisnfsne' as Tabela,
    CdEmpresa,
    CdSequencia,
    NrSeqControle,
    NrDoctoFiscal,
    NrNFSe,
    DtHrInclusao
FROM SOFTRAN_BRASILMAXI_HML.dbo.sisnfsne
WHERE NrDoctoFiscal = 6465
ORDER BY CdEmpresa;

-- 2. Verificar status atual na tabela nfse
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

-- 3. ATUALIZAR: Marcar todas as NFSe 6465 como processadas
-- Execute apenas se confirmar que os registros existem nas tabelas de destino acima
UPDATE AFS_INTEGRADOR.dbo.nfse
SET 
    processed = 1,
    status = 'processed',
    error_message = NULL,
    updated_at = GETDATE()
WHERE NumeroNfse = 6465
  AND (processed = 0 OR status != 'processed');

-- 4. Verificar se foi atualizado
SELECT 
    id,
    NumeroNfse,
    CnpjIdentPrestador,
    processed,
    status,
    error_message,
    updated_at
FROM AFS_INTEGRADOR.dbo.nfse
WHERE NumeroNfse = 6465
ORDER BY id DESC;

