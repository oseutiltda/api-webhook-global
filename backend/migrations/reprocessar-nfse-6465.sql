-- Script para reprocessar NFSe 6465
-- ATENÇÃO: Execute este script apenas se a NFSe não foi integrada corretamente

-- 1. Verificar o status atual
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

-- 2. Verificar se a NFSe 6465 já existe nas tabelas de destino
-- Como existem registros em GTCConhe, a NFSe deve ser marcada como processada

-- Verificar registros em GTCConhe
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

-- Verificar registros em sisnfsne
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

-- 3. Marcar todas as NFSe 6465 como processadas (já que existem nas tabelas de destino)
-- Execute apenas se confirmar que os registros existem nas tabelas de destino
/*
UPDATE AFS_INTEGRADOR.dbo.nfse
SET 
    processed = 1,
    status = 'processed',
    error_message = NULL,
    updated_at = GETDATE()
WHERE NumeroNfse = 6465
  AND processed = 0;

-- Verificar se foi atualizado
SELECT 
    id,
    NumeroNfse,
    processed,
    status,
    updated_at
FROM AFS_INTEGRADOR.dbo.nfse
WHERE NumeroNfse = 6465;
*/

-- 2. Se a NFSe estiver com processed = 0, ela será processada automaticamente pelo worker
-- Se quiser forçar o reprocessamento, execute:

-- Descomente as linhas abaixo apenas se necessário:
/*
-- Resetar o status para pendente (processed = 0)
UPDATE AFS_INTEGRADOR.dbo.nfse
SET 
    processed = 0,
    status = 'pending',
    error_message = NULL,
    updated_at = GETDATE()
WHERE NumeroNfse = 6465
  AND processed = 1;

-- Verificar se foi resetado
SELECT 
    id,
    NumeroNfse,
    processed,
    status,
    updated_at
FROM AFS_INTEGRADOR.dbo.nfse
WHERE NumeroNfse = 6465;
*/

-- 3. Verificar se há registros duplicados nas tabelas de destino
-- Se houver, pode ser necessário limpar antes de reprocessar

-- Verificar em GTCConhe
SELECT 
    CdEmpresa,
    NrSeqControle,
    NrDoctoFiscal,
    DtEmissao
FROM SOFTRAN_BRASILMAXI_HML.dbo.GTCConhe
WHERE NrDoctoFiscal = 6465;

-- Verificar em sisnfsne
SELECT 
    CdEmpresa,
    CdSequencia,
    NrSeqControle,
    NrDoctoFiscal,
    NrNFSe
FROM SOFTRAN_BRASILMAXI_HML.dbo.sisnfsne
WHERE NrDoctoFiscal = 6465;

