-- ============================================
-- Script de teste para cancelar CT-e
-- CT-e: NrDoctoFiscal = 369, Serie = 6, CdEmpresaLigada = 100
-- ============================================

USE AFS_INTEGRADOR;
GO

-- Primeiro, verificar o registro antes de atualizar
SELECT 
    gh.NrSeqControle,
    gh.CdEmpresa,
    gh.CdEmpresaLigada,
    gh.NrDoctoFiscal,
    gh.NrSerie,
    gh.CdTpDoctoFiscal,
    gh.InConhecimento,
    gh.DtCancelamento,
    gh.CdInscricao,
    gh.DtEmissao
FROM SOFTRAN_BRASILMAXI_HML..GTCCONHE gh WITH (NOLOCK)
WHERE gh.CdEmpresaLigada = 100
  AND gh.NrDoctoFiscal = 369
  AND CAST(gh.NrSerie AS VARCHAR) = '6'
  AND (gh.CdTpDoctoFiscal = 100 OR gh.CdTpDoctoFiscal = 1100)
ORDER BY gh.NrSeqControle DESC;
GO

-- Atualizar para cancelar o CT-e
-- Define InConhecimento = 1 (cancelado) e DtCancelamento = data atual
UPDATE SOFTRAN_BRASILMAXI_HML..GTCCONHE
SET 
    InConhecimento = 1,
    DtCancelamento = GETDATE()
WHERE CdEmpresaLigada = 100
  AND NrDoctoFiscal = 369
  AND CAST(NrSerie AS VARCHAR) = '6'
  AND (CdTpDoctoFiscal = 100 OR CdTpDoctoFiscal = 1100);
GO

-- Verificar o registro após atualização
SELECT 
    gh.NrSeqControle,
    gh.CdEmpresa,
    gh.CdEmpresaLigada,
    gh.NrDoctoFiscal,
    gh.NrSerie,
    gh.CdTpDoctoFiscal,
    gh.InConhecimento,
    gh.DtCancelamento,
    gh.CdInscricao,
    gh.DtEmissao
FROM SOFTRAN_BRASILMAXI_HML..GTCCONHE gh WITH (NOLOCK)
WHERE gh.CdEmpresaLigada = 100
  AND gh.NrDoctoFiscal = 369
  AND CAST(gh.NrSerie AS VARCHAR) = '6'
  AND (gh.CdTpDoctoFiscal = 100 OR gh.CdTpDoctoFiscal = 1100)
ORDER BY gh.NrSeqControle DESC;
GO

PRINT 'CT-e cancelado com sucesso!';
PRINT 'InConhecimento foi atualizado para 1 (cancelado)';
PRINT 'DtCancelamento foi atualizado para: ' + CONVERT(VARCHAR, GETDATE(), 120);
GO

