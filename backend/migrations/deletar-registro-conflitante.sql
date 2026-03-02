-- ============================================
-- Script para DELETAR o registro que está causando o conflito
-- Erro: Cannot insert duplicate key row with unique index 'iCdEmpresaLigada'. 
--       The duplicate key value is (100, 369, 6, 1100).
-- ============================================

USE SOFTRAN_BRASILMAXI;
GO

-- Buscar o registro exato que está causando o conflito
-- Baseado nos valores do erro: (100, 369, 6, 1100)
-- Índice único: (CdEmpresaLigada, NrDoctoFiscal, NrSerie, CdTpDoctoFiscal)
SELECT 
    gh.CdEmpresa,
    gh.NrSeqControle,
    gh.CdEmpresaLigada,      -- Valor 1: 100
    gh.NrDoctoFiscal,         -- Valor 2: 369
    gh.NrSerie,               -- Valor 3: 6
    gh.CdTpDoctoFiscal,       -- Valor 4: 1100
    gc.CdChaveAcesso,
    gc.CdChaveCTe,
    gh.DtEmissao
FROM 
    SOFTRAN_BRASILMAXI..GTCCONHE gh WITH (NOLOCK)
    LEFT JOIN SOFTRAN_BRASILMAXI..GTCCONCE gc ON 
        gc.CdEmpresa = gh.CdEmpresa 
        AND gc.NrSeqControle = gh.NrSeqControle
WHERE 
    gh.CdEmpresaLigada = 100
    AND gh.NrDoctoFiscal = 369
    AND gh.NrSerie = '6'
    AND gh.CdTpDoctoFiscal = 1100;
GO

-- DELETAR o registro conflitante
-- CUIDADO: Execute apenas se tiver certeza que este é o registro correto
/*
DELETE FROM SOFTRAN_BRASILMAXI..GTCCONHE
WHERE CdEmpresaLigada = 100
  AND NrDoctoFiscal = 369
  AND NrSerie = '6'
  AND CdTpDoctoFiscal = 1100;
GO
*/

