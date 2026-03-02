-- Query para encontrar o registro conflitante na tabela GTCCONHE
-- Dados extraídos do CT-e id=3392, external_id=33259489
-- XML: nCT=369, serie=6, emit CNPJ=59530832000162

USE AFS_INTEGRADOR;
GO

-- Dados do CT-e:
-- - nCT = 369 -> NrDoctoFiscal = 369
-- - serie = 6 -> NrSerie = '6'
-- - emit CNPJ = 59530832000162 -> cdEmpresa = 100 (via P_EMPRESA_SENIOR_POR_CNPJ_LISTAR)
-- - cdTpDoctoFiscal transformado = 1100 (procedure adiciona prefixo "1" ao cdEmpresa)

-- Índice único: (CdEmpresaLigada, NrDoctoFiscal, NrSerie, CdTpDoctoFiscal)
-- Valores esperados:
-- - CdEmpresaLigada = 100
-- - NrDoctoFiscal = 369
-- - NrSerie = '6'
-- - CdTpDoctoFiscal = 1100 (transformado) ou 100 (original)

SELECT 
    gh.NrSeqControle,
    gh.CdEmpresa,
    gh.CdEmpresaLigada,
    gh.NrDoctoFiscal,
    gh.NrSerie,
    gh.CdTpDoctoFiscal,
    gh.CdInscricao,
    gh.DtEmissao,
    gh.CdRemetente,
    gh.CdDestinatario,
    gh.VlTotalPrestacao,
    gh.VlLiquido,
    gh.InConhecimento,
    gh.InFatura
FROM SOFTRAN_BRASILMAXI_HML..GTCCONHE gh WITH (NOLOCK)
WHERE gh.CdEmpresaLigada = 100
  AND gh.NrDoctoFiscal = 369
  AND CAST(gh.NrSerie AS VARCHAR) = '6'
  AND (gh.CdTpDoctoFiscal = 100 OR gh.CdTpDoctoFiscal = 1100)
ORDER BY gh.NrSeqControle DESC;
GO

-- Query alternativa: Buscar todos os registros com mesmo NrDoctoFiscal e Serie
-- para esta empresa (pode haver mais de um conflitante)
SELECT 
    gh.NrSeqControle,
    gh.CdEmpresa,
    gh.CdEmpresaLigada,
    gh.NrDoctoFiscal,
    gh.NrSerie,
    gh.CdTpDoctoFiscal,
    gh.CdInscricao,
    gh.DtEmissao,
    gh.CdRemetente,
    gh.CdDestinatario,
    gh.VlTotalPrestacao,
    gh.VlLiquido
FROM SOFTRAN_BRASILMAXI_HML..GTCCONHE gh WITH (NOLOCK)
WHERE gh.CdEmpresaLigada = 100
  AND gh.NrDoctoFiscal = 369
  AND CAST(gh.NrSerie AS VARCHAR) = '6'
ORDER BY gh.NrSeqControle DESC;
GO

-- Query para verificar registro relacionado em GTCCONCE (pela chave de acesso)
-- Chave de acesso do CT-e: 35251259530832000162570060000003691775085946
SELECT 
    gc.NrSeqControle,
    gc.CdEmpresa,
    gc.CdChaveAcesso,
    gc.DtEmissao,
    gc.NrCTe
FROM SOFTRAN_BRASILMAXI_HML..GTCCONCE gc WITH (NOLOCK)
WHERE gc.CdChaveAcesso = '35251259530832000162570060000003691775085946'
  AND gc.CdEmpresa = 100
ORDER BY gc.NrSeqControle DESC;
GO

