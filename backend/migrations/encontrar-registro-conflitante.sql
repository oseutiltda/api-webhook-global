-- ============================================
-- Script DIRETO para encontrar o registro que está causando o conflito
-- Erro: Cannot insert duplicate key row with unique index 'iCdEmpresaLigada'. 
--       The duplicate key value is (100, 369, 6, 1100).
-- ============================================

USE SOFTRAN_BRASILMAXI;
GO

-- PASSO 1: Descobrir quais campos compõem o índice iCdEmpresaLigada
-- Esta query é a MAIS IMPORTANTE - copie e cole o resultado
SELECT 
    i.name AS IndexName,
    COL_NAME(ic.object_id, ic.column_id) AS ColumnName,
    ic.key_ordinal AS OrdemNoIndice
FROM 
    sys.indexes i
    INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
WHERE 
    i.object_id = OBJECT_ID('SOFTRAN_BRASILMAXI.dbo.GTCConhe')
    AND i.name = 'iCdEmpresaLigada'
ORDER BY 
    ic.key_ordinal;
GO

-- PASSO 2: Buscar o registro que pode estar causando o conflito
-- Baseado nos valores do erro: (100, 369, 6, 1100)
-- Assumindo que pode ser uma combinação de campos, vamos buscar registros recentes com CdEmpresaLigada = 100

SELECT TOP 50
    gh.CdEmpresa,
    gh.NrSeqControle,
    gh.CdEmpresaLigada,      -- Valor 1: 100
    gh.CdEmpresaColeta,       -- Pode ser valor 2, 3 ou 4
    gh.CdOperacao,            -- Pode ser valor 2, 3 ou 4 (no CT-e atual é 535312, mas pode ser 369)
    gh.CdTransporte,          -- Pode ser valor 2, 3 ou 4 (no CT-e atual é 1, mas pode ser outro)
    gh.CdRemetente,
    gh.CdDestinatario,
    gh.CdInscricao,
    gh.NrDoctoFiscal,
    gh.CdTpDoctoFiscal,
    gh.NrSerie,
    gh.DtEmissao,
    gc.CdChaveAcesso,
    gc.CdChaveCTe,
    gc.NrProtocoloCTe
FROM 
    SOFTRAN_BRASILMAXI..GTCConhe gh WITH (NOLOCK)
    LEFT JOIN SOFTRAN_BRASILMAXI..GTCCONCE gc ON 
        gc.CdEmpresa = gh.CdEmpresa 
        AND gc.NrSeqControle = gh.NrSeqControle
WHERE 
    gh.CdEmpresaLigada = 100
    AND (
        -- Buscar registros com valores que podem estar no índice
        gh.CdEmpresaColeta IN (369, 6, 100, 1100) OR
        gh.CdOperacao IN (369, 6, 535312, 1100) OR
        gh.CdTransporte IN (369, 6, 1, 1100) OR
        gh.NrSeqControle IN (62553, 369, 6, 1100)
    )
ORDER BY 
    gh.NrSeqControle DESC;
GO

-- PASSO 3: Verificar se existe um registro com os valores específicos do CT-e atual
-- CT-e que está falhando: external_id=33259489, cdChaveCTe=77508594, nrSeqControle=62553
SELECT 
    gh.CdEmpresa,
    gh.NrSeqControle,
    gh.CdEmpresaLigada,
    gh.CdEmpresaColeta,
    gh.CdOperacao,
    gh.CdTransporte,
    gh.CdRemetente,
    gh.CdDestinatario,
    gc.CdChaveAcesso,
    gc.CdChaveCTe,
    gc.NrProtocoloCTe,
    'REGISTRO EXISTENTE QUE PODE ESTAR CAUSANDO CONFLITO' AS Observacao
FROM 
    SOFTRAN_BRASILMAXI..GTCConhe gh WITH (NOLOCK)
    LEFT JOIN SOFTRAN_BRASILMAXI..GTCCONCE gc ON 
        gc.CdEmpresa = gh.CdEmpresa 
        AND gc.NrSeqControle = gh.NrSeqControle
WHERE 
    (
        -- Verificar se existe registro com a mesma chave de acesso (mesmo CT-e)
        gc.CdChaveCTe = 77508594
        OR gh.NrSeqControle = 62553
        OR gc.CdChaveAcesso LIKE '%77508594%'
    )
    AND gh.CdEmpresa = 100;
GO

-- PASSO 4: Buscar o registro EXATO que está causando o conflito
-- Baseado nos valores do erro (100, 369, 6, 1100) e sabendo que o índice tem 4 campos
-- Vamos tentar diferentes combinações lógicas baseadas nos campos mais comuns:

-- Combinação 1: (CdEmpresaLigada, CdEmpresaColeta, CdOperacao, CdTransporte)
SELECT TOP 10
    gh.CdEmpresa,
    gh.NrSeqControle,
    gh.CdEmpresaLigada,    -- = 100
    gh.CdEmpresaColeta,     -- = 369 ou 6 ou 1100?
    gh.CdOperacao,          -- = 369 ou 6 ou 1100?
    gh.CdTransporte,        -- = 369 ou 6 ou 1100?
    gc.CdChaveAcesso,
    gc.CdChaveCTe,
    'COMBINACAO_1' AS TipoBusca
FROM 
    SOFTRAN_BRASILMAXI..GTCConhe gh WITH (NOLOCK)
    LEFT JOIN SOFTRAN_BRASILMAXI..GTCCONCE gc ON 
        gc.CdEmpresa = gh.CdEmpresa 
        AND gc.NrSeqControle = gh.NrSeqControle
WHERE 
    gh.CdEmpresaLigada = 100
    AND (
        (gh.CdEmpresaColeta = 369 AND gh.CdOperacao = 6 AND gh.CdTransporte = 1100) OR
        (gh.CdEmpresaColeta = 369 AND gh.CdOperacao = 1100 AND gh.CdTransporte = 6) OR
        (gh.CdEmpresaColeta = 6 AND gh.CdOperacao = 369 AND gh.CdTransporte = 1100) OR
        (gh.CdEmpresaColeta = 6 AND gh.CdOperacao = 1100 AND gh.CdTransporte = 369) OR
        (gh.CdEmpresaColeta = 1100 AND gh.CdOperacao = 369 AND gh.CdTransporte = 6) OR
        (gh.CdEmpresaColeta = 1100 AND gh.CdOperacao = 6 AND gh.CdTransporte = 369)
    );
GO

-- Combinação 2: Pode ser (CdEmpresaLigada, CdEmpresaColeta, ?, ?) - buscar registros com CdEmpresaColeta igual aos valores
SELECT TOP 10
    gh.CdEmpresa,
    gh.NrSeqControle,
    gh.CdEmpresaLigada,    -- = 100
    gh.CdEmpresaColeta,     -- = 369, 6 ou 1100?
    gh.CdOperacao,
    gh.CdTransporte,
    gh.NrDoctoFiscal,
    gh.CdTpDoctoFiscal,
    gc.CdChaveAcesso,
    'COMBINACAO_2' AS TipoBusca
FROM 
    SOFTRAN_BRASILMAXI..GTCConhe gh WITH (NOLOCK)
    LEFT JOIN SOFTRAN_BRASILMAXI..GTCCONCE gc ON 
        gc.CdEmpresa = gh.CdEmpresa 
        AND gc.NrSeqControle = gh.NrSeqControle
WHERE 
    gh.CdEmpresaLigada = 100
    AND gh.CdEmpresaColeta IN (369, 6, 100, 1100)
ORDER BY 
    gh.NrSeqControle DESC;
GO

