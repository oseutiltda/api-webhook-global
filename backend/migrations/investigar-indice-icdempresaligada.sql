-- ============================================
-- Script para investigar o erro de índice único iCdEmpresaLigada
-- Erro: Cannot insert duplicate key row in object 'dbo.GTCConhe' 
--       with unique index 'iCdEmpresaLigada'. 
--       The duplicate key value is (100, 369, 6, 1100).
-- ============================================

USE SOFTRAN_BRASILMAXI;
GO

-- 1. Verificar a estrutura do índice para entender quais campos compõem o índice único
SELECT 
    i.name AS IndexName,
    i.type_desc AS IndexType,
    i.is_unique AS IsUnique,
    COL_NAME(ic.object_id, ic.column_id) AS ColumnName,
    ic.key_ordinal AS KeyOrdinal
FROM 
    sys.indexes i
    INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
WHERE 
    i.object_id = OBJECT_ID('dbo.GTCConhe')
    AND i.name = 'iCdEmpresaLigada'
ORDER BY 
    ic.key_ordinal;
GO

-- 2. Buscar o registro que está causando o conflito
-- Baseado no erro: (100, 369, 6, 1100)
-- Assumindo que o índice pode conter: CdEmpresaLigada, e mais alguns campos
SELECT TOP 10
    CdEmpresa,
    NrSeqControle,
    CdEmpresaLigada,
    CdEmpresaColeta,
    CdRemetente,
    CdDestinatario,
    CdInscricao,
    NrDoctoFiscal,
    CdTpDoctoFiscal,
    NrSerie,
    DtEmissao,
    CdChaveAcesso = (
        SELECT TOP 1 CdChaveAcesso 
        FROM SOFTRAN_BRASILMAXI..GTCCONCE gc 
        WHERE gc.CdEmpresa = gh.CdEmpresa 
          AND gc.NrSeqControle = gh.NrSeqControle
    )
FROM 
    SOFTRAN_BRASILMAXI..GTCConhe gh WITH (NOLOCK)
WHERE 
    CdEmpresaLigada = 100
    -- Tentar encontrar registros com valores próximos aos mencionados no erro
    AND (
        CdEmpresa = 100 OR
        CdEmpresaLigada = 100 OR
        CdEmpresaColeta = 369 OR
        CdRemetente = '369' OR
        CdDestinatario = '369' OR
        NrSeqControle = 369 OR
        NrSeqControle = 6 OR
        NrSeqControle = 1100
    )
ORDER BY 
    NrSeqControle DESC;
GO

-- 3. Buscar mais especificamente baseado nos valores do erro: (100, 369, 6, 1100)
-- Tentar diferentes combinações de campos (usando campos que realmente existem)
SELECT TOP 10
    CdEmpresa,
    NrSeqControle,
    CdEmpresaLigada,
    CdEmpresaColeta,
    CdRemetente,
    CdDestinatario,
    CdOperacao,
    CdTransporte,
    CdInscricao,
    NrDoctoFiscal,
    CdTpDoctoFiscal,
    NrSerie,
    CdChaveAcesso = (
        SELECT TOP 1 CdChaveAcesso 
        FROM SOFTRAN_BRASILMAXI..GTCCONCE gc 
        WHERE gc.CdEmpresa = gh.CdEmpresa 
          AND gc.NrSeqControle = gh.NrSeqControle
    )
FROM 
    SOFTRAN_BRASILMAXI..GTCConhe gh WITH (NOLOCK)
WHERE 
    CdEmpresaLigada = 100
ORDER BY 
    NrSeqControle DESC;
GO

-- 4. Verificar se há registro relacionado ao CT-e específico (external_id: 33259489, nrSeqControle: 62553)
SELECT 
    gh.CdEmpresa,
    gh.NrSeqControle,
    gh.CdEmpresaLigada,
    gc.CdChaveAcesso,
    gc.CdChaveCTe,
    gc.NrProtocoloCTe
FROM 
    SOFTRAN_BRASILMAXI..GTCConhe gh WITH (NOLOCK)
    LEFT JOIN SOFTRAN_BRASILMAXI..GTCCONCE gc ON 
        gc.CdEmpresa = gh.CdEmpresa 
        AND gc.NrSeqControle = gh.NrSeqControle
WHERE 
    gh.CdEmpresa = 100
    AND (
        gh.NrSeqControle = 62553
        OR gc.CdChaveCTe = 77508594
        OR gh.CdEmpresaLigada = 100
    )
ORDER BY 
    gh.NrSeqControle DESC;
GO

-- 5. Buscar TODOS os registros com CdEmpresaLigada = 100 para análise completa
SELECT 
    COUNT(*) AS TotalRegistros,
    MIN(NrSeqControle) AS MinNrSeqControle,
    MAX(NrSeqControle) AS MaxNrSeqControle
FROM 
    SOFTRAN_BRASILMAXI..GTCConhe WITH (NOLOCK)
WHERE 
    CdEmpresaLigada = 100;
GO

-- 6. IMPORTANTE: Verificar a estrutura do índice para descobrir EXATAMENTE quais campos compõem o índice
-- Esta query é a MAIS IMPORTANTE - ela mostra os campos exatos do índice único
SELECT 
    i.name AS IndexName,
    i.type_desc AS IndexType,
    i.is_unique AS IsUnique,
    i.is_primary_key AS IsPrimaryKey,
    COL_NAME(ic.object_id, ic.column_id) AS ColumnName,
    ic.key_ordinal AS KeyOrdinal,
    ic.is_descending_key AS IsDescending,
    t.name AS TableName
FROM 
    sys.indexes i
    INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    INNER JOIN sys.tables t ON i.object_id = t.object_id
WHERE 
    i.object_id = OBJECT_ID('SOFTRAN_BRASILMAXI.dbo.GTCConhe')
    AND i.name = 'iCdEmpresaLigada'
ORDER BY 
    ic.key_ordinal;
GO

-- 7. Após descobrir os campos do índice, buscar o registro exato que está causando o conflito
-- Valores do erro: (100, 369, 6, 1100)
-- IMPORTANTE: Esta query usa apenas os campos que realmente existem na tabela GTCConhe

SELECT TOP 20
    gh.CdEmpresa,
    gh.NrSeqControle,
    gh.CdEmpresaLigada,      -- Valor 1: 100 (confirmado no erro)
    gh.CdEmpresaColeta,       -- Possível valor 2
    gh.CdRemetente,           -- CNPJ do remetente
    gh.CdDestinatario,        -- CNPJ do destinatário
    gh.CdOperacao,            -- Possível valor (no CT-e é 535312, mas pode ser 369)
    gh.CdTransporte,          -- Possível valor (no CT-e é 1, mas pode ser outro)
    gh.CdInscricao,           -- Possível campo do índice
    gh.NrDoctoFiscal,         -- Número do documento fiscal
    gh.CdTpDoctoFiscal,       -- Tipo do documento fiscal
    gh.NrSerie,               -- Série do documento
    gc.CdChaveAcesso,         -- Chave de acesso do CT-e
    gc.CdChaveCTe             -- Chave do CT-e
FROM 
    SOFTRAN_BRASILMAXI..GTCConhe gh WITH (NOLOCK)
    LEFT JOIN SOFTRAN_BRASILMAXI..GTCCONCE gc ON 
        gc.CdEmpresa = gh.CdEmpresa 
        AND gc.NrSeqControle = gh.NrSeqControle
WHERE 
    gh.CdEmpresaLigada = 100
    AND (
        -- Tentar diferentes combinações baseadas nos valores (100, 369, 6, 1100)
        -- Esses valores podem estar em diferentes campos dependendo da estrutura do índice
        gh.CdEmpresaColeta = 369 OR
        gh.CdOperacao = 369 OR
        gh.CdTransporte = 369 OR
        CAST(gh.CdRemetente AS INT) = 369 OR
        CAST(gh.CdDestinatario AS INT) = 369 OR
        gh.CdOperacao = 6 OR
        gh.CdTransporte = 6 OR
        gh.CdEmpresaColeta = 6 OR
        gh.CdOperacao = 1100 OR
        gh.CdTransporte = 1100 OR
        gh.CdEmpresaColeta = 1100
    )
ORDER BY 
    gh.NrSeqControle DESC;
GO

-- 8. EXECUTAR APÓS VER A ESTRUTURA DO ÍNDICE (Query 1 e 6)
-- Esta query busca o registro EXATO baseado nos campos do índice que foram descobertos
-- IMPORTANTE: Substitua os campos abaixo pelos campos REAIS retornados nas queries 1 e 6

-- Exemplo (AJUSTE CONFORME O RESULTADO DAS QUERIES 1 E 6):
-- Se o índice for (CdEmpresaLigada, CdEmpresaColeta, CdOperacao, CdTransporte):
/*
SELECT TOP 5
    gh.CdEmpresa,
    gh.NrSeqControle,
    gh.CdEmpresaLigada,
    gh.CdEmpresaColeta,
    gh.CdOperacao,
    gh.CdTransporte,
    gc.CdChaveAcesso,
    gc.CdChaveCTe,
    gh.NrDoctoFiscal,
    gh.DtEmissao
FROM 
    SOFTRAN_BRASILMAXI..GTCConhe gh WITH (NOLOCK)
    LEFT JOIN SOFTRAN_BRASILMAXI..GTCCONCE gc ON 
        gc.CdEmpresa = gh.CdEmpresa 
        AND gc.NrSeqControle = gh.NrSeqControle
WHERE 
    gh.CdEmpresaLigada = 100
    AND gh.CdEmpresaColeta = 369  -- Ajuste baseado no resultado
    AND gh.CdOperacao = 6         -- Ajuste baseado no resultado
    AND gh.CdTransporte = 1100    -- Ajuste baseado no resultado
ORDER BY 
    gh.NrSeqControle DESC;
*/
GO

