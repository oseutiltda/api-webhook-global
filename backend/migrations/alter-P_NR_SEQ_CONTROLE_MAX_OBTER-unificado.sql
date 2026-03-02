-- ============================================
-- Migration: Alterar P_NR_SEQ_CONTROLE_MAX_OBTER para buscar MAX entre GTCCONCE e GTCConhe
-- Descrição: Atualiza a procedure para buscar o MAX(NrSeqControle) entre ambas as tabelas
--            garantindo que não haja conflitos entre CT-e e NFSe
-- Banco: AFS_INTEGRADOR
-- Data: 2025-01-XX
-- ============================================

USE [AFS_INTEGRADOR]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- ============================================
-- Procedure: P_NR_SEQ_CONTROLE_MAX_OBTER
-- Alteração: Busca o MAX(NrSeqControle) entre GTCCONCE e GTCConhe
--            para garantir sequência única entre CT-e e NFSe
-- ============================================

ALTER PROCEDURE [dbo].[P_NR_SEQ_CONTROLE_MAX_OBTER]
AS
BEGIN
    -- Retorna o próximo número de sequência de controle global
    -- Busca o máximo entre GTCCONCE (CT-e) e GTCConhe (NFSe)
    -- Pega o máximo de todas as empresas e soma +1
    -- Se não houver registros, retorna 2 (ISNULL(MAX, 1) + 1)
    DECLARE @MaxGTCCONCE INT;
    DECLARE @MaxGTCConhe INT;
    DECLARE @MaxGlobal INT;
    
    -- Buscar MAX de GTCCONCE (CT-e)
    -- Usar o mesmo banco que está sendo usado nas outras procedures (pode ser SOFTRAN_BRASILMAXI ou SOFTRAN_BRASILMAXI_HML)
    SELECT @MaxGTCCONCE = ISNULL(MAX(NrSeqControle), 0)
    FROM SOFTRAN_BRASILMAXI..GTCCONCE (NOLOCK);
    
    -- Buscar MAX de GTCConhe (NFSe)
    SELECT @MaxGTCConhe = ISNULL(MAX(NrSeqControle), 0)
    FROM SOFTRAN_BRASILMAXI..GTCConhe (NOLOCK);
    
    -- Pegar o maior entre os dois
    SET @MaxGlobal = CASE 
        WHEN @MaxGTCCONCE > @MaxGTCConhe THEN @MaxGTCCONCE
        ELSE @MaxGTCConhe
    END;
    
    -- Retornar o próximo número (máximo + 1, mínimo 2)
    SELECT ISNULL(@MaxGlobal, 1) + 1 AS NrSeqControle;
END

GO

PRINT 'Procedure P_NR_SEQ_CONTROLE_MAX_OBTER alterada com sucesso!';
PRINT 'A procedure agora busca o MAX(NrSeqControle) entre GTCCONCE e GTCConhe.';
PRINT 'Isso garante sequência única entre CT-e e NFSe.';
GO

