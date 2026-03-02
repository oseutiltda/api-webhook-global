-- ============================================
-- Migration: Alterar P_NR_SEQ_CONTROLE_MAX_OBTER
-- Descrição: Remove o parâmetro @CdEmpresa e o filtro WHERE para obter o MAX global
-- Banco: AFS_INTEGRADOR
-- Data: 12/4/2025
-- ============================================

USE [AFS_INTEGRADOR]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- ============================================
-- Procedure: P_NR_SEQ_CONTROLE_MAX_OBTER
-- Alteração: Removido parâmetro @CdEmpresa e filtro WHERE
-- Agora busca o MAX(NrSeqControle) global de todas as empresas
-- ============================================

ALTER PROCEDURE [dbo].[P_NR_SEQ_CONTROLE_MAX_OBTER]
AS
BEGIN
    -- Retorna o próximo número de sequência de controle global
    -- Pega o máximo de todas as empresas e soma +1
    -- Se não houver registros, retorna 2 (ISNULL(MAX, 1) + 1)
    SELECT 
        ISNULL(MAX(NrSeqControle), 1) + 1 AS NrSeqControle
    FROM SOFTRAN_BRASILMAXI..GTCCONCE (NOLOCK)
END

GO

PRINT 'Procedure P_NR_SEQ_CONTROLE_MAX_OBTER alterada com sucesso!';
PRINT 'O parâmetro @CdEmpresa foi removido e o filtro WHERE foi desconsiderado.';
PRINT 'A procedure agora busca o MAX(NrSeqControle) global de todas as empresas.';
GO

