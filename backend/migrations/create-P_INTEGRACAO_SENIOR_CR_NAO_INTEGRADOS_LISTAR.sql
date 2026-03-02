-- ============================================
-- Migration: Criar Procedure P_INTEGRACAO_SENIOR_CR_NAO_INTEGRADOS_LISTAR
-- Descrição: Lista CT-e não integrados (processed = 0) seguindo o padrão do código C# original
-- Banco: AFS_INTEGRADOR
-- Baseado no método ListarContasReceberNaoIntegradas() do CteRepository-worker.txt
-- ============================================

USE [AFS_INTEGRADOR]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- Verificar se a procedure já existe
IF OBJECT_ID('dbo.P_INTEGRACAO_SENIOR_CR_NAO_INTEGRADOS_LISTAR', 'P') IS NOT NULL
BEGIN
    PRINT 'Procedure P_INTEGRACAO_SENIOR_CR_NAO_INTEGRADOS_LISTAR já existe. Removendo para recriar...';
    DROP PROCEDURE dbo.P_INTEGRACAO_SENIOR_CR_NAO_INTEGRADOS_LISTAR;
END
GO

PRINT 'Criando procedure P_INTEGRACAO_SENIOR_CR_NAO_INTEGRADOS_LISTAR...';
GO

-- ============================================
-- Procedure: P_INTEGRACAO_SENIOR_CR_NAO_INTEGRADOS_LISTAR
-- Descrição: Lista CT-e não integrados (processed = 0)
-- Retorna: id, external_id, Status, XML, Processado
-- Baseado no código C# original: ListarContasReceberNaoIntegradas()
-- ============================================
CREATE PROCEDURE [dbo].[P_INTEGRACAO_SENIOR_CR_NAO_INTEGRADOS_LISTAR]
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Buscar CT-e não processados (processed = 0)
    -- Ordenado por created_at ASC para processar os mais antigos primeiro
    SELECT 
        id,
        external_id,
        status AS Status,
        xml AS XML,
        processed AS Processado
    FROM dbo.ctes WITH (NOLOCK)
    WHERE processed = 0
    ORDER BY created_at ASC;
END
GO

PRINT 'Procedure P_INTEGRACAO_SENIOR_CR_NAO_INTEGRADOS_LISTAR criada com sucesso!';
GO

