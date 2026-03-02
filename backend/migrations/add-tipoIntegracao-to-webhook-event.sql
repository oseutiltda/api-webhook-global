-- ============================================
-- Migration: Adicionar campo tipoIntegracao à tabela WebhookEvent
-- Descrição: Adiciona campo para identificar de qual serviço é o evento (Web API ou Worker)
-- ============================================

USE AFS_INTEGRADOR;
GO

-- Adicionar campo tipoIntegracao se não existir
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'WebhookEvent' 
    AND COLUMN_NAME = 'tipoIntegracao'
)
BEGIN
    ALTER TABLE dbo.WebhookEvent
    ADD tipoIntegracao NVARCHAR(50) NULL;
    
    PRINT 'Campo tipoIntegracao adicionado';
END
GO

-- Criar índice para o campo tipoIntegracao
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes 
    WHERE name = 'IX_WebhookEvent_TipoIntegracao' 
    AND object_id = OBJECT_ID('dbo.WebhookEvent')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_WebhookEvent_TipoIntegracao 
    ON dbo.WebhookEvent (tipoIntegracao)
    WHERE tipoIntegracao IS NOT NULL;
    
    PRINT 'Índice IX_WebhookEvent_TipoIntegracao criado';
END
GO

PRINT 'Migration concluída com sucesso!';
GO

