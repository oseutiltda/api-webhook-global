-- ============================================
-- Migration: Adicionar campos de integração à tabela WebhookEvent
-- Descrição: Adiciona campos para monitoramento de status de integração com Senior
-- ============================================

USE AFS_INTEGRADOR;
GO

-- Adicionar novos campos se não existirem
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'WebhookEvent' 
    AND COLUMN_NAME = 'integrationStatus'
)
BEGIN
    ALTER TABLE dbo.WebhookEvent
    ADD integrationStatus NVARCHAR(50) NULL;
    
    PRINT 'Campo integrationStatus adicionado';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'WebhookEvent' 
    AND COLUMN_NAME = 'processingTimeMs'
)
BEGIN
    ALTER TABLE dbo.WebhookEvent
    ADD processingTimeMs INT NULL;
    
    PRINT 'Campo processingTimeMs adicionado';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'WebhookEvent' 
    AND COLUMN_NAME = 'integrationTimeMs'
)
BEGIN
    ALTER TABLE dbo.WebhookEvent
    ADD integrationTimeMs INT NULL;
    
    PRINT 'Campo integrationTimeMs adicionado';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'WebhookEvent' 
    AND COLUMN_NAME = 'seniorId'
)
BEGIN
    ALTER TABLE dbo.WebhookEvent
    ADD seniorId NVARCHAR(255) NULL;
    
    PRINT 'Campo seniorId adicionado';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'WebhookEvent' 
    AND COLUMN_NAME = 'metadata'
)
BEGIN
    ALTER TABLE dbo.WebhookEvent
    ADD metadata NVARCHAR(2000) NULL;
    
    PRINT 'Campo metadata adicionado';
END
GO

-- Criar índices para os novos campos
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes 
    WHERE name = 'IX_WebhookEvent_IntegrationStatus' 
    AND object_id = OBJECT_ID('dbo.WebhookEvent')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_WebhookEvent_IntegrationStatus 
    ON dbo.WebhookEvent (integrationStatus)
    WHERE integrationStatus IS NOT NULL;
    
    PRINT 'Índice IX_WebhookEvent_IntegrationStatus criado';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes 
    WHERE name = 'IX_WebhookEvent_Source' 
    AND object_id = OBJECT_ID('dbo.WebhookEvent')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_WebhookEvent_Source 
    ON dbo.WebhookEvent (source);
    
    PRINT 'Índice IX_WebhookEvent_Source criado';
END
GO

PRINT 'Migration concluída com sucesso!';
GO

