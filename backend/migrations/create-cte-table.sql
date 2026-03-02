-- ============================================
-- Migration: Criar tabela CT-e
-- Descrição: Cria a tabela cte para armazenar CT-e recebidos via API
-- Banco: AFS_INTEGRADOR
-- ============================================

USE AFS_INTEGRADOR;
GO

-- Verificar se a tabela já existe
IF OBJECT_ID('dbo.cte', 'U') IS NOT NULL
BEGIN
    PRINT 'Tabela dbo.cte já existe.';
    SELECT 'Tabela já existe' AS Status;
    RETURN;
END
GO

PRINT 'Criando tabela dbo.cte...';
GO

-- Criar a tabela cte
CREATE TABLE dbo.cte (
    id                  INT             IDENTITY(1,1) PRIMARY KEY,
    external_id         INT             NOT NULL,
    authorization_number INT            NOT NULL,
    status              NVARCHAR(50)    NOT NULL,
    xml                 NTEXT           NOT NULL,
    event_xml           NTEXT           NULL,
    processed           BIT             NOT NULL DEFAULT 0,
    created_at          DATETIME        NOT NULL DEFAULT GETDATE(),
    updated_at          DATETIME        NOT NULL DEFAULT GETDATE()
);
GO

-- Criar índices
CREATE NONCLUSTERED INDEX IX_cte_external_id 
    ON dbo.cte (external_id);
GO

CREATE NONCLUSTERED INDEX IX_cte_authorization_number 
    ON dbo.cte (authorization_number);
GO

CREATE NONCLUSTERED INDEX IX_cte_status 
    ON dbo.cte (status);
GO

CREATE NONCLUSTERED INDEX IX_cte_processed 
    ON dbo.cte (processed);
GO

-- Criar índice composto para busca rápida
CREATE NONCLUSTERED INDEX IX_cte_external_auth_status 
    ON dbo.cte (external_id, authorization_number, status);
GO

PRINT 'Tabela dbo.cte criada com sucesso!';
SELECT 'Tabela criada com sucesso' AS Status;
GO

