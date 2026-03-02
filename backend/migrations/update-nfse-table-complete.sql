-- ============================================
-- Migration: Atualizar tabela NFSe com todos os campos necessários
-- Descrição: Adiciona/atualiza todos os campos usados no processamento de NFSe
-- Banco: AFS_INTEGRADOR
-- ============================================

USE AFS_INTEGRADOR;
GO

-- Verificar se a tabela existe
IF OBJECT_ID('dbo.nfse', 'U') IS NULL
BEGIN
    PRINT 'ERRO: Tabela dbo.nfse não existe. Execute primeiro o script de criação da tabela.';
    RETURN;
END
GO

PRINT 'Iniciando atualização da tabela dbo.nfse...';
GO

-- ============================================
-- Campos principais de identificação
-- ============================================

-- id (chave primária) - deve existir, apenas verificar
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'id'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD id INT IDENTITY(1,1) PRIMARY KEY;
    PRINT 'Campo id adicionado (PRIMARY KEY)';
END
GO

-- external_id
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'external_id'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD external_id INT NULL;
    PRINT 'Campo external_id adicionado';
END
GO

-- NumeroNfse (obrigatório)
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'NumeroNfse'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD NumeroNfse INT NOT NULL;
    PRINT 'Campo NumeroNfse adicionado';
END
GO

-- CodigoVerificacao
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'CodigoVerificacao'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD CodigoVerificacao VARCHAR(100) NULL;
    PRINT 'Campo CodigoVerificacao adicionado';
END
GO

-- ============================================
-- Campos de datas
-- ============================================

-- DataEmissao
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'DataEmissao'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD DataEmissao DATETIME NULL;
    PRINT 'Campo DataEmissao adicionado';
END
GO

-- DataEmissaoRps
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'DataEmissaoRps'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD DataEmissaoRps DATETIME NULL;
    PRINT 'Campo DataEmissaoRps adicionado';
END
GO

-- DtCompetencia
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'DtCompetencia'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD DtCompetencia DATETIME NULL;
    PRINT 'Campo DtCompetencia adicionado';
END
GO

-- DataDeAlteracao
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'DataDeAlteracao'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD DataDeAlteracao DATETIME NULL;
    PRINT 'Campo DataDeAlteracao adicionado';
END
GO

-- DataCancelamento
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'DataCancelamento'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD DataCancelamento DATETIME NULL;
    PRINT 'Campo DataCancelamento adicionado';
END
GO

-- DataPrazo
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'DataPrazo'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD DataPrazo DATETIME NULL;
    PRINT 'Campo DataPrazo adicionado';
END
GO

-- created_at
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'created_at'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD created_at DATETIME NULL DEFAULT GETDATE();
    PRINT 'Campo created_at adicionado';
END
GO

-- updated_at
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'updated_at'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD updated_at DATETIME NULL;
    PRINT 'Campo updated_at adicionado';
END
GO

-- ============================================
-- Campos de RPS
-- ============================================

-- NumeroIdentificacaoRps
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'NumeroIdentificacaoRps'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD NumeroIdentificacaoRps INT NULL;
    PRINT 'Campo NumeroIdentificacaoRps adicionado';
END
GO

-- SerieIdentificacaoRps
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'SerieIdentificacaoRps'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD SerieIdentificacaoRps VARCHAR(100) NULL;
    PRINT 'Campo SerieIdentificacaoRps adicionado';
END
GO

-- TipoIdentificacaoRps
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'TipoIdentificacaoRps'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD TipoIdentificacaoRps INT NULL;
    PRINT 'Campo TipoIdentificacaoRps adicionado';
END
GO

-- ============================================
-- Campos de operação
-- ============================================

-- NaturezaOperacao
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'NaturezaOperacao'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD NaturezaOperacao INT NULL;
    PRINT 'Campo NaturezaOperacao adicionado';
END
GO

-- OptanteSimplesNacional
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'OptanteSimplesNacional'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD OptanteSimplesNacional INT NULL;
    PRINT 'Campo OptanteSimplesNacional adicionado';
END
GO

-- IncentivadorCultural
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'IncentivadorCultural'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD IncentivadorCultural INT NULL;
    PRINT 'Campo IncentivadorCultural adicionado';
END
GO

-- ============================================
-- Campos de valores (DECIMAL(18,2))
-- ============================================

-- ValorServicos
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'ValorServicos'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD ValorServicos DECIMAL(18,2) NULL;
    PRINT 'Campo ValorServicos adicionado';
END
GO

-- ValorDeducoes
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'ValorDeducoes'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD ValorDeducoes DECIMAL(18,2) NULL;
    PRINT 'Campo ValorDeducoes adicionado';
END
GO

-- ValorIss
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'ValorIss'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD ValorIss DECIMAL(18,2) NULL;
    PRINT 'Campo ValorIss adicionado';
END
GO

-- ValorIssRetido
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'ValorIssRetido'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD ValorIssRetido DECIMAL(18,2) NULL;
    PRINT 'Campo ValorIssRetido adicionado';
END
GO

-- BaseCalculo
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'BaseCalculo'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD BaseCalculo DECIMAL(18,2) NULL;
    PRINT 'Campo BaseCalculo adicionado';
END
GO

-- Aliquota
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'Aliquota'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD Aliquota DECIMAL(18,2) NULL;
    PRINT 'Campo Aliquota adicionado';
END
GO

-- ValorLiquidoNfse
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'ValorLiquidoNfse'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD ValorLiquidoNfse DECIMAL(18,2) NULL;
    PRINT 'Campo ValorLiquidoNfse adicionado';
END
GO

-- ValorCredito
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'ValorCredito'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD ValorCredito DECIMAL(18,2) NULL;
    PRINT 'Campo ValorCredito adicionado';
END
GO

-- ItemListaServico
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'ItemListaServico'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD ItemListaServico DECIMAL(18,2) NULL;
    PRINT 'Campo ItemListaServico adicionado';
END
GO

-- ============================================
-- Campos de tributação
-- ============================================

-- IssRetido
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'IssRetido'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD IssRetido INT NULL;
    PRINT 'Campo IssRetido adicionado';
END
GO

-- CdTributacaoMunicipio
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'CdTributacaoMunicipio'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD CdTributacaoMunicipio VARCHAR(50) NULL;
    PRINT 'Campo CdTributacaoMunicipio adicionado';
END
GO

-- Discriminacao
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'Discriminacao'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD Discriminacao VARCHAR(255) NULL;
    PRINT 'Campo Discriminacao adicionado';
END
GO

-- CodigoMunicipio
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'CodigoMunicipio'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD CodigoMunicipio INT NULL;
    PRINT 'Campo CodigoMunicipio adicionado';
END
GO

-- ============================================
-- Campos de prestador (obrigatórios)
-- ============================================

-- corporation_id
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'corporation_id'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD corporation_id INT NOT NULL DEFAULT 0;
    PRINT 'Campo corporation_id adicionado';
END
GO

-- external_idPrestador
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'external_idPrestador'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD external_idPrestador INT NOT NULL DEFAULT 0;
    PRINT 'Campo external_idPrestador adicionado';
END
GO

-- person_idPrestador
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'person_idPrestador'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD person_idPrestador INT NOT NULL DEFAULT 0;
    PRINT 'Campo person_idPrestador adicionado';
END
GO

-- CnpjIdentPrestador (obrigatório)
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'CnpjIdentPrestador'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD CnpjIdentPrestador VARCHAR(14) NOT NULL DEFAULT '';
    PRINT 'Campo CnpjIdentPrestador adicionado';
END
GO

-- InscMunicipalPrestador
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'InscMunicipalPrestador'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD InscMunicipalPrestador VARCHAR(100) NULL;
    PRINT 'Campo InscMunicipalPrestador adicionado';
END
GO

-- RazaoSocialPrestador (obrigatório)
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'RazaoSocialPrestador'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD RazaoSocialPrestador VARCHAR(255) NOT NULL DEFAULT '';
    PRINT 'Campo RazaoSocialPrestador adicionado';
END
GO

-- NomeFantasiaPrestador
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'NomeFantasiaPrestador'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD NomeFantasiaPrestador VARCHAR(255) NULL;
    PRINT 'Campo NomeFantasiaPrestador adicionado';
END
GO

-- CodEnderecoPrestador
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'CodEnderecoPrestador'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD CodEnderecoPrestador INT NULL;
    PRINT 'Campo CodEnderecoPrestador adicionado';
END
GO

-- CodContatoPrestador
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'CodContatoPrestador'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD CodContatoPrestador INT NULL;
    PRINT 'Campo CodContatoPrestador adicionado';
END
GO

-- ============================================
-- Campos de tomador
-- ============================================

-- customer_id
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'customer_id'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD customer_id INT NOT NULL DEFAULT 0;
    PRINT 'Campo customer_id adicionado';
END
GO

-- external_idTomador
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'external_idTomador'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD external_idTomador INT NOT NULL DEFAULT 0;
    PRINT 'Campo external_idTomador adicionado';
END
GO

-- CnpjIdentTomador
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'CnpjIdentTomador'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD CnpjIdentTomador VARCHAR(14) NULL;
    PRINT 'Campo CnpjIdentTomador adicionado';
END
GO

-- CpfIdentTomador
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'CpfIdentTomador'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD CpfIdentTomador VARCHAR(11) NULL;
    PRINT 'Campo CpfIdentTomador adicionado';
END
GO

-- InscMunicipalTomador
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'InscMunicipalTomador'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD InscMunicipalTomador VARCHAR(100) NULL;
    PRINT 'Campo InscMunicipalTomador adicionado';
END
GO

-- RazaoSocialTomador
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'RazaoSocialTomador'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD RazaoSocialTomador VARCHAR(255) NULL;
    PRINT 'Campo RazaoSocialTomador adicionado';
END
GO

-- CodEnderecoTomador
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'CodEnderecoTomador'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD CodEnderecoTomador INT NULL;
    PRINT 'Campo CodEnderecoTomador adicionado';
END
GO

-- CodContatoTomador
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'CodContatoTomador'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD CodContatoTomador INT NULL;
    PRINT 'Campo CodContatoTomador adicionado';
END
GO

-- ============================================
-- Campos de órgão gerador
-- ============================================

-- CdMunicipioOrgaoGerador
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'CdMunicipioOrgaoGerador'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD CdMunicipioOrgaoGerador INT NULL;
    PRINT 'Campo CdMunicipioOrgaoGerador adicionado';
END
GO

-- UFOrgaoGerador
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'UFOrgaoGerador'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD UFOrgaoGerador VARCHAR(50) NULL;
    PRINT 'Campo UFOrgaoGerador adicionado';
END
GO

-- ============================================
-- Campos de controle e status
-- ============================================

-- status (obrigatório)
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'status'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD status NVARCHAR(20) NOT NULL DEFAULT 'pending';
    PRINT 'Campo status adicionado';
END
GO

-- error_message
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'error_message'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD error_message NTEXT NULL;
    PRINT 'Campo error_message adicionado';
END
GO

-- processed (obrigatório)
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'processed'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD processed BIT NOT NULL DEFAULT 0;
    PRINT 'Campo processed adicionado';
END
GO

-- cancelado
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'cancelado'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD cancelado BIT NULL DEFAULT 0;
    PRINT 'Campo cancelado adicionado';
END
GO

-- Obscancelado
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'Obscancelado'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD Obscancelado VARCHAR(255) NULL;
    PRINT 'Campo Obscancelado adicionado';
END
GO

-- ============================================
-- Campos de endereço do prestador
-- ============================================

-- Prestador_Logradouro
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'Prestador_Logradouro'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD Prestador_Logradouro VARCHAR(255) NULL;
    PRINT 'Campo Prestador_Logradouro adicionado';
END
GO

-- Prestador_Numero
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'Prestador_Numero'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD Prestador_Numero VARCHAR(50) NULL;
    PRINT 'Campo Prestador_Numero adicionado';
END
GO

-- Prestador_Complemento
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'Prestador_Complemento'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD Prestador_Complemento VARCHAR(255) NULL;
    PRINT 'Campo Prestador_Complemento adicionado';
END
GO

-- Prestador_Bairro
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'Prestador_Bairro'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD Prestador_Bairro VARCHAR(255) NULL;
    PRINT 'Campo Prestador_Bairro adicionado';
END
GO

-- Prestador_CodigoMunicipio
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'Prestador_CodigoMunicipio'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD Prestador_CodigoMunicipio INT NULL;
    PRINT 'Campo Prestador_CodigoMunicipio adicionado';
END
GO

-- Prestador_UF
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'Prestador_UF'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD Prestador_UF VARCHAR(10) NULL;
    PRINT 'Campo Prestador_UF adicionado';
END
GO

-- Prestador_CEP
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'Prestador_CEP'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD Prestador_CEP VARCHAR(20) NULL;
    PRINT 'Campo Prestador_CEP adicionado';
END
GO

-- Prestador_Telefone
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'Prestador_Telefone'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD Prestador_Telefone VARCHAR(50) NULL;
    PRINT 'Campo Prestador_Telefone adicionado';
END
GO

-- Prestador_Email
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'Prestador_Email'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD Prestador_Email VARCHAR(255) NULL;
    PRINT 'Campo Prestador_Email adicionado';
END
GO

-- ============================================
-- Campos de endereço do tomador
-- ============================================

-- Tomador_Logradouro
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'Tomador_Logradouro'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD Tomador_Logradouro VARCHAR(255) NULL;
    PRINT 'Campo Tomador_Logradouro adicionado';
END
GO

-- Tomador_Numero
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'Tomador_Numero'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD Tomador_Numero VARCHAR(50) NULL;
    PRINT 'Campo Tomador_Numero adicionado';
END
GO

-- Tomador_Complemento
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'Tomador_Complemento'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD Tomador_Complemento VARCHAR(255) NULL;
    PRINT 'Campo Tomador_Complemento adicionado';
END
GO

-- Tomador_Bairro
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'Tomador_Bairro'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD Tomador_Bairro VARCHAR(255) NULL;
    PRINT 'Campo Tomador_Bairro adicionado';
END
GO

-- Tomador_CodigoMunicipio
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'Tomador_CodigoMunicipio'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD Tomador_CodigoMunicipio INT NULL;
    PRINT 'Campo Tomador_CodigoMunicipio adicionado';
END
GO

-- Tomador_UF
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'Tomador_UF'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD Tomador_UF VARCHAR(10) NULL;
    PRINT 'Campo Tomador_UF adicionado';
END
GO

-- Tomador_CEP
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'Tomador_CEP'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD Tomador_CEP VARCHAR(20) NULL;
    PRINT 'Campo Tomador_CEP adicionado';
END
GO

-- Tomador_Telefone
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'Tomador_Telefone'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD Tomador_Telefone VARCHAR(50) NULL;
    PRINT 'Campo Tomador_Telefone adicionado';
END
GO

-- Tomador_Email
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'Tomador_Email'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD Tomador_Email VARCHAR(255) NULL;
    PRINT 'Campo Tomador_Email adicionado';
END
GO

-- ============================================
-- Campos de logística de frete
-- ============================================

-- PesoReal
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'PesoReal'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD PesoReal DECIMAL(18,2) NULL;
    PRINT 'Campo PesoReal adicionado';
END
GO

-- PesoCubado
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'PesoCubado'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD PesoCubado DECIMAL(18,2) NULL;
    PRINT 'Campo PesoCubado adicionado';
END
GO

-- QuantidadeVolumes
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'QuantidadeVolumes'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD QuantidadeVolumes INT NULL;
    PRINT 'Campo QuantidadeVolumes adicionado';
END
GO

-- ValorProduto
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'ValorProduto'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD ValorProduto DECIMAL(18,2) NULL;
    PRINT 'Campo ValorProduto adicionado';
END
GO

-- ValorNota
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'ValorNota'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD ValorNota DECIMAL(18,2) NULL;
    PRINT 'Campo ValorNota adicionado';
END
GO

-- ValorFretePeso
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'ValorFretePeso'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD ValorFretePeso DECIMAL(18,2) NULL;
    PRINT 'Campo ValorFretePeso adicionado';
END
GO

-- ValorAdv
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'ValorAdv'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD ValorAdv DECIMAL(18,2) NULL;
    PRINT 'Campo ValorAdv adicionado';
END
GO

-- ValorOutros
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'ValorOutros'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD ValorOutros DECIMAL(18,2) NULL;
    PRINT 'Campo ValorOutros adicionado';
END
GO

-- ComentarioFrete
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'ComentarioFrete'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD ComentarioFrete NTEXT NULL;
    PRINT 'Campo ComentarioFrete adicionado';
END
GO

-- ============================================
-- Campos de alteração e cancelamento
-- ============================================

-- UsuarioAlteracao
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'UsuarioAlteracao'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD UsuarioAlteracao VARCHAR(255) NULL;
    PRINT 'Campo UsuarioAlteracao adicionado';
END
GO

-- MotivoCancelamento
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'MotivoCancelamento'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD MotivoCancelamento VARCHAR(255) NULL;
    PRINT 'Campo MotivoCancelamento adicionado';
END
GO

-- FilialCancelamento
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'FilialCancelamento'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD FilialCancelamento VARCHAR(100) NULL;
    PRINT 'Campo FilialCancelamento adicionado';
END
GO

-- ============================================
-- Campos de CNPJs relacionados
-- ============================================

-- CnpjConsignatario
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'CnpjConsignatario'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD CnpjConsignatario VARCHAR(20) NULL;
    PRINT 'Campo CnpjConsignatario adicionado';
END
GO

-- CnpjRedespacho
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'CnpjRedespacho'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD CnpjRedespacho VARCHAR(255) NULL;
    PRINT 'Campo CnpjRedespacho adicionado';
END
GO

-- CnpjExpedidor
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'CnpjExpedidor'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD CnpjExpedidor VARCHAR(20) NULL;
    PRINT 'Campo CnpjExpedidor adicionado';
END
GO

-- CnpjRemetente
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'CnpjRemetente'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD CnpjRemetente VARCHAR(20) NULL;
    PRINT 'Campo CnpjRemetente adicionado';
END
GO

-- CepRemetente
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'CepRemetente'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD CepRemetente VARCHAR(20) NULL;
    PRINT 'Campo CepRemetente adicionado';
END
GO

-- CnpjDestinatario
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'CnpjDestinatario'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD CnpjDestinatario VARCHAR(20) NULL;
    PRINT 'Campo CnpjDestinatario adicionado';
END
GO

-- CepDestinatario
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'CepDestinatario'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD CepDestinatario VARCHAR(20) NULL;
    PRINT 'Campo CepDestinatario adicionado';
END
GO

-- LogradouroDestinatario
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'LogradouroDestinatario'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD LogradouroDestinatario VARCHAR(255) NULL;
    PRINT 'Campo LogradouroDestinatario adicionado';
END
GO

-- ============================================
-- Campos de PIS/COFINS
-- ============================================

-- ValorBaseCalculoPIS
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'ValorBaseCalculoPIS'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD ValorBaseCalculoPIS DECIMAL(18,2) NULL;
    PRINT 'Campo ValorBaseCalculoPIS adicionado';
END
GO

-- AliqPIS
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'AliqPIS'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD AliqPIS DECIMAL(18,2) NULL;
    PRINT 'Campo AliqPIS adicionado';
END
GO

-- ValorPIS
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'ValorPIS'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD ValorPIS DECIMAL(18,2) NULL;
    PRINT 'Campo ValorPIS adicionado';
END
GO

-- AliqCOFINS
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'AliqCOFINS'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD AliqCOFINS DECIMAL(18,2) NULL;
    PRINT 'Campo AliqCOFINS adicionado';
END
GO

-- ValorCOFINS
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'ValorCOFINS'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD ValorCOFINS DECIMAL(18,2) NULL;
    PRINT 'Campo ValorCOFINS adicionado';
END
GO

-- ============================================
-- Campos de entrega
-- ============================================

-- DiasEntrega
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'DiasEntrega'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD DiasEntrega INT NULL;
    PRINT 'Campo DiasEntrega adicionado';
END
GO

-- ============================================
-- Campo de controle JSON original
-- ============================================

-- NumeroJsonOriginal
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME = 'nfse' 
    AND COLUMN_NAME = 'NumeroJsonOriginal'
)
BEGIN
    ALTER TABLE dbo.nfse
    ADD NumeroJsonOriginal INT NULL;
    PRINT 'Campo NumeroJsonOriginal adicionado';
END
GO

-- ============================================
-- Criar índices para melhor performance
-- ============================================

-- Índice em NumeroNfse (usado frequentemente)
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes 
    WHERE name = 'IX_nfse_NumeroNfse' 
    AND object_id = OBJECT_ID('dbo.nfse')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_nfse_NumeroNfse 
    ON dbo.nfse (NumeroNfse);
    PRINT 'Índice IX_nfse_NumeroNfse criado';
END
GO

-- Índice em CnpjIdentPrestador (usado para busca)
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes 
    WHERE name = 'IX_nfse_CnpjIdentPrestador' 
    AND object_id = OBJECT_ID('dbo.nfse')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_nfse_CnpjIdentPrestador 
    ON dbo.nfse (CnpjIdentPrestador);
    PRINT 'Índice IX_nfse_CnpjIdentPrestador criado';
END
GO

-- Índice em processed (usado para buscar pendentes)
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes 
    WHERE name = 'IX_nfse_processed' 
    AND object_id = OBJECT_ID('dbo.nfse')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_nfse_processed 
    ON dbo.nfse (processed)
    WHERE processed = 0;
    PRINT 'Índice IX_nfse_processed criado';
END
GO

-- Índice em cancelado (usado para filtrar)
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes 
    WHERE name = 'IX_nfse_cancelado' 
    AND object_id = OBJECT_ID('dbo.nfse')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_nfse_cancelado 
    ON dbo.nfse (cancelado)
    WHERE cancelado = 0;
    PRINT 'Índice IX_nfse_cancelado criado';
END
GO

-- Índice composto para busca de pendentes (processed + cancelado)
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes 
    WHERE name = 'IX_nfse_processed_cancelado' 
    AND object_id = OBJECT_ID('dbo.nfse')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_nfse_processed_cancelado 
    ON dbo.nfse (processed, cancelado)
    WHERE processed = 0 AND cancelado = 0;
    PRINT 'Índice IX_nfse_processed_cancelado criado';
END
GO

-- Índice em customer_id (usado para relacionamentos)
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes 
    WHERE name = 'IX_nfse_customer_id' 
    AND object_id = OBJECT_ID('dbo.nfse')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_nfse_customer_id 
    ON dbo.nfse (customer_id);
    PRINT 'Índice IX_nfse_customer_id criado';
END
GO

-- Índice em corporation_id (usado para relacionamentos)
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes 
    WHERE name = 'IX_nfse_corporation_id' 
    AND object_id = OBJECT_ID('dbo.nfse')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_nfse_corporation_id 
    ON dbo.nfse (corporation_id);
    PRINT 'Índice IX_nfse_corporation_id criado';
END
GO

PRINT '';
PRINT '============================================';
PRINT 'Migration concluída com sucesso!';
PRINT '============================================';
PRINT '';
PRINT 'Todos os campos necessários foram adicionados à tabela dbo.nfse.';
PRINT 'Índices criados para melhorar a performance das consultas.';
PRINT '';
GO

