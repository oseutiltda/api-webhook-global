import { PrismaClient, Prisma } from '@prisma/client';
import { logger } from '../utils/logger';
import { obterProximoNrSeqControle } from '../utils/nrSeqControle';
import { env } from '../config/env';

// Função para traduzir erros SQL para linguagem mais simples
function translateError(errorMessage: string, numeroDocumento?: number): string {
  let translated = '';
  const docInfo = numeroDocumento ? ` (Documento: ${numeroDocumento})` : '';

  // Erros de chave duplicada
  if (
    errorMessage.includes('SISNFSNE0') ||
    errorMessage.includes('duplicate key') ||
    errorMessage.includes('2627')
  ) {
    if (errorMessage.includes('SISNFSNE')) {
      translated = `O registro já existe no sistema${docInfo}. A nota fiscal já foi processada anteriormente.`;
    } else {
      translated = `Registro duplicado${docInfo}. Este documento já foi inserido no banco de dados.`;
    }
  }
  // Erro de objeto não encontrado
  else if (errorMessage.includes('Invalid object name') || errorMessage.includes('208')) {
    translated = `Tabela não encontrada no banco de dados${docInfo}. Verifique se a tabela existe.`;
  }
  // Erro de conversão de tipo
  else if (errorMessage.includes('Conversion failed') || errorMessage.includes('245')) {
    translated = `Erro de conversão de dados${docInfo}. Um valor não está no formato correto.`;
  }
  // Erro de valor nulo
  else if (errorMessage.includes('NULL') || errorMessage.includes('515')) {
    translated = `Campo obrigatório não preenchido${docInfo}. Verifique se todos os dados necessários foram informados.`;
  }
  // Erro de violação de constraint
  else if (errorMessage.includes('constraint') || errorMessage.includes('547')) {
    translated = `Violação de regra do banco de dados${docInfo}. Os dados não atendem às regras de validação.`;
  }
  // Erro de timeout
  else if (errorMessage.includes('timeout') || errorMessage.includes('ETIMEDOUT')) {
    translated = `Tempo de espera esgotado${docInfo}. A operação demorou muito para ser concluída.`;
  }
  // Erro de conexão
  else if (errorMessage.includes('connection') || errorMessage.includes('ECONNREFUSED')) {
    translated = `Erro de conexão com o banco de dados${docInfo}. Não foi possível conectar ao servidor.`;
  }
  // Erro genérico
  else {
    // Tentar extrair informações úteis da mensagem original
    const match = errorMessage.match(/Message:\s*(.+?)(?:\n|$)/i);
    if (match) {
      translated = `Erro no processamento${docInfo}: ${match[1]}`;
    } else {
      translated = `Erro desconhecido durante o processamento${docInfo}. Entre em contato com o suporte técnico.`;
    }
  }

  return translated;
}

const parseNumber = (value: string | undefined, fallback: number) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};

// Garante que um valor seja passado como INT no SQL Server (evita erro de conversão numeric to int)
const ensureInt = (value: number | null | undefined, defaultValue: number = 0): string => {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return `CAST(${defaultValue} AS INT)`;
  }
  const intValue = Math.floor(Number(value));
  // Para valores dentro do range de INT, usar CAST explícito
  if (intValue >= -2147483648 && intValue <= 2147483647) {
    return `CAST(${intValue} AS INT)`;
  }
  // Para valores maiores, usar BIGINT
  return `CAST(${intValue} AS BIGINT)`;
};

const NFSE_BATCH_SIZE = parseNumber(process.env.NFSE_WORKER_BATCH_SIZE, 5);
const NFSE_CD_EMPRESA = parseNumber(process.env.NFSE_CD_EMPRESA, 300);
const NFSE_CD_EMPRESA_TABELA = parseNumber(process.env.NFSE_CD_EMPRESA_TABELA, 100);
const NFSE_CD_EMPRESA_DESTINO = parseNumber(process.env.NFSE_CD_EMPRESA_DESTINO, NFSE_CD_EMPRESA);
const NFSE_CD_EMPRESA_ENTREGA = parseNumber(process.env.NFSE_CD_EMPRESA_ENTREGA, 100);
const NFSE_NR_SERIE = parseNumber(process.env.NFSE_NR_SERIE, 25);
const NFSE_SOURCE_DATABASE = process.env.NFSE_SOURCE_DATABASE || 'AFS_INTEGRADOR';
// Banco de destino onde estão as tabelas GTCConhe, GTCCONIA, sisnfsne
// Usa SENIOR_DATABASE como padrão se NFSE_DESTINATION_DATABASE não estiver definido
const NFSE_DESTINATION_DATABASE = process.env.NFSE_DESTINATION_DATABASE || env.SENIOR_DATABASE;
const NFSE_TABLE = `[${NFSE_SOURCE_DATABASE}].[dbo].[nfse]`;
const NFSE_DEST_PREFIX = `[${NFSE_DESTINATION_DATABASE}].[dbo]`;
const IS_POSTGRES = env.DATABASE_URL.startsWith('postgresql://');
const ENABLE_SQLSERVER_LEGACY = env.ENABLE_SQLSERVER_LEGACY;

/**
 * Calcula o CdEmpresa baseado no CNPJ do prestador
 * REGRA: Aplicada consistentemente para todas as tabelas
 * - CNPJ '59530832000162' -> 100
 * - CNPJ '16836335000184' -> 300
 * - Outros CNPJs -> usa variável de ambiente NFSE_CD_EMPRESA (padrão: 300)
 */
const calcularCdEmpresa = (cnpjPrestador: string | null | undefined): number => {
  if (!cnpjPrestador) {
    return NFSE_CD_EMPRESA;
  }

  const cnpjClean = cnpjPrestador.replace(/\D/g, '');

  if (cnpjClean === '59530832000162') {
    return 100;
  }

  if (cnpjClean === '16836335000184') {
    return 300;
  }

  return NFSE_CD_EMPRESA;
};

type TransactionClient = Prisma.TransactionClient;

const buildInsertScript = (nfseId: number, nrSeqControle: number) => {
  // Usar o nome completo do banco nas referências às tabelas
  const destDb = NFSE_DESTINATION_DATABASE;
  logger.info(
    { destDb, nfseId, nrSeqControle, envValue: process.env.NFSE_DESTINATION_DATABASE },
    'Construindo script SQL',
  );
  return `
SET NOCOUNT ON;

DECLARE @NfseId INT = ${nfseId};
DECLARE @CdEmpresa INT;
-- CdEmpresaTabela: Fixo 100 (variável de ambiente NFSE_CD_EMPRESA_TABELA, padrão: 100)
DECLARE @CdEmpresaTabela INT = ${NFSE_CD_EMPRESA_TABELA};
DECLARE @CdEmpresaDestino INT;
-- CdEmpresaEntrega: Fixo 100 (variável de ambiente NFSE_CD_EMPRESA_ENTREGA, padrão: 100)
DECLARE @CdEmpresaEntrega INT = ${NFSE_CD_EMPRESA_ENTREGA};
DECLARE @NrSerie INT = 25; -- Série fixa para inserção de NFSe

-- NrSeqControle já calculado no código TypeScript (busca MAX entre GTCCONCE e GTCConhe)
DECLARE @NrSeqControle INT = ${nrSeqControle};
DECLARE @CdSequencia INT;

DECLARE 
  @CnpjPrestador NVARCHAR(20),
  @CdInscricao NVARCHAR(20),
  @Data DATETIME,
  @DataEntrega DATETIME,
  @CdRemetente NVARCHAR(20),
  @CdDestinatario NVARCHAR(20),
  @NrCepColeta NVARCHAR(20),
  @NrCepEntrega NVARCHAR(20),
  @QtPeso DECIMAL(18,4),
  @QtPesoCubado DECIMAL(18,4),
  @QtVolume DECIMAL(18,4),
  @VlMercadoria DECIMAL(18,4),
  @VlNFCobrada DECIMAL(18,4),
  @VlTotalPrestacao DECIMAL(18,4),
  @VlBaseCalculo DECIMAL(18,4),
  @VlLiquido DECIMAL(18,4),
  @VlFretePeso DECIMAL(18,4),
  @VlFreteValor DECIMAL(18,4),
  @VlOutros DECIMAL(18,4),
  @VlAliqISS DECIMAL(18,4),
  @VlISS DECIMAL(18,4),
  @NrDoctoFiscal INT,
  @DtCancelamento DATETIME,
  @CdMotivoCancelamento NVARCHAR(255),
  @CdFilialCancelamento NVARCHAR(100),
  @CdConsignatario NVARCHAR(50),
  @CdRedespacho NVARCHAR(50),
  @DiasEntrega INT,
  @VlBaseCalcPis DECIMAL(18,4),
  @VlAliqPis DECIMAL(18,4),
  @VlPis DECIMAL(18,4),
  @VlAliqCofins DECIMAL(18,4),
  @VlCofins DECIMAL(18,4),
  @CdExpedidorCarga NVARCHAR(50),
  @NrNFSe INT,
  @CdVerifNFSe NVARCHAR(100),
  @CdTpDoctoFiscal INT,
  @InConhecimento INT,
  @IssRetido INT,
  @CdVinculacaoISS INT;

SELECT 
  @CnpjPrestador = ISNULL(NULLIF(CnpjIdentPrestador, ''), ''),
  @CdInscricao = ISNULL(NULLIF(CnpjIdentTomador, ''), '0'),
  @Data = ISNULL(DataEmissao, GETDATE()),
  @DataEntrega = ISNULL(DataPrazo, ISNULL(DataEmissao, GETDATE())),
  @CdRemetente = ISNULL(NULLIF(COALESCE(CnpjRemetente, CnpjIdentPrestador), ''), '0'),
  @CdDestinatario = ISNULL(NULLIF(COALESCE(CnpjDestinatario, CnpjIdentTomador), ''), '0'),
  @NrCepColeta = ISNULL(NULLIF(REPLACE(CepRemetente, '-', ''), ''), '00000000'),
  @NrCepEntrega = ISNULL(NULLIF(REPLACE(CepDestinatario, '-', ''), ''), '00000000'),
  @QtPeso = ISNULL(PesoReal, 0),
  @QtPesoCubado = ISNULL(PesoCubado, 0),
  @QtVolume = ISNULL(QuantidadeVolumes, 0),
  @VlMercadoria = ISNULL(ValorProduto, ValorServicos),
  @VlNFCobrada = ISNULL(ValorNota, ValorServicos),
  @VlTotalPrestacao = ISNULL(ValorServicos, 0),
  @VlBaseCalculo = ISNULL(BaseCalculo, 0),
  @VlLiquido = ISNULL(ValorLiquidoNfse, 0),
  @VlFretePeso = ISNULL(ValorFretePeso, 0),
  @VlFreteValor = ISNULL(ValorAdv, 0),
  @VlOutros = ISNULL(ValorOutros, 0),
  @VlAliqISS = ISNULL(Aliquota, 0),
  @VlISS = ISNULL(ValorIss, 0),
  @NrDoctoFiscal = NumeroNfse,
  @DtCancelamento = DataCancelamento,
  @CdMotivoCancelamento = NULLIF(MotivoCancelamento, ''),
  @CdFilialCancelamento = NULLIF(FilialCancelamento, ''),
  @CdConsignatario = NULLIF(CnpjConsignatario, ''),
  @CdRedespacho = CASE WHEN CnpjRedespacho LIKE '%#%' THEN NULL ELSE NULLIF(CnpjRedespacho, '') END,
  @DiasEntrega = ISNULL(DiasEntrega, 1),
  @VlBaseCalcPis = ISNULL(ValorBaseCalculoPIS, 0),
  @VlAliqPis = ISNULL(AliqPIS, 0),
  @VlPis = ISNULL(ValorPIS, 0),
  @VlAliqCofins = ISNULL(AliqCOFINS, 0),
  @VlCofins = ISNULL(ValorCOFINS, 0),
  @CdExpedidorCarga = ISNULL(NULLIF(COALESCE(CnpjRemetente, CnpjIdentPrestador), ''), '0'),
  @NrNFSe = NumeroNfse,
  @CdVerifNFSe = ISNULL(CodigoVerificacao, ''),
  @CdTpDoctoFiscal =
    CASE
      WHEN CnpjIdentPrestador = '59530832000162' THEN 20
      WHEN CnpjIdentPrestador = '16836335000184' THEN 30
      ELSE 30
    END,
  @NrSerie = 25, -- Série fixa para inserção de NFSe (ignorar SerieIdentificacaoRps)
  @IssRetido = ISNULL(IssRetido, 0)
FROM ${NFSE_TABLE} WITH (UPDLOCK)
WHERE id = @NfseId;

-- REGRA: Calcular CdVinculacaoISS na tabela GTCCONIA
-- Se IssRetido = 1 no JSON, então gravar cdvinculacaoISS = 138
-- Caso contrário, gravar NULL
SET @CdVinculacaoISS = CASE WHEN @IssRetido = 1 THEN 138 ELSE NULL END;

-- Verificar se CodigoVerificacao = 'canceled' e ajustar DtCancelamento e InConhecimento
IF UPPER(LTRIM(RTRIM(@CdVerifNFSe))) = 'CANCELED'
BEGIN
  -- Se DtCancelamento for NULL, usar a data atual
  IF @DtCancelamento IS NULL
  BEGIN
    SET @DtCancelamento = GETDATE();
  END
  
  -- Definir InConhecimento como 1 (cancelado)
  SET @InConhecimento = 1;
END
ELSE
BEGIN
  -- Se não estiver cancelado, InConhecimento = 0
  SET @InConhecimento = 0;
END

IF @CdInscricao IS NULL OR @NrDoctoFiscal IS NULL
BEGIN
  THROW 50010, 'NFSe não encontrada para processamento', 1;
END

-- Calcular CdEmpresa baseado no CNPJ do prestador (emitente)
-- REGRA APLICADA PARA TODAS AS TABELAS:
-- - CNPJ '59530832000162' -> CdEmpresa = 100
-- - CNPJ '16836335000184' -> CdEmpresa = 300
-- - Outros CNPJs -> CdEmpresa = variável de ambiente NFSE_CD_EMPRESA (padrão: 300)
SET @CdEmpresa = 
  CASE
    WHEN @CnpjPrestador = '59530832000162' THEN 100
    WHEN @CnpjPrestador = '16836335000184' THEN 300
    ELSE ${NFSE_CD_EMPRESA}
  END;

-- CdEmpresaDestino usa o mesmo valor calculado (mesma regra)
SET @CdEmpresaDestino = @CdEmpresa;

-- Verificar se já existe um registro para esta NFSe (para evitar reprocessamento)
-- Verificação através da combinação única de 5 campos: NumeroNfse + CdEmpresa + CdRemetente + NrSerie + CdTpDoctoFiscal
-- IMPORTANTE: Usar sempre todos os 5 campos para evitar conflitos com CT-e
-- Níveis de verificação:
--   Nível 1: Combinação exata (todos os 5 campos)
--   Nível 2: Alternativa com CdTpDoctoFiscal = 0 (para registros inseridos manualmente)
DECLARE @RegistroJaExiste BIT = 0;
DECLARE @CdSequenciaExistente INT;
DECLARE @NrSeqControleExistente INT;
DECLARE @NrDoctoFiscalExistente INT;
DECLARE @NrNFSeExistente INT;
DECLARE @CdVerifNFSeExistente NVARCHAR(100);

-- Primeiro, tentar encontrar registro existente com a combinação exata
SELECT 
  @CdSequenciaExistente = s.CdSequencia,
  @NrSeqControleExistente = s.NrSeqControle,
  @NrDoctoFiscalExistente = s.NrDoctoFiscal,
  @NrNFSeExistente = s.NrNFSe,
  @CdVerifNFSeExistente = s.CdVerifNFSe
FROM [${destDb}].dbo.sisnfsne s WITH (UPDLOCK, HOLDLOCK)
INNER JOIN [${destDb}].dbo.GTCConhe g WITH (UPDLOCK, HOLDLOCK)
  ON s.NrSeqControle = g.NrSeqControle 
  AND s.CdEmpresa = g.CdEmpresa
WHERE s.CdEmpresa = @CdEmpresa 
  AND s.NrDoctoFiscal = @NrDoctoFiscal
  AND s.NrSerie = @NrSerie
  AND g.CdRemetente = @CdRemetente
  AND g.CdTpDoctoFiscal = @CdTpDoctoFiscal;

-- Se não encontrou com a combinação exata, tentar com CdTpDoctoFiscal = 0
IF @CdSequenciaExistente IS NULL
BEGIN
  SELECT 
    @CdSequenciaExistente = s.CdSequencia,
    @NrSeqControleExistente = s.NrSeqControle,
    @NrDoctoFiscalExistente = s.NrDoctoFiscal,
    @NrNFSeExistente = s.NrNFSe,
    @CdVerifNFSeExistente = s.CdVerifNFSe
  FROM [${destDb}].dbo.sisnfsne s WITH (UPDLOCK, HOLDLOCK)
  INNER JOIN [${destDb}].dbo.GTCConhe g WITH (UPDLOCK, HOLDLOCK)
    ON s.NrSeqControle = g.NrSeqControle 
    AND s.CdEmpresa = g.CdEmpresa
  WHERE s.CdEmpresa = @CdEmpresa 
    AND s.NrDoctoFiscal = @NrDoctoFiscal
    AND s.NrSerie = @NrSerie
    AND g.CdRemetente = @CdRemetente
    AND g.CdTpDoctoFiscal = 0
  ORDER BY s.CdSequencia DESC; -- Pegar o mais recente
END

-- Se encontrou registro existente, apenas atualizar cancelamento se necessário e marcar como processado
IF @CdSequenciaExistente IS NOT NULL
BEGIN
  SET @RegistroJaExiste = 1;
  
  -- Atualizar campos de cancelamento SOMENTE se os dados corresponderem E CodigoVerificacao = 'canceled'
  -- IMPORTANTE: Usar os 5 campos no WHERE do UPDATE para garantir que está atualizando o registro correto
  IF UPPER(LTRIM(RTRIM(@CdVerifNFSe))) = 'CANCELED'
     AND @NrDoctoFiscalExistente = @NrDoctoFiscal
     AND @NrNFSeExistente = @NrNFSe
     AND (@CdVerifNFSeExistente = @CdVerifNFSe OR (@CdVerifNFSeExistente IS NULL AND @CdVerifNFSe IS NULL))
  BEGIN
    -- Atualizar GTCConhe usando os 5 campos no WHERE para garantir que está atualizando o registro correto
    UPDATE [${destDb}].dbo.GTCConhe
    SET 
      DtCancelamento = @DtCancelamento,
      InConhecimento = 1,
      CdMotivoCancelamento = @CdMotivoCancelamento,
      CdFilialCancelamento = @CdFilialCancelamento
    WHERE NrDoctoFiscal = @NrDoctoFiscal
      AND CdEmpresa = @CdEmpresa
      AND CdRemetente = @CdRemetente
      AND NrSerie = @NrSerie
      AND CdTpDoctoFiscal = @CdTpDoctoFiscal;
    
    -- Atualizar também sisnfsne usando JOIN com GTCConhe para garantir que está atualizando o registro correto
    UPDATE s
    SET 
      CdVerifNFSe = 'canceled',
      DtHrRetNFSe = GETDATE()
    FROM [${destDb}].dbo.sisnfsne s
    INNER JOIN [${destDb}].dbo.GTCConhe g
      ON s.NrSeqControle = g.NrSeqControle
      AND s.CdEmpresa = g.CdEmpresa
    WHERE s.NrDoctoFiscal = @NrDoctoFiscal
      AND s.CdEmpresa = @CdEmpresa
      AND s.NrSerie = @NrSerie
      AND g.CdRemetente = @CdRemetente
      AND g.CdTpDoctoFiscal = @CdTpDoctoFiscal;
  END
  
  -- Atualizar a NFSe como processada (já foi inserida anteriormente)
  UPDATE ${NFSE_TABLE}
  SET 
    processed = 1,
    status = 'processed',
    updated_at = GETDATE(),
    error_message = NULL
  WHERE id = @NfseId;
  
  -- Retornar os valores existentes (NÃO reutilizar para novas inserções)
  SELECT @NrSeqControleExistente AS NrSeqControle, @CdSequenciaExistente AS CdSequencia;
  RETURN;
END

-- Se chegou aqui, a NFSe não foi processada ainda - SEMPRE gerar novo NrSeqControle
-- REGRA: O NrSeqControle já foi calculado no código TypeScript antes de construir este script
--        buscando o MAX global entre GTCCONCE (CT-e) e GTCConhe (NFSe), garantindo sequência única
-- NUNCA reutilizar um NrSeqControle existente
-- O valor @NrSeqControle já foi definido como parâmetro do script

-- Calcular novo CdSequencia para sisnfsne
-- REGRA: Sempre gerar novo CdSequencia = MAX(CdSequencia) + 1 global (sem filtro por CdEmpresa)
SELECT @CdSequencia = ISNULL(MAX(CdSequencia), 0) + 1
FROM [${destDb}].dbo.sisnfsne WITH (UPDLOCK, HOLDLOCK);

-- Verificar se o CdSequencia calculado já existe (GLOBALMENTE, pois a PK é somente em CdSequencia)
-- e incrementar até encontrar um disponível (proteção contra inserções simultâneas)
DECLARE @Tentativas INT = 0;
WHILE EXISTS (SELECT 1 FROM [${destDb}].dbo.sisnfsne WITH (UPDLOCK, HOLDLOCK) WHERE CdSequencia = @CdSequencia) AND @Tentativas < 1000
BEGIN
  SET @CdSequencia = @CdSequencia + 1;
  SET @Tentativas = @Tentativas + 1;
END

IF @Tentativas >= 1000
BEGIN
  THROW 50011, 'Não foi possível encontrar um CdSequencia disponível após 1000 tentativas', 1;
END

INSERT INTO [${destDb}].dbo.GTCConhe (
    CdEmpresa,
    NrSeqControle,
    CdInscricao,
    DtEmissao,
    DtEntrega,
    InTipoFatura,
    NrDiasEntrega,
    InTipoFrete,
    CdRemetente,
    CdDestinatario,
    NrCepColeta,
    NrCepEntrega,
    NrCepCalcAte,
    CdNatureza,
    CdEspecie,
    CdTabelaPreco,
    CdTarifa,
    QtPeso,
    QtPesoCubado,
    QtVolume,
    VlMercadoria,
    VlNFCobrada,
    CdTransporte,
    CdOperacao,
    InICMS,
    VlTotalPrestacao,
    VlBaseCalculo,
    VlLiquido,
    InConhecimento,
    InFatura,
    CdMotorista,
    DsPlacaVeiculo,
    VlFretePeso,
    VlFreteValor,
    VlOutros,
    DsComentario,
    NrSerie,
    FgICMSIncluso,
    InTipoFreteRedespacho,
    CdEmpresaCotacao,
    CdCotacao,
    DsUsuario,
    DtDigitacao,
    DsLocalColeta,
    QtEntregas,
    InOrigemConhec,
    InICMSDestacado,
    VlAliqISS,
    VlISS,
    CdEmpresaTabela,
    InTipoEmissao,
    InImpressao,
    CdEmpresaDestino,
    InControle,
    VlBaseCalcComissao,
    InArmazenagem,
    InCalcMultiplasNat,
    CdSituacaoCarga,
    DsLocalEntrega,
    NrDoctoFiscal,
    CdTpDoctoFiscal,
    InFOBDirigido,
    FgConversao,
    InTpCTE,
    DtCancelamento,
    CdMotivoCancelamento,
    CdFilialCancelamento,
    CdConsignatario,
    CdRedespacho,
    InExportadoEDI,
    DtEntrada,
    DtPagamentoVista,
    NrNotaFiscal,
    DsMarca,
    NrMarca,
    VlFreteIdeal,
    InManifesto,
    InEntregaManifesto,
    CdFranquia,
    InCargaEncomenda,
    CdCCustoContabil,
    NrPlacaReboque1,
    NrPlacaReboque2,
    NrPlacaReboque3,
    CdMotorista2,
    NrHodVeiculo,
    VlCoeficientePeso,
    DsLocaEntrega,
    CdTipoVeiculo,
    NrRepom,
    QtMetrosCubicos,
    CdEmpresaCartaFrete,
    NrDoctoCartaFrete,
    VlPercComissao,
    CdEmpresaRef,
    NrSeqControleRef,
    NrCepColetaANT,
    NrCepEntregaANT,
    CdPercurso,
    CdPercursoComercial,
    NrConhecRedespacho,
    NrNotaFiscalCobol,
    CdEmpresaFVColeta,
    NrFichaViagemColeta,
    DtImpressao,
    InSeguroFrete,
    NrPreCHC,
    InPercComissao,
    InISS,
    CdEmpresaColeta,
    CdEmpresaLigada,
    CdPercursoAnt,
    NrColeta,
    VlAliqIRRF,
    VlIRRF,
    CdEmpresaSubst,
    NrSeqControleSubst,
    CdGerenciadorRisco,
    VlDescCalc,
    VlCAPCalc,
    VlCADCalc,
    QtMinuto,
    CdComposicaoVeic,
    NrInscEstadualRem,
    CodBeneficio,
    NrInscEstadualDest,
    NrInscEstadualConsig,
    InExigibilidadeISS
)
VALUES (
    @CdEmpresa,
    @NrSeqControle,
    @CdInscricao,
    @Data,
    @DataEntrega,
    0,
    @DiasEntrega,
    1,
    @CdRemetente,
    @CdDestinatario,
    @NrCepColeta,
    @NrCepEntrega,
    @NrCepEntrega,
    18,
    2,
    52,
    1,
    @QtPeso,
    @QtPesoCubado,
    @QtVolume,
    @VlMercadoria,
    @VlNFCobrada,
    2,
    535003,
    0,
    @VlTotalPrestacao,
    @VlBaseCalculo,
    @VlLiquido,
    @InConhecimento,
    0,
    '',
    '',
    @VlFretePeso,
    @VlFreteValor,
    @VlOutros,
    '',
    @NrSerie,
    0,
    1,
    0,
    0,
    'INTEGRACAO',
    @Data,
    '',
    1,
    0,
    1,
    @VlAliqISS,
    @VlISS,
    @CdEmpresaTabela,
    0,
    1,
    @CdEmpresaDestino,
    @CdEmpresaDestino,
    @VlLiquido,
    0,
    0,
    2,
    '',
    @NrDoctoFiscal,
    @CdTpDoctoFiscal,
    0,
    0,
    0,
    @DtCancelamento,
    @CdMotivoCancelamento,
    @CdFilialCancelamento,
    @CdConsignatario,
    @CdRedespacho,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
);

UPDATE [${destDb}].dbo.GTCConhe
SET 
  CdEmpresaColeta = @CdEmpresaDestino,
  CdEmpresaLigada = @CdEmpresaDestino,
  DtCancelamento = @DtCancelamento,
  InConhecimento = @InConhecimento
WHERE CdEmpresa = @CdEmpresa
  AND NrSeqControle = @NrSeqControle;

INSERT INTO [${destDb}].dbo.GTCCONIA (
    CdEmpresa,
    NrSeqControle,
    CdConceitoEmbarque,
    NrNSU,
    InMercEntDeposito,
    DtEmissaoConheOrigem,
    QtPesoCubadoReal,
    InCargaTranspDestExp,
    InMercRetDeposito,
    QtDiasPermDeposito,
    CdEmpContrContainer,
    CdPedidoContrContainer,
    InVeiculo,
    CdEmpCTeOri,
    NrSeqControleCTeOri,
    CdEmpNFeAnu,
    NrDoctoNFeAnu,
    CdInscrNFEAnu,
    CdTpDoctoNFeAnu,
    NrSerieNFeAnu,
    INDESTCARGAEXPORT,
    InTpServ,
    InGlobalizado,
    CdCFPS,
    CdOperacaoDIFAL,
    CdSeqVincOpDIFAL,
    InModalTransp,
    CdEmpresaEntrega,
    CdEmpAnu,
    NrSeqControleAnu,
    CdExpedidorCarga,
    QtPesoCubadoOper,
    InTpDocAnterior,
    InDeclaracaoTranspPF,
    CdClasseEmissao,
    VlBaseCalcPis,
    VlAliqPis,
    VlPis,
    VlAliqCofins,
    VlCofins,
    DtEmissaoDocAnterior,
    NrSubSerie,
    NrCNPJCPFColEnt,
    DsConhecOrigem,
    DsObsVincOperacao,
    CdRota,
    CdInscrCliColeta,
    NrAnoGeracaoSmp,
    NrSmp,
    CdViagem,
    CdVinculacaoISS,
    NrChaveAcessoCTeOrigem,
    NrGNRE,
    DtPrevEntr,
    NrDiasEntrPrev,
    DsChaveImpressao,
    CdEmissorDocAnterior,
    CdDocAnterior,
    NrSerieDocAnterior,
    CdATividadeRMS,
    imLogEDI,
    NrChaveAcessoCTeRef,
    DsLogCotacao,
    DsInfoManuseioCarga,
    CdSitTributariaCBS,
    VlBaseCalcCBS,
    VlAliqCBS,
    VlRedAliqCBS,
    VlCBS,
    CdSitTributariaIBSEst,
    VlBaseCalcIBSEst,
    VlAliqIBSEst,
    VlRedAliqIBSEst,
    VlIBSEst,
    CdSitTributariaIBSMun,
    VlBaseCalcIBSMun,
    VlAliqIBSMun,
    VlRedAliqIBSMun,
    VlIBSMun,
    VlPercCredPresumidoCBS,
    VlCredPresumidoCBS,
    VlPercCredPresumidoIBSEst,
    VlCredPresumidoIBSEst,
    VlTotDocFiscal,
    VlPercDeferidoCBS,
    VlAliqDeferidoCBS,
    VlPercTribRegularCBS,
    VlTribRegularCBS,
    VlPercDeferidoIBSMun,
    VlAliqDeferidoIBSMun,
    VlPercTribRegularIBSMun,
    VlTribRegularIBSMun,
    VlPercDeferidoIBSEst,
    VlAliqDeferidoIBSEst,
    VlPercTribRegularIBSEst,
    VlTribRegularIBSEst
)
VALUES (
    @CdEmpresa,
    @NrSeqControle,
    0,
    0,
    0,
    @Data,
    @QtPesoCubado,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    2447,
    0,
    0,
    0,
    @CdEmpresaEntrega,
    0,
    0,
    @CdExpedidorCarga,
    @QtPesoCubado,
    0,
    0,
    1,
    @VlBaseCalcPis,
    @VlAliqPis,
    @VlPis,
    @VlAliqCofins,
    @VlCofins,
    NULL, -- DtEmissaoDocAnterior
    NULL, -- NrSubSerie
    NULL, -- NrCNPJCPFColEnt
    NULL, -- DsConhecOrigem
    NULL, -- DsObsVincOperacao
    NULL, -- CdRota
    NULL, -- CdInscrCliColeta
    NULL, -- NrAnoGeracaoSmp
    NULL, -- NrSmp
    NULL, -- CdViagem
    @CdVinculacaoISS, -- CdVinculacaoISS
    NULL, -- NrChaveAcessoCTeOrigem
    NULL, -- NrGNRE
    NULL, -- DtPrevEntr
    NULL, -- NrDiasEntrPrev
    NULL, -- DsChaveImpressao
    NULL, -- CdEmissorDocAnterior
    NULL, -- CdDocAnterior
    NULL, -- NrSerieDocAnterior
    NULL, -- CdATividadeRMS
    NULL, -- imLogEDI
    NULL, -- NrChaveAcessoCTeRef
    NULL, -- DsLogCotacao
    NULL, -- DsInfoManuseioCarga
    NULL, -- CdSitTributariaCBS
    NULL, -- VlBaseCalcCBS
    NULL, -- VlAliqCBS
    NULL, -- VlRedAliqCBS
    NULL, -- VlCBS
    NULL, -- CdSitTributariaIBSEst
    NULL, -- VlBaseCalcIBSEst
    NULL, -- VlAliqIBSEst
    NULL, -- VlRedAliqIBSEst
    NULL, -- VlIBSEst
    NULL, -- CdSitTributariaIBSMun
    NULL, -- VlBaseCalcIBSMun
    NULL, -- VlAliqIBSMun
    NULL, -- VlRedAliqIBSMun
    NULL, -- VlIBSMun
    NULL, -- VlPercCredPresumidoCBS
    NULL, -- VlCredPresumidoCBS
    NULL, -- VlPercCredPresumidoIBSEst
    NULL, -- VlCredPresumidoIBSEst
    NULL, -- VlTotDocFiscal
    NULL, -- VlPercDeferidoCBS
    NULL, -- VlAliqDeferidoCBS
    NULL, -- VlPercTribRegularCBS
    NULL, -- VlTribRegularCBS
    NULL, -- VlPercDeferidoIBSMun
    NULL, -- VlAliqDeferidoIBSMun
    NULL, -- VlPercTribRegularIBSMun
    NULL, -- VlTribRegularIBSMun
    NULL, -- VlPercDeferidoIBSEst
    NULL, -- VlAliqDeferidoIBSEst
    NULL, -- VlPercTribRegularIBSEst
    NULL -- VlTribRegularIBSEst
);

-- Verificação de segurança: verificar se o CdSequencia já existe (proteção contra inserções simultâneas)
-- Se existir conflito, incrementar até encontrar um disponível
IF EXISTS (SELECT 1 FROM [${destDb}].dbo.sisnfsne WITH (UPDLOCK, HOLDLOCK) WHERE CdSequencia = @CdSequencia)
BEGIN
  -- Conflito detectado - incrementar CdSequencia até encontrar disponível
  SET @Tentativas = 0;
  SET @CdSequencia = @CdSequencia + 1;
  WHILE EXISTS (SELECT 1 FROM [${destDb}].dbo.sisnfsne WITH (UPDLOCK, HOLDLOCK) WHERE CdSequencia = @CdSequencia) AND @Tentativas < 1000
  BEGIN
    SET @CdSequencia = @CdSequencia + 1;
    SET @Tentativas = @Tentativas + 1;
  END
  
  IF @Tentativas >= 1000
  BEGIN
    THROW 50012, 'Não foi possível encontrar um CdSequencia disponível após conflito', 1;
  END
END

-- Inserir em sisnfsne com o novo NrSeqControle gerado
INSERT INTO [${destDb}].dbo.sisnfsne (
      CdSequencia,
      CdEmpresa,
      NrSeqControle,
      NrDoctoFiscal,
      CdEmitente,
      NrNotaFiscal,
      NrSerie,
      DsMensagem,
      InCancelada,
      DsUsuInclusao,
      DtHrInclusao,
      InFornecDoctoEletron,
      DsUsuVinculacao,
      DtHrVinculacao,
      InTpArquivo,
      InRemRetorno,
      InSitAverbAuto,
      NrNFSe,
      CdVerifNFSe,
      DtHrRetNFSe
  )
  VALUES (
      @CdSequencia,
      @CdEmpresa,
      @NrSeqControle,
      @NrDoctoFiscal,
      0,
      0,
      1,
      '1 - Mensagem Código 1',
      0,
      'INTEGRACAO',
      @Data,
      4,
      'INTEGRACAO',
      @Data,
      1,
      2,
      1,
      @NrNFSe,
      @CdVerifNFSe,
      @Data
  );

UPDATE ${NFSE_TABLE}
SET 
  processed = 1,
  status = 'processed',
  updated_at = GETDATE(),
  error_message = NULL
WHERE id = @NfseId;

SELECT @NrSeqControle AS NrSeqControle, @CdSequencia AS CdSequencia;
`;
};

async function insertIntoDestination(tx: TransactionClient, nfseId: number) {
  // Calcular nrSeqControle antes de construir o script SQL
  // Busca MAX entre GTCCONCE (CT-e) e GTCConhe (NFSe) para garantir sequência única
  const nrSeqControle = await obterProximoNrSeqControle(tx, NFSE_DESTINATION_DATABASE);

  // Buscar IssRetido para log de debug da regra cdvinculacaoISS
  let issRetidoValue: number | null = null;
  try {
    const issRetidoData = await tx.$queryRawUnsafe<Array<{ IssRetido: number | null }>>(
      `SELECT IssRetido FROM ${NFSE_TABLE} WHERE id = ${nfseId}`,
    );
    if (issRetidoData && issRetidoData.length > 0 && issRetidoData[0]) {
      issRetidoValue = issRetidoData[0].IssRetido;
    }
  } catch (error) {
    // Ignorar erro na busca do IssRetido (não crítico)
  }

  logger.info(
    {
      nfseId,
      nrSeqControle,
      destDb: NFSE_DESTINATION_DATABASE,
      issRetido: issRetidoValue,
      cdVinculacaoISS: issRetidoValue === 1 ? 138 : null,
      regra: 'Se issRetido = 1, então cdvinculacaoISS = 138 na tabela GTCCONIA',
    },
    'NrSeqControle calculado, construindo script SQL',
  );

  const sqlScript = buildInsertScript(nfseId, nrSeqControle);

  // Log para debug - verificar se o SQL está correto
  const fromGTCConheMatch = sqlScript.match(/FROM\s+([^\s]+)\s+GTCConhe/i);
  const insertGTCConheMatch = sqlScript.match(/INSERT\s+INTO\s+([^\s]+)\s+GTCConhe/i);
  logger.debug(
    {
      nfseId,
      destDb: NFSE_DESTINATION_DATABASE,
      fromMatch: fromGTCConheMatch?.[1],
      insertMatch: insertGTCConheMatch?.[1],
      scriptExcerpt: sqlScript.substring(140, 200), // Mostrar a parte do FROM
    },
    'Executando script SQL - verificando interpolação',
  );

  const result =
    await tx.$queryRawUnsafe<{ NrSeqControle: number; CdSequencia: number }[]>(sqlScript);
  return result?.[0];
}

async function processSingleNfse(prisma: PrismaClient, nfseId: number) {
  return prisma.$transaction(async (tx) => insertIntoDestination(tx, nfseId));
}

async function processPendingNfsePostgres(prisma: PrismaClient) {
  const pendingEvents = await prisma.webhookEvent.findMany({
    where: {
      status: { in: ['pending', 'processing'] },
      OR: [
        { source: '/nfse/autorizado' },
        { source: '/api/NFSe/InserirNFSe' },
        { source: { contains: '/nfse/autorizado' } },
      ],
    },
    orderBy: { receivedAt: 'asc' },
    take: NFSE_BATCH_SIZE,
  });

  if (pendingEvents.length === 0) {
    logger.debug('Nenhum evento NFSe pendente para processamento local');
    return;
  }

  logger.info(
    { count: pendingEvents.length },
    'Processando NFSe em modo PostgreSQL local (sem SQL Server/Senior)',
  );

  for (const event of pendingEvents) {
    const start = Date.now();

    try {
      const metadataBase =
        typeof event.metadata === 'string' && event.metadata.trim().length > 0
          ? (() => {
              try {
                return JSON.parse(event.metadata);
              } catch {
                return { rawMetadata: event.metadata };
              }
            })()
          : {};

      await prisma.webhookEvent.update({
        where: { id: event.id },
        data: { status: 'processing' },
      });

      await prisma.webhookEvent.update({
        where: { id: event.id },
        data: {
          status: 'processed',
          processedAt: new Date(),
          integrationStatus: 'integrated',
          processingTimeMs: Date.now() - start,
          retryCount: 0,
          errorMessage: null,
          metadata: JSON.stringify({
            ...metadataBase,
            workerMode: 'postgres_local_sem_legacy',
            workerService: 'nfseSync',
            etapa: 'worker_local_processed',
            observacao:
              'Integracao externa desativada; evento NFSe mantido em modo local no PostgreSQL.',
          }).substring(0, 2000),
        },
      });
    } catch (eventError: any) {
      await prisma.webhookEvent
        .update({
          where: { id: event.id },
          data: {
            status: 'failed',
            processedAt: new Date(),
            integrationStatus: 'failed',
            retryCount: (event.retryCount ?? 0) + 1,
            errorMessage: (eventError?.message || 'Falha no processamento local NFSe').substring(
              0,
              1000,
            ),
            processingTimeMs: Date.now() - start,
          },
        })
        .catch(() => undefined);

      logger.error(
        { eventId: event.id, source: event.source, error: eventError?.message },
        'Falha no processamento local de evento NFSe',
      );
    }
  }
}

export async function processPendingNfse(prisma: PrismaClient) {
  if (IS_POSTGRES && !ENABLE_SQLSERVER_LEGACY) {
    await processPendingNfsePostgres(prisma);
    return;
  }

  try {
    // Buscar notas pendentes (processed = 0), incluindo notas canceladas
    // Notas canceladas também precisam ser processadas para atualizar o status de cancelamento
    const pending = await prisma.$queryRawUnsafe<{ id: number }[]>(`
      SELECT TOP (${NFSE_BATCH_SIZE}) id
      FROM ${NFSE_TABLE} WITH (UPDLOCK, READPAST)
      WHERE processed = 0
      ORDER BY id ASC
    `);

    if (pending.length === 0) {
      logger.debug('Nenhuma NFSe pendente para processamento');
      return;
    }

    logger.info(
      { count: pending.length, ids: pending.map((n) => n.id) },
      'Processando NFSe pendentes',
    );

    for (const nfse of pending) {
      logger.info({ nfseId: nfse.id }, 'Iniciando processamento da NFSe');
      // REGRA: Processar sempre que processed = 0, independentemente de eventos na tabela WebhookEvent
      // Isso garante que notas marcadas como não processadas sejam sempre processadas

      const eventId = `nfse-${nfse.id}-${Date.now()}`;
      let webhookEvent = null;
      let numeroDocumento: number | undefined = undefined;

      try {
        // Buscar número do documento, série RPS e CNPJ antes de processar
        let nfseInfo: {
          NumeroNfse: number;
          SerieIdentificacaoRps: number | null;
          CnpjIdentPrestador: string;
        } | null = null;
        try {
          logger.debug({ nfseId: nfse.id, nfseTable: NFSE_TABLE }, 'Buscando dados da NFSe');
          const nfseData = await prisma.$queryRawUnsafe<
            Array<{
              NumeroNfse: number;
              SerieIdentificacaoRps: number | null;
              CnpjIdentPrestador: string;
              NumeroIdentificacaoRps: number | null;
            }>
          >(`
            SELECT NumeroNfse, SerieIdentificacaoRps, CnpjIdentPrestador, NumeroIdentificacaoRps
            FROM ${NFSE_TABLE}
            WHERE id = ${nfse.id}
          `);
          if (nfseData && nfseData.length > 0 && nfseData[0]) {
            nfseInfo = {
              NumeroNfse: nfseData[0].NumeroNfse,
              SerieIdentificacaoRps: nfseData[0].SerieIdentificacaoRps,
              CnpjIdentPrestador: nfseData[0].CnpjIdentPrestador,
            };
            numeroDocumento = nfseInfo.NumeroNfse;
            logger.debug(
              {
                nfseId: nfse.id,
                numeroNfse: nfseInfo.NumeroNfse,
                serieRps: nfseInfo.SerieIdentificacaoRps,
                numeroRps: nfseData[0].NumeroIdentificacaoRps,
                cnpjPrestador: nfseInfo.CnpjIdentPrestador,
              },
              'Dados da NFSe obtidos com sucesso',
            );
          } else {
            logger.warn({ nfseId: nfse.id }, 'NFSe não encontrada na tabela (retornou vazio)');
          }
        } catch (error: any) {
          logger.error(
            { nfseId: nfse.id, error: error?.message, stack: error?.stack },
            'Erro ao buscar dados da NFSe',
          );
        }

        // Verificar se o registro já existe nas tabelas de destino antes de processar
        if (nfseInfo) {
          const cdEmpresa = calcularCdEmpresa(nfseInfo.CnpjIdentPrestador);

          logger.debug(
            {
              nfseId: nfse.id,
              numeroNfse: nfseInfo.NumeroNfse,
              cnpjPrestador: nfseInfo.CnpjIdentPrestador,
              cdEmpresa,
              destDb: NFSE_DESTINATION_DATABASE,
            },
            'Verificando se NFSe já existe nas tabelas de destino',
          );

          try {
            // Verificar se existe em GTCConhe
            // IMPORTANTE: A combinação única é: NumeroNfse + CdEmpresa + CdRemetente + NrSerie + CdTpDoctoFiscal
            // SEMPRE verificar com todos esses 5 campos para evitar falsos positivos (evita conflito com CT-e)
            // Usar apenas 2 níveis de verificação:
            //   Nível 1: Combinação exata (todos os 5 campos)
            //   Nível 2: Alternativa com CdTpDoctoFiscal = 0 (para registros inseridos manualmente)
            const serieRps = 25; // Série fixa para inserção
            const cnpjPrestador = nfseInfo.CnpjIdentPrestador.replace(/\D/g, ''); // Remover formatação do CNPJ
            const cdTpDoctoFiscal =
              nfseInfo.CnpjIdentPrestador === '59530832000162'
                ? 20
                : nfseInfo.CnpjIdentPrestador === '16836335000184'
                  ? 30
                  : 30;

            // Verificação PRINCIPAL: NumeroNfse + CdEmpresa + CdRemetente + NrSerie + CdTpDoctoFiscal
            // Primeiro tentar com a combinação exata
            let existingGTCConhe = await prisma.$queryRawUnsafe<
              Array<{
                NrSeqControle: number;
                CdEmpresa: number;
                NrSerie: number;
                CdRemetente: string;
                CdTpDoctoFiscal: number;
              }>
            >(`
              SELECT TOP 1 NrSeqControle, CdEmpresa, NrSerie, CdRemetente, CdTpDoctoFiscal
              FROM [${NFSE_DESTINATION_DATABASE}].dbo.GTCConhe
              WHERE NrDoctoFiscal = ${ensureInt(nfseInfo.NumeroNfse)}
                AND CdEmpresa = ${ensureInt(cdEmpresa)}
                AND CdRemetente = '${cnpjPrestador}'
                AND NrSerie = ${ensureInt(serieRps)}
                AND CdTpDoctoFiscal = ${ensureInt(cdTpDoctoFiscal)}
              ORDER BY NrSeqControle DESC
            `);

            // Se não encontrou, tentar com CdTpDoctoFiscal = 0
            if (!existingGTCConhe || existingGTCConhe.length === 0) {
              existingGTCConhe = await prisma.$queryRawUnsafe<
                Array<{
                  NrSeqControle: number;
                  CdEmpresa: number;
                  NrSerie: number;
                  CdRemetente: string;
                  CdTpDoctoFiscal: number;
                }>
              >(`
                SELECT TOP 1 NrSeqControle, CdEmpresa, NrSerie, CdRemetente, CdTpDoctoFiscal
                FROM [${NFSE_DESTINATION_DATABASE}].dbo.GTCConhe
                WHERE NrDoctoFiscal = ${ensureInt(nfseInfo.NumeroNfse)}
                  AND CdEmpresa = ${ensureInt(cdEmpresa)}
                  AND CdRemetente = '${cnpjPrestador}'
                  AND CdTpDoctoFiscal = ${ensureInt(0)}
                ORDER BY NrSeqControle DESC
              `);
            }

            // Verificar se existe em sisnfsne
            // Primeiro tentar com a combinação exata
            let existingSisnfsne = await prisma.$queryRawUnsafe<
              Array<{
                CdSequencia: number;
                CdEmpresa: number;
                NrSerie: number;
                NrSeqControle: number;
              }>
            >(`
              SELECT TOP 1 s.CdSequencia, s.CdEmpresa, s.NrSerie, s.NrSeqControle
              FROM [${NFSE_DESTINATION_DATABASE}].dbo.sisnfsne s
              INNER JOIN [${NFSE_DESTINATION_DATABASE}].dbo.GTCConhe g 
                ON s.NrSeqControle = g.NrSeqControle 
                AND s.CdEmpresa = g.CdEmpresa
              WHERE s.NrDoctoFiscal = ${ensureInt(nfseInfo.NumeroNfse)}
                AND s.CdEmpresa = ${ensureInt(cdEmpresa)}
                AND s.NrSerie = ${ensureInt(serieRps)}
                AND g.CdRemetente = '${cnpjPrestador}'
                AND g.CdTpDoctoFiscal = ${ensureInt(cdTpDoctoFiscal)}
              ORDER BY s.CdSequencia DESC
            `);

            // Se não encontrou, tentar com CdTpDoctoFiscal = 0 mas mantendo NrSerie
            // IMPORTANTE: Ainda considerar NrSerie para evitar falsos positivos
            if (!existingSisnfsne || existingSisnfsne.length === 0) {
              existingSisnfsne = await prisma.$queryRawUnsafe<
                Array<{
                  CdSequencia: number;
                  CdEmpresa: number;
                  NrSerie: number;
                  NrSeqControle: number;
                }>
              >(`
                SELECT TOP 1 s.CdSequencia, s.CdEmpresa, s.NrSerie, s.NrSeqControle
                FROM [${NFSE_DESTINATION_DATABASE}].dbo.sisnfsne s
                INNER JOIN [${NFSE_DESTINATION_DATABASE}].dbo.GTCConhe g 
                  ON s.NrSeqControle = g.NrSeqControle 
                  AND s.CdEmpresa = g.CdEmpresa
                WHERE s.NrDoctoFiscal = ${ensureInt(nfseInfo.NumeroNfse)}
                  AND s.CdEmpresa = ${ensureInt(cdEmpresa)}
                  AND s.NrSerie = ${ensureInt(serieRps)}
                  AND g.CdRemetente = '${cnpjPrestador}'
                  AND g.CdTpDoctoFiscal = ${ensureInt(0)}
                ORDER BY s.CdSequencia DESC
              `);
            }

            logger.debug(
              {
                nfseId: nfse.id,
                numeroNfse: nfseInfo.NumeroNfse,
                serieRpsOriginal: nfseInfo.SerieIdentificacaoRps,
                serieUsada: 25, // Série fixa para inserção
                existingGTCConheCount: existingGTCConhe?.length || 0,
                existingSisnfsneCount: existingSisnfsne?.length || 0,
                existsInGTCConhe: existingGTCConhe && existingGTCConhe.length > 0,
                existsInSisnfsne: existingSisnfsne && existingSisnfsne.length > 0,
                gtcEmpresa:
                  existingGTCConhe && existingGTCConhe.length > 0 && existingGTCConhe[0]
                    ? existingGTCConhe[0].CdEmpresa
                    : null,
                gtcSerie:
                  existingGTCConhe && existingGTCConhe.length > 0 && existingGTCConhe[0]
                    ? existingGTCConhe[0].NrSerie
                    : null,
                sisEmpresa:
                  existingSisnfsne && existingSisnfsne.length > 0 && existingSisnfsne[0]
                    ? existingSisnfsne[0].CdEmpresa
                    : null,
                sisSerie:
                  existingSisnfsne && existingSisnfsne.length > 0 && existingSisnfsne[0]
                    ? existingSisnfsne[0].NrSerie
                    : null,
              },
              'Resultado da verificação de registros existentes',
            );

            // IMPORTANTE: Com a nova regra de sempre gerar novo NrSeqControle,
            // só marcamos como processado se existir COMPLETO em todas as tabelas (GTCConhe E sisnfsne)
            // Se existir incompleto, deixamos o script SQL processar (vai gerar novo NrSeqControle)
            const existsComplete =
              existingGTCConhe &&
              existingGTCConhe.length > 0 &&
              existingSisnfsne &&
              existingSisnfsne.length > 0;

            // Se existe completo, verificar se precisa atualizar cancelamento
            if (existsComplete) {
              const gtcRecord =
                existingGTCConhe && existingGTCConhe.length > 0 && existingGTCConhe[0]
                  ? existingGTCConhe[0]
                  : null;
              const sisRecord =
                existingSisnfsne && existingSisnfsne.length > 0 && existingSisnfsne[0]
                  ? existingSisnfsne[0]
                  : null;

              // Verificar se CodigoVerificacao = 'canceled' e atualizar GTCCONHE se necessário
              const nfseCancelamento = await prisma.$queryRawUnsafe<
                Array<{
                  CodigoVerificacao: string | null;
                  DataCancelamento: Date | null;
                  MotivoCancelamento: string | null;
                  FilialCancelamento: string | null;
                }>
              >(`
                SELECT CodigoVerificacao, DataCancelamento, MotivoCancelamento, FilialCancelamento
                FROM ${NFSE_TABLE}
                WHERE id = ${nfse.id}
              `);

              const codigoVerificacao =
                nfseCancelamento && nfseCancelamento.length > 0 && nfseCancelamento[0]
                  ? (nfseCancelamento[0].CodigoVerificacao || '').trim().toUpperCase()
                  : '';

              // IMPORTANTE: UPDATE só acontece se os dados corresponderem E CodigoVerificacao = 'canceled'
              // Verificar se os dados da nota atual correspondem aos dados da nota já inserida
              if (codigoVerificacao === 'CANCELED' && gtcRecord) {
                // Verificar se os dados correspondem: buscar dados do registro existente usando os 5 campos
                // IMPORTANTE: Usar todos os 5 campos para garantir que está verificando o registro correto
                const dadosExistentes = await prisma.$queryRawUnsafe<
                  Array<{
                    NrDoctoFiscal: number;
                    NrNFSe: number | null;
                    CdVerifNFSe: string | null;
                  }>
                >(`
                  SELECT 
                    g.NrDoctoFiscal,
                    s.NrNFSe,
                    s.CdVerifNFSe
                  FROM [${NFSE_DESTINATION_DATABASE}].dbo.GTCConhe g
                  LEFT JOIN [${NFSE_DESTINATION_DATABASE}].dbo.sisnfsne s 
                    ON s.NrSeqControle = g.NrSeqControle 
                    AND s.CdEmpresa = g.CdEmpresa
                  WHERE g.NrDoctoFiscal = ${ensureInt(nfseInfo.NumeroNfse)}
                    AND g.CdEmpresa = ${ensureInt(gtcRecord.CdEmpresa)}
                    AND g.CdRemetente = '${cnpjPrestador}'
                    AND g.NrSerie = ${ensureInt(serieRps)}
                    AND g.CdTpDoctoFiscal = ${ensureInt(gtcRecord.CdTpDoctoFiscal)}
                `);

                const dadosCorrespondem =
                  dadosExistentes && dadosExistentes.length > 0 && dadosExistentes[0]
                    ? dadosExistentes[0].NrDoctoFiscal === nfseInfo.NumeroNfse &&
                      (dadosExistentes[0].NrNFSe === nfseInfo.NumeroNfse ||
                        (dadosExistentes[0].NrNFSe === null && nfseInfo.NumeroNfse === null)) &&
                      (dadosExistentes[0].CdVerifNFSe === codigoVerificacao.toLowerCase() ||
                        dadosExistentes[0].CdVerifNFSe === null)
                    : false;

                if (dadosCorrespondem) {
                  const dataCancelamento = nfseCancelamento[0]?.DataCancelamento
                    ? new Date(nfseCancelamento[0].DataCancelamento).toISOString().split('T')[0]
                    : new Date().toISOString().split('T')[0];
                  const motivoCancelamento = nfseCancelamento[0]?.MotivoCancelamento || null;
                  const filialCancelamento = nfseCancelamento[0]?.FilialCancelamento || null;

                  // Atualizar GTCCONHE usando os 5 campos no WHERE para garantir que está atualizando o registro correto
                  await prisma.$executeRawUnsafe(`
                    UPDATE [${NFSE_DESTINATION_DATABASE}].dbo.GTCConhe
                    SET 
                      DtCancelamento = '${dataCancelamento}',
                      InConhecimento = ${ensureInt(1)},
                      CdMotivoCancelamento = ${motivoCancelamento ? `'${motivoCancelamento.replace(/'/g, "''")}'` : 'NULL'},
                      CdFilialCancelamento = ${filialCancelamento ? `'${filialCancelamento.replace(/'/g, "''")}'` : 'NULL'}
                    WHERE NrDoctoFiscal = ${ensureInt(nfseInfo.NumeroNfse)}
                      AND CdEmpresa = ${ensureInt(gtcRecord.CdEmpresa)}
                      AND CdRemetente = '${cnpjPrestador}'
                      AND NrSerie = ${ensureInt(serieRps)}
                      AND CdTpDoctoFiscal = ${ensureInt(gtcRecord.CdTpDoctoFiscal)}
                  `);

                  // Atualizar sisnfsne usando JOIN com GTCConhe para garantir que está atualizando o registro correto
                  if (sisRecord) {
                    await prisma.$executeRawUnsafe(`
                      UPDATE s
                      SET 
                        CdVerifNFSe = 'canceled',
                        DtHrRetNFSe = GETDATE()
                      FROM [${NFSE_DESTINATION_DATABASE}].dbo.sisnfsne s
                      INNER JOIN [${NFSE_DESTINATION_DATABASE}].dbo.GTCConhe g
                        ON s.NrSeqControle = g.NrSeqControle
                        AND s.CdEmpresa = g.CdEmpresa
                      WHERE s.NrDoctoFiscal = ${ensureInt(nfseInfo.NumeroNfse)}
                        AND s.CdEmpresa = ${ensureInt(gtcRecord.CdEmpresa)}
                        AND s.NrSerie = ${ensureInt(serieRps)}
                        AND g.CdRemetente = '${cnpjPrestador}'
                        AND g.CdTpDoctoFiscal = ${ensureInt(gtcRecord.CdTpDoctoFiscal)}
                    `);
                  }

                  logger.info(
                    {
                      nfseId: nfse.id,
                      numeroNfse: nfseInfo.NumeroNfse,
                      nrSeqControle: gtcRecord.NrSeqControle,
                      cdEmpresa: gtcRecord.CdEmpresa,
                      cdSequencia: sisRecord?.CdSequencia || null,
                    },
                    'Registro cancelado atualizado na GTCCONHE e sisnfsne (dados correspondem)',
                  );
                } else {
                  logger.warn(
                    {
                      nfseId: nfse.id,
                      numeroNfse: nfseInfo.NumeroNfse,
                      nrSeqControle: gtcRecord.NrSeqControle,
                      cdEmpresa: gtcRecord.CdEmpresa,
                      reason: 'Dados não correspondem - não atualizar mesmo estando cancelado',
                    },
                    'Registro cancelado mas dados não correspondem - não atualizar',
                  );
                }
              }

              logger.info(
                {
                  nfseId: nfse.id,
                  numeroNfse: nfseInfo.NumeroNfse,
                  existsInGTCConhe: gtcRecord !== null,
                  existsInSisnfsne: sisRecord !== null,
                  nrSeqControle: gtcRecord?.NrSeqControle || null,
                  cdSequencia: sisRecord?.CdSequencia || null,
                  gtcEmpresa: gtcRecord?.CdEmpresa || null,
                  sisEmpresa: sisRecord?.CdEmpresa || null,
                  isCanceled: codigoVerificacao === 'CANCELED',
                },
                'Registro completo já existe nas tabelas de destino, marcando como processada',
              );

              await prisma.$executeRawUnsafe(`
                UPDATE ${NFSE_TABLE}
                SET 
                  processed = 1,
                  status = 'processed',
                  updated_at = GETDATE(),
                  error_message = NULL
                WHERE id = ${nfse.id}
              `);
            } else {
              // Registro existe mas está incompleto (só em uma tabela)
              // Deixar o script SQL processar normalmente (vai gerar novo NrSeqControle conforme a nova regra)
              const existsInGTC = existingGTCConhe && existingGTCConhe.length > 0;
              const existsInSis = existingSisnfsne && existingSisnfsne.length > 0;

              logger.info(
                {
                  nfseId: nfse.id,
                  numeroNfse: nfseInfo.NumeroNfse,
                  existsInGTCConhe: existsInGTC,
                  existsInSisnfsne: existsInSis,
                  nrSeqControle: existingGTCConhe?.[0]?.NrSeqControle || null,
                  cdSequencia: existingSisnfsne?.[0]?.CdSequencia || null,
                  note: 'Registro incompleto encontrado - continuando processamento para gerar novo NrSeqControle',
                },
                'Registro incompleto encontrado, continuando processamento normal',
              );

              // Continuar com o processamento normal abaixo (deixar o script SQL inserir com novo NrSeqControle)
            }
          } catch (checkError: any) {
            // Se houver erro na verificação, continuar com o processamento normal
            logger.warn(
              { nfseId: nfse.id, error: checkError?.message },
              'Erro ao verificar registros existentes, continuando com processamento',
            );
          }
        }

        // Criar registro na tabela WebhookEvent para monitoramento
        try {
          webhookEvent = await prisma.webhookEvent.create({
            data: {
              id: eventId,
              source: `worker/nfse/${nfse.id}${numeroDocumento ? ` (NFSe: ${numeroDocumento})` : ''}`,
              receivedAt: new Date(),
              status: 'processing',
              retryCount: 0,
              tipoIntegracao: 'Worker',
            },
          });
        } catch (error: any) {
          // Se a tabela WebhookEvent não existir, apenas logar e continuar
          if (error?.code === 'P2021' || error?.code === 'P2003') {
            logger.debug('Tabela WebhookEvent não disponível, continuando sem registro');
          } else {
            logger.warn({ error: error?.message }, 'Erro ao criar registro WebhookEvent');
          }
        }

        // Processar a NFSe
        logger.info(
          { nfseId: nfse.id, numeroNfse: numeroDocumento },
          'Iniciando processamento da NFSe',
        );
        const result = await processSingleNfse(prisma, nfse.id);

        if (!result) {
          logger.warn(
            { nfseId: nfse.id, numeroNfse: numeroDocumento },
            'Processamento da NFSe retornou resultado vazio',
          );
        } else {
          logger.info(
            {
              nfseId: nfse.id,
              numeroNfse: numeroDocumento,
              nrSeqControle: result?.NrSeqControle,
              cdSequencia: result?.CdSequencia,
            },
            'NFSe processada com sucesso',
          );
        }

        // Atualizar registro WebhookEvent como processado
        if (webhookEvent) {
          try {
            const processingTimeMs = Date.now() - new Date(webhookEvent.receivedAt).getTime();
            const numDoc = numeroDocumento || nfse.id;
            const seniorId = result?.NrSeqControle
              ? `NFSe-${numDoc}-${result.NrSeqControle}`
              : `NFSe-${numDoc}`;

            // NFSe insere em 3 tabelas: GTCCONIA, GTCConhe, sisnfsne
            const tabelasInseridas = ['GTCCONIA', 'GTCConhe', 'sisnfsne'];

            const metadata: any = {
              nfseId: nfse.id,
              numeroNfse: numeroDocumento || null,
              nrSeqControle: result?.NrSeqControle || null,
              etapa: 'concluido',
              tabelasInseridas,
              resumo: {
                totalTabelas: tabelasInseridas.length,
                sucesso: tabelasInseridas.length,
                falhas: 0,
              },
            };

            await prisma.webhookEvent.update({
              where: { id: eventId },
              data: {
                status: 'processed',
                processedAt: new Date(),
                processingTimeMs,
                integrationStatus: 'integrated',
                integrationTimeMs: processingTimeMs, // Para NFSe, o tempo de integração é o mesmo do processamento
                seniorId,
                errorMessage: null,
                metadata: JSON.stringify(metadata).substring(0, 2000),
              },
            });
          } catch (error: any) {
            logger.warn({ error: error?.message }, 'Erro ao atualizar WebhookEvent');
          }
        }
      } catch (error: any) {
        const rawMessage = error?.message?.substring(0, 1000) ?? 'Erro desconhecido';
        const translatedMessage = translateError(rawMessage, numeroDocumento);
        logger.error(
          { nfseId: nfse.id, error: rawMessage, translated: translatedMessage },
          'Erro ao processar NFSe',
        );

        // Verificar se o erro é de chave duplicada (GTCConhe, sisnfsne ou qualquer outra tabela)
        // Se for, verificar se o registro já existe e marcar como processada
        const isDuplicateKeyError =
          rawMessage.includes('GTCConhe0') ||
          rawMessage.includes('SISNFSNE0') ||
          rawMessage.includes('duplicate key') ||
          rawMessage.includes('2627');
        let recordExists = false;

        if (isDuplicateKeyError) {
          try {
            // Buscar dados da NFSe para verificar se o registro já existe
            const nfseData = await prisma.$queryRawUnsafe<
              Array<{
                NumeroNfse: number;
                SerieIdentificacaoRps: number | null;
                CnpjIdentPrestador: string;
              }>
            >(`
              SELECT NumeroNfse, SerieIdentificacaoRps, CnpjIdentPrestador
              FROM ${NFSE_TABLE}
              WHERE id = ${nfse.id}
            `);

            if (nfseData && nfseData.length > 0 && nfseData[0]) {
              const nfseInfo = nfseData[0];
              const serieRps = 25; // Série fixa para inserção
              const cdEmpresa = calcularCdEmpresa(nfseInfo.CnpjIdentPrestador);
              const cnpjPrestador = nfseInfo.CnpjIdentPrestador.replace(/\D/g, ''); // Remover formatação do CNPJ
              const cdTpDoctoFiscal =
                nfseInfo.CnpjIdentPrestador === '59530832000162'
                  ? 20
                  : nfseInfo.CnpjIdentPrestador === '16836335000184'
                    ? 30
                    : 30;

              // Extrair CdSequencia do erro se mencionado (ex: "The duplicate key value is (12349)")
              let cdSequenciaFromError: number | null = null;
              const cdSequenciaMatch = rawMessage.match(/duplicate key value is\s*\((\d+)\)/i);
              if (cdSequenciaMatch && cdSequenciaMatch[1]) {
                cdSequenciaFromError = parseInt(cdSequenciaMatch[1], 10);
                logger.info(
                  {
                    nfseId: nfse.id,
                    cdSequenciaFromError,
                    reason: 'CdSequencia extraído do erro de chave duplicada',
                  },
                  'CdSequencia encontrado no erro',
                );
              }

              // Se o erro menciona um CdSequencia específico, buscar diretamente por ele
              if (cdSequenciaFromError && rawMessage.includes('SISNFSNE0')) {
                const existingSisnfsneByCdSequencia = await prisma.$queryRawUnsafe<
                  Array<{
                    CdSequencia: number;
                    CdEmpresa: number;
                    NrSerie: number;
                    NrSeqControle: number;
                    NrDoctoFiscal: number;
                  }>
                >(`
                  SELECT TOP 1 CdSequencia, CdEmpresa, NrSerie, NrSeqControle, NrDoctoFiscal
                  FROM [${NFSE_DESTINATION_DATABASE}].dbo.sisnfsne
                  WHERE CdSequencia = ${ensureInt(cdSequenciaFromError)}
                    AND CdEmpresa = ${ensureInt(cdEmpresa)}
                `);

                if (existingSisnfsneByCdSequencia && existingSisnfsneByCdSequencia.length > 0) {
                  const sisRecord = existingSisnfsneByCdSequencia[0];
                  if (sisRecord) {
                    // Buscar o GTCConhe correspondente
                    const gtcRecord = await prisma.$queryRawUnsafe<
                      Array<{
                        NrSeqControle: number;
                        CdEmpresa: number;
                        NrSerie: number;
                        CdRemetente: string;
                        CdTpDoctoFiscal: number;
                        NrDoctoFiscal: number;
                      }>
                    >(`
                      SELECT TOP 1 NrSeqControle, CdEmpresa, NrSerie, CdRemetente, CdTpDoctoFiscal, NrDoctoFiscal
                      FROM [${NFSE_DESTINATION_DATABASE}].dbo.GTCConhe
                      WHERE NrSeqControle = ${ensureInt(sisRecord.NrSeqControle)}
                        AND CdEmpresa = ${ensureInt(sisRecord.CdEmpresa)}
                    `);

                    if (
                      gtcRecord &&
                      gtcRecord.length > 0 &&
                      gtcRecord[0] &&
                      gtcRecord[0].NrDoctoFiscal === nfseInfo.NumeroNfse
                    ) {
                      logger.info(
                        {
                          nfseId: nfse.id,
                          numeroNfse: nfseInfo.NumeroNfse,
                          cdSequencia: cdSequenciaFromError,
                          nrSeqControle: sisRecord.NrSeqControle,
                          reason:
                            'Registro encontrado pelo CdSequencia mencionado no erro - corresponde à NFSe atual',
                        },
                        'Registro encontrado na sisnfsne pelo CdSequencia do erro',
                      );
                      recordExists = true;
                      // Continuar com o processamento abaixo para atualizar cancelamento se necessário
                    } else {
                      // O CdSequencia mencionado no erro NÃO corresponde à NFSe atual
                      // Isso significa que houve um conflito de chave primária, mas o registro é de outra NFSe
                      // NÃO marcar como processada - deixar que o sistema tente inserir novamente
                      logger.warn(
                        {
                          nfseId: nfse.id,
                          numeroNfse: nfseInfo.NumeroNfse,
                          cdSequencia: cdSequenciaFromError,
                          nrDoctoFiscalEncontrado:
                            gtcRecord && gtcRecord.length > 0 && gtcRecord[0]
                              ? gtcRecord[0].NrDoctoFiscal
                              : null,
                          reason:
                            'CdSequencia mencionado no erro NÃO corresponde à NFSe atual - não marcar como processada',
                        },
                        'Conflito de CdSequencia mas registro não corresponde à NFSe atual',
                      );
                      // NÃO setar recordExists = true, para que o sistema tente inserir novamente
                    }
                  }
                }
              }

              // Verificar se existe registro em GTCConhe
              // Primeiro tentar com a combinação exata, depois com valores alternativos
              let existingGTCConhe = await prisma.$queryRawUnsafe<
                Array<{
                  NrSeqControle: number;
                  CdEmpresa: number;
                  NrSerie: number;
                  CdRemetente: string;
                  CdTpDoctoFiscal: number;
                }>
              >(`
                SELECT TOP 1 NrSeqControle, CdEmpresa, NrSerie, CdRemetente, CdTpDoctoFiscal
                FROM [${NFSE_DESTINATION_DATABASE}].dbo.GTCConhe
                WHERE NrDoctoFiscal = ${nfseInfo.NumeroNfse}
                  AND CdEmpresa = ${cdEmpresa}
                  AND CdRemetente = '${cnpjPrestador}'
                  AND NrSerie = ${serieRps}
                  AND CdTpDoctoFiscal = ${cdTpDoctoFiscal}
                ORDER BY NrSeqControle DESC
              `);

              // Se não encontrou, tentar com CdTpDoctoFiscal = 0 mas mantendo NrSerie
              // IMPORTANTE: Ainda considerar NrSerie para evitar falsos positivos
              if ((!existingGTCConhe || existingGTCConhe.length === 0) && !recordExists) {
                existingGTCConhe = await prisma.$queryRawUnsafe<
                  Array<{
                    NrSeqControle: number;
                    CdEmpresa: number;
                    NrSerie: number;
                    CdRemetente: string;
                    CdTpDoctoFiscal: number;
                  }>
                >(`
                  SELECT TOP 1 NrSeqControle, CdEmpresa, NrSerie, CdRemetente, CdTpDoctoFiscal
                  FROM [${NFSE_DESTINATION_DATABASE}].dbo.GTCConhe
                  WHERE NrDoctoFiscal = ${ensureInt(nfseInfo.NumeroNfse)}
                    AND CdEmpresa = ${ensureInt(cdEmpresa)}
                    AND CdRemetente = '${cnpjPrestador}'
                    AND NrSerie = ${serieRps}
                    AND CdTpDoctoFiscal = 0
                  ORDER BY NrSeqControle DESC
                `);
              }

              // Verificar se existe registro na sisnfsne
              // Primeiro tentar com a combinação exata, depois com valores alternativos
              let existingSisnfsne = await prisma.$queryRawUnsafe<
                Array<{
                  CdSequencia: number;
                  CdEmpresa: number;
                  NrSerie: number;
                  NrSeqControle: number;
                }>
              >(`
                SELECT TOP 1 s.CdSequencia, s.CdEmpresa, s.NrSerie, s.NrSeqControle
                FROM [${NFSE_DESTINATION_DATABASE}].dbo.sisnfsne s
                INNER JOIN [${NFSE_DESTINATION_DATABASE}].dbo.GTCConhe g 
                  ON s.NrSeqControle = g.NrSeqControle 
                  AND s.CdEmpresa = g.CdEmpresa
                WHERE s.NrDoctoFiscal = ${nfseInfo.NumeroNfse}
                  AND s.CdEmpresa = ${cdEmpresa}
                  AND s.NrSerie = ${serieRps}
                  AND g.CdRemetente = '${cnpjPrestador}'
                  AND g.CdTpDoctoFiscal = ${cdTpDoctoFiscal}
                ORDER BY s.CdSequencia DESC
              `);

              // Se não encontrou, tentar com CdTpDoctoFiscal = 0 mas mantendo NrSerie
              // IMPORTANTE: Ainda considerar NrSerie para evitar falsos positivos
              if ((!existingSisnfsne || existingSisnfsne.length === 0) && !recordExists) {
                existingSisnfsne = await prisma.$queryRawUnsafe<
                  Array<{
                    CdSequencia: number;
                    CdEmpresa: number;
                    NrSerie: number;
                    NrSeqControle: number;
                  }>
                >(`
                  SELECT TOP 1 s.CdSequencia, s.CdEmpresa, s.NrSerie, s.NrSeqControle
                  FROM [${NFSE_DESTINATION_DATABASE}].dbo.sisnfsne s
                  INNER JOIN [${NFSE_DESTINATION_DATABASE}].dbo.GTCConhe g 
                    ON s.NrSeqControle = g.NrSeqControle 
                    AND s.CdEmpresa = g.CdEmpresa
                  WHERE s.NrDoctoFiscal = ${ensureInt(nfseInfo.NumeroNfse)}
                    AND s.CdEmpresa = ${ensureInt(cdEmpresa)}
                    AND s.NrSerie = ${serieRps}
                    AND g.CdRemetente = '${cnpjPrestador}'
                    AND g.CdTpDoctoFiscal = 0
                  ORDER BY s.CdSequencia DESC
                `);
              }

              // Se existe em qualquer uma das tabelas, verificar se precisa atualizar cancelamento
              if (
                (existingGTCConhe && existingGTCConhe.length > 0) ||
                (existingSisnfsne && existingSisnfsne.length > 0)
              ) {
                // Registro já existe, verificar se precisa atualizar cancelamento
                recordExists = true;

                const gtcRecord =
                  existingGTCConhe && existingGTCConhe.length > 0 && existingGTCConhe[0]
                    ? existingGTCConhe[0]
                    : null;

                // Verificar se CodigoVerificacao = 'canceled' e atualizar GTCCONHE se necessário
                const nfseCancelamento = await prisma.$queryRawUnsafe<
                  Array<{
                    CodigoVerificacao: string | null;
                    DataCancelamento: Date | null;
                    MotivoCancelamento: string | null;
                    FilialCancelamento: string | null;
                  }>
                >(`
                  SELECT CodigoVerificacao, DataCancelamento, MotivoCancelamento, FilialCancelamento
                  FROM ${NFSE_TABLE}
                  WHERE id = ${ensureInt(nfse.id)}
                `);

                const codigoVerificacao =
                  nfseCancelamento && nfseCancelamento.length > 0 && nfseCancelamento[0]
                    ? (nfseCancelamento[0].CodigoVerificacao || '').trim().toUpperCase()
                    : '';

                // IMPORTANTE: UPDATE só acontece se os dados corresponderem E CodigoVerificacao = 'canceled'
                // Verificar se os dados da nota atual correspondem aos dados da nota já inserida
                if (codigoVerificacao === 'CANCELED' && gtcRecord) {
                  // Verificar se os dados correspondem: buscar dados do registro existente usando os 5 campos
                  // IMPORTANTE: Usar todos os 5 campos para garantir que está verificando o registro correto
                  const dadosExistentes = await prisma.$queryRawUnsafe<
                    Array<{
                      NrDoctoFiscal: number;
                      NrNFSe: number | null;
                      CdVerifNFSe: string | null;
                    }>
                  >(`
                    SELECT 
                      g.NrDoctoFiscal,
                      s.NrNFSe,
                      s.CdVerifNFSe
                    FROM [${NFSE_DESTINATION_DATABASE}].dbo.GTCConhe g
                    LEFT JOIN [${NFSE_DESTINATION_DATABASE}].dbo.sisnfsne s 
                      ON s.NrSeqControle = g.NrSeqControle 
                      AND s.CdEmpresa = g.CdEmpresa
                    WHERE g.NrDoctoFiscal = ${ensureInt(nfseInfo.NumeroNfse)}
                      AND g.CdEmpresa = ${ensureInt(gtcRecord.CdEmpresa)}
                      AND g.CdRemetente = '${cnpjPrestador}'
                      AND g.NrSerie = ${serieRps}
                      AND g.CdTpDoctoFiscal = ${ensureInt(gtcRecord.CdTpDoctoFiscal)}
                  `);

                  const dadosCorrespondem =
                    dadosExistentes && dadosExistentes.length > 0 && dadosExistentes[0]
                      ? dadosExistentes[0].NrDoctoFiscal === nfseInfo.NumeroNfse &&
                        (dadosExistentes[0].NrNFSe === nfseInfo.NumeroNfse ||
                          (dadosExistentes[0].NrNFSe === null && nfseInfo.NumeroNfse === null)) &&
                        (dadosExistentes[0].CdVerifNFSe === codigoVerificacao.toLowerCase() ||
                          dadosExistentes[0].CdVerifNFSe === null)
                      : false;

                  if (dadosCorrespondem) {
                    const dataCancelamento = nfseCancelamento[0]?.DataCancelamento
                      ? new Date(nfseCancelamento[0].DataCancelamento).toISOString().split('T')[0]
                      : new Date().toISOString().split('T')[0];
                    const motivoCancelamento = nfseCancelamento[0]?.MotivoCancelamento || null;
                    const filialCancelamento = nfseCancelamento[0]?.FilialCancelamento || null;

                    // Atualizar GTCCONHE usando os 5 campos no WHERE para garantir que está atualizando o registro correto
                    await prisma.$executeRawUnsafe(`
                      UPDATE [${NFSE_DESTINATION_DATABASE}].dbo.GTCConhe
                      SET 
                        DtCancelamento = '${dataCancelamento}',
                        InConhecimento = ${ensureInt(1)},
                        CdMotivoCancelamento = ${motivoCancelamento ? `'${motivoCancelamento.replace(/'/g, "''")}'` : 'NULL'},
                        CdFilialCancelamento = ${filialCancelamento ? `'${filialCancelamento.replace(/'/g, "''")}'` : 'NULL'}
                      WHERE NrDoctoFiscal = ${ensureInt(nfseInfo.NumeroNfse)}
                        AND CdEmpresa = ${ensureInt(gtcRecord.CdEmpresa)}
                        AND CdRemetente = '${cnpjPrestador}'
                        AND NrSerie = ${serieRps}
                        AND CdTpDoctoFiscal = ${ensureInt(gtcRecord.CdTpDoctoFiscal)}
                    `);

                    // Atualizar sisnfsne usando JOIN com GTCConhe para garantir que está atualizando o registro correto
                    const sisRecord =
                      existingSisnfsne && existingSisnfsne.length > 0 && existingSisnfsne[0]
                        ? existingSisnfsne[0]
                        : null;
                    if (sisRecord) {
                      await prisma.$executeRawUnsafe(`
                        UPDATE s
                        SET 
                          CdVerifNFSe = 'canceled',
                          DtHrRetNFSe = GETDATE()
                        FROM [${NFSE_DESTINATION_DATABASE}].dbo.sisnfsne s
                        INNER JOIN [${NFSE_DESTINATION_DATABASE}].dbo.GTCConhe g
                          ON s.NrSeqControle = g.NrSeqControle
                          AND s.CdEmpresa = g.CdEmpresa
                        WHERE s.NrDoctoFiscal = ${ensureInt(nfseInfo.NumeroNfse)}
                          AND s.CdEmpresa = ${ensureInt(gtcRecord.CdEmpresa)}
                          AND s.NrSerie = ${serieRps}
                          AND g.CdRemetente = '${cnpjPrestador}'
                          AND g.CdTpDoctoFiscal = ${ensureInt(gtcRecord.CdTpDoctoFiscal)}
                      `);
                    }

                    logger.info(
                      {
                        nfseId: nfse.id,
                        numeroNfse: nfseInfo.NumeroNfse,
                        nrSeqControle: gtcRecord.NrSeqControle,
                        cdEmpresa: gtcRecord.CdEmpresa,
                        cdSequencia: sisRecord?.CdSequencia || null,
                      },
                      'Registro cancelado atualizado na GTCCONHE e sisnfsne (dados correspondem - erro duplicado)',
                    );
                  } else {
                    logger.warn(
                      {
                        nfseId: nfse.id,
                        numeroNfse: nfseInfo.NumeroNfse,
                        nrSeqControle: gtcRecord.NrSeqControle,
                        cdEmpresa: gtcRecord.CdEmpresa,
                        reason:
                          'Dados não correspondem - não atualizar mesmo estando cancelado (erro duplicado)',
                      },
                      'Registro cancelado mas dados não correspondem - não atualizar',
                    );
                  }
                }

                logger.info(
                  {
                    nfseId: nfse.id,
                    numeroNfse: nfseInfo.NumeroNfse,
                    existsInGTCConhe: existingGTCConhe && existingGTCConhe.length > 0,
                    existsInSisnfsne: existingSisnfsne && existingSisnfsne.length > 0,
                    isCanceled: codigoVerificacao === 'CANCELED',
                  },
                  'Registro já existe nas tabelas de destino, marcando como processada',
                );

                await prisma.$executeRawUnsafe(`
                  UPDATE ${NFSE_TABLE}
                  SET 
                    processed = 1,
                    status = 'processed',
                    updated_at = GETDATE(),
                    error_message = NULL
                  WHERE id = ${ensureInt(nfse.id)}
                `);

                // Atualizar WebhookEvent como processado (se existir)
                // Verificar se já existe um evento para esta NFSe antes de criar/atualizar
                try {
                  let eventToUpdate = webhookEvent;
                  if (!eventToUpdate) {
                    // Buscar evento existente
                    eventToUpdate = await prisma.webhookEvent.findFirst({
                      where: {
                        source: {
                          contains: `worker/nfse/${nfse.id}`,
                        },
                      },
                      orderBy: {
                        receivedAt: 'desc',
                      },
                    });
                  }

                  if (eventToUpdate) {
                    const processingTimeMs =
                      Date.now() - new Date(eventToUpdate.receivedAt).getTime();
                    const numDoc = numeroDocumento || nfse.id;
                    const gtcRecord =
                      existingGTCConhe && existingGTCConhe.length > 0 && existingGTCConhe[0]
                        ? existingGTCConhe[0]
                        : null;
                    const sisRecord =
                      existingSisnfsne && existingSisnfsne.length > 0 && existingSisnfsne[0]
                        ? existingSisnfsne[0]
                        : null;

                    const seniorId = gtcRecord
                      ? `NFSe-${numDoc}-GTC-${gtcRecord.NrSeqControle}-E${gtcRecord.CdEmpresa}`
                      : sisRecord
                        ? `NFSe-${numDoc}-SIS-${sisRecord.CdSequencia}-E${sisRecord.CdEmpresa}`
                        : `NFSe-${numDoc}`;

                    await prisma.webhookEvent.update({
                      where: { id: eventToUpdate.id },
                      data: {
                        status: 'processed',
                        processedAt: new Date(),
                        errorMessage: translatedMessage.substring(0, 1000), // Preservar mensagem de erro de duplicidade
                        processingTimeMs,
                        integrationStatus: 'integrated',
                        seniorId,
                        metadata: JSON.stringify({
                          nfseId: nfse.id,
                          numeroNfse: numeroDocumento || null,
                          alreadyExists: true,
                          duplicateKeyError: true,
                        }).substring(0, 2000),
                      },
                    });
                  }
                } catch (updateError: any) {
                  logger.warn({ error: updateError?.message }, 'Erro ao atualizar WebhookEvent');
                }

                continue; // Pular para a próxima NFSe
              }
            }
          } catch (checkError: any) {
            logger.warn(
              { nfseId: nfse.id, error: checkError?.message },
              'Erro ao verificar registro existente',
            );
            // Mesmo se a verificação falhar, se é erro de chave duplicada, marcar como processada
            // porque o erro indica que o registro já existe no banco
            if (isDuplicateKeyError) {
              logger.info(
                {
                  nfseId: nfse.id,
                  numeroNfse: numeroDocumento,
                  reason:
                    'Erro de chave duplicada detectado, marcando como processada mesmo com falha na verificação',
                },
                'Marcando NFSe como processada devido a erro de chave duplicada',
              );

              // Marcar NFSe como processada
              try {
                await prisma.$executeRawUnsafe(`
                  UPDATE ${NFSE_TABLE}
                  SET 
                    processed = 1,
                    status = 'processed',
                    updated_at = GETDATE(),
                    error_message = NULL
                  WHERE id = ${ensureInt(nfse.id)}
                `);

                // Atualizar WebhookEvent como processado
                let eventToUpdate = webhookEvent;
                if (!eventToUpdate) {
                  eventToUpdate = await prisma.webhookEvent.findFirst({
                    where: {
                      source: {
                        contains: `worker/nfse/${nfse.id}`,
                      },
                    },
                    orderBy: {
                      receivedAt: 'desc',
                    },
                  });
                }

                if (eventToUpdate) {
                  const processingTimeMs =
                    Date.now() - new Date(eventToUpdate.receivedAt).getTime();
                  const numDoc = numeroDocumento || nfse.id;

                  // NFSe já existe nas tabelas (verificação falhou, mas registro existe)
                  const tabelasComRegistro = ['GTCCONIA', 'GTCConhe', 'sisnfsne'];

                  const metadata: any = {
                    nfseId: nfse.id,
                    numeroNfse: numeroDocumento || null,
                    etapa: 'verificação_existência',
                    jaExistia: true,
                    tabelasComRegistro,
                    resumo: {
                      totalTabelas: 3,
                      sucesso: 3,
                      falhas: 0,
                    },
                  };

                  await prisma.webhookEvent.update({
                    where: { id: eventToUpdate.id },
                    data: {
                      status: 'processed',
                      processedAt: new Date(),
                      errorMessage: translatedMessage.substring(0, 1000), // Preservar mensagem de erro de duplicidade
                      processingTimeMs,
                      integrationStatus: 'skipped',
                      seniorId: `NFSe-${numDoc}`,
                      metadata: JSON.stringify(metadata).substring(0, 2000),
                    },
                  });
                }
              } catch (markError: any) {
                logger.error(
                  { nfseId: nfse.id, error: markError?.message },
                  'Erro ao marcar NFSe como processada após erro de chave duplicada',
                );
              }

              continue; // Pular para a próxima NFSe
            }
          }
        }

        // Se o registro já existe (detectado no tratamento de erro de chave duplicada),
        // a NFSe já foi marcada como processada acima, então não fazer nada
        if (recordExists) {
          logger.info(
            { nfseId: nfse.id, numeroNfse: numeroDocumento },
            'Registro já existe, NFSe marcada como processada',
          );
          continue; // Pular para a próxima NFSe
        }

        // Se é erro de chave duplicada mas não conseguimos verificar, verificar se é erro de CdSequencia
        // Se for erro de CdSequencia e não corresponde à NFSe atual, NÃO marcar como processada
        // Deixar que o sistema tente inserir novamente na próxima execução
        if (isDuplicateKeyError && !recordExists) {
          // Se o erro é de SISNFSNE0 (CdSequencia), pode ser que o CdSequencia calculado já existe
          // mas não corresponde à NFSe atual. Nesse caso, NÃO marcar como processada
          // O sistema tentará inserir novamente na próxima execução com um novo CdSequencia
          if (rawMessage.includes('SISNFSNE0')) {
            logger.warn(
              {
                nfseId: nfse.id,
                numeroNfse: numeroDocumento,
                reason:
                  'Erro de chave duplicada em sisnfsne (CdSequencia) mas registro não encontrado ou não corresponde - NÃO marcar como processada',
              },
              'Conflito de CdSequencia - tentará inserir novamente na próxima execução',
            );
            // NÃO marcar como processada - deixar que tente novamente
          } else {
            // Para outros tipos de erro de chave duplicada (GTCConhe0), marcar como processada
            logger.info(
              {
                nfseId: nfse.id,
                numeroNfse: numeroDocumento,
                reason:
                  'Erro de chave duplicada mas verificação não encontrou registro, marcando como processada mesmo assim',
              },
              'Marcando NFSe como processada devido a erro de chave duplicada',
            );

            try {
              await prisma.$executeRawUnsafe(`
                UPDATE ${NFSE_TABLE}
                SET 
                  processed = 1,
                  status = 'processed',
                  updated_at = GETDATE(),
                  error_message = NULL
                WHERE id = ${nfse.id}
              `);

              // Atualizar WebhookEvent como processado
              let eventToUpdate = webhookEvent;
              if (!eventToUpdate) {
                eventToUpdate = await prisma.webhookEvent.findFirst({
                  where: {
                    source: {
                      contains: `worker/nfse/${nfse.id}`,
                    },
                  },
                  orderBy: {
                    receivedAt: 'desc',
                  },
                });
              }

              if (eventToUpdate) {
                const processingTimeMs = Date.now() - new Date(eventToUpdate.receivedAt).getTime();
                const numDoc = numeroDocumento || nfse.id;

                // NFSe já existe nas tabelas (verificação falhou, mas registro existe)
                const tabelasComRegistro = ['GTCCONIA', 'GTCConhe', 'sisnfsne'];

                const metadata: any = {
                  nfseId: nfse.id,
                  numeroNfse: numeroDocumento || null,
                  etapa: 'verificação_existência',
                  jaExistia: true,
                  tabelasComRegistro,
                  resumo: {
                    totalTabelas: 3,
                    sucesso: 3,
                    falhas: 0,
                  },
                };

                await prisma.webhookEvent.update({
                  where: { id: eventToUpdate.id },
                  data: {
                    status: 'processed',
                    processedAt: new Date(),
                    errorMessage: translatedMessage.substring(0, 1000), // Preservar mensagem de erro de duplicidade
                    processingTimeMs,
                    integrationStatus: 'skipped',
                    seniorId: `NFSe-${numDoc}`,
                    metadata: JSON.stringify(metadata).substring(0, 2000),
                  },
                });
              }
            } catch (markError: any) {
              logger.error(
                { nfseId: nfse.id, error: markError?.message },
                'Erro ao marcar NFSe como processada após erro de chave duplicada',
              );
            }

            continue; // Pular para a próxima NFSe
          }
        }

        // Atualizar registro WebhookEvent como falhado
        if (webhookEvent) {
          try {
            const processingTimeMs = Date.now() - new Date(webhookEvent.receivedAt).getTime();

            // Identificar qual tabela falhou baseado na mensagem de erro
            const tabelasFalhadas: Array<{ tabela: string; erro: string }> = [];
            if (rawMessage.includes('GTCConhe') || rawMessage.includes('GTCConhe0')) {
              tabelasFalhadas.push({ tabela: 'GTCConhe', erro: translatedMessage });
            }
            if (rawMessage.includes('GTCCONIA') || rawMessage.includes('GTCCONIA0')) {
              tabelasFalhadas.push({ tabela: 'GTCCONIA', erro: translatedMessage });
            }
            if (
              rawMessage.includes('sisnfsne') ||
              rawMessage.includes('SISNFSNE') ||
              rawMessage.includes('SISNFSNE0')
            ) {
              tabelasFalhadas.push({ tabela: 'sisnfsne', erro: translatedMessage });
            }

            // Se não identificou tabela específica, adicionar todas como falhadas
            if (tabelasFalhadas.length === 0) {
              tabelasFalhadas.push({
                tabela: 'GTCCONIA, GTCConhe, sisnfsne',
                erro: translatedMessage,
              });
            }

            const metadata: any = {
              nfseId: nfse.id,
              numeroNfse: numeroDocumento || null,
              erro: translatedMessage,
              etapa: 'falha',
              tabelasFalhadas: tabelasFalhadas,
              resumo: {
                totalTabelas: 3,
                sucesso: 0,
                falhas: tabelasFalhadas.length,
              },
            };

            await prisma.webhookEvent.update({
              where: { id: eventId },
              data: {
                status: 'failed',
                processedAt: new Date(),
                errorMessage: translatedMessage.substring(0, 1000), // Usar mensagem traduzida com número do documento
                retryCount: 1,
                processingTimeMs,
                integrationStatus: 'failed',
                metadata: JSON.stringify(metadata).substring(0, 2000),
              },
            });
          } catch (updateError: any) {
            logger.warn({ error: updateError?.message }, 'Erro ao atualizar WebhookEvent');
          }
        }

        // Atualizar status na tabela nfse apenas se não foi marcada como processada acima
        // Usar mensagem traduzida com número do documento
        await prisma
          .$executeRaw(
            Prisma.sql`UPDATE ${Prisma.raw(NFSE_TABLE)} SET status = 'error', error_message = LEFT(${translatedMessage}, 1000) WHERE id = ${nfse.id}`,
          )
          .catch(() => undefined);
      }
    }
  } catch (error: any) {
    if (error?.code === 'P2021' && error?.meta?.table?.toLowerCase?.() === 'dbo.nfse') {
      logger.debug('Tabela nfse não encontrada, ignorando processamento de NFSe');
      return;
    }
    throw error;
  }
}
