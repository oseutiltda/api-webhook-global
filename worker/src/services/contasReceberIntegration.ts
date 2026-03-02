import type { Prisma, PrismaClient } from '@prisma/client';
import { logger } from '../utils/logger';
import { env } from '../config/env';
import type {
  Fatura,
  Filial,
  Cliente,
  ContaContabil,
  CentroCusto,
  Parcela,
  FaturaItens,
  SISCliFa,
} from '../types/contasReceber';

type PrismaExecutor = PrismaClient | Prisma.TransactionClient;

// Banco de dados Senior configurado via variável de ambiente
const SENIOR_DATABASE = env.SENIOR_DATABASE;

const toSqlValue = (value: any): string => {
  if (value === null || value === undefined) return 'NULL';
  if (typeof value === 'string') {
    const escaped = value.replace(/'/g, "''");
    return `'${escaped}'`;
  }
  if (typeof value === 'number') return String(value);
  if (typeof value === 'boolean') return value ? '1' : '0';
  if (value instanceof Date) {
    return `'${value.toISOString().split('T')[0]}'`;
  }
  return `'${String(value)}'`;
};

const parseDate = (dateStr: Date | string | null | undefined): string => {
  if (!dateStr) return 'NULL';
  try {
    const date = dateStr instanceof Date ? dateStr : new Date(dateStr);
    if (isNaN(date.getTime())) return 'NULL';
    return `'${date.toISOString().split('T')[0]}'`;
  } catch {
    return 'NULL';
  }
};

const parseDateTime = (dateStr: Date | string | null | undefined): string => {
  if (!dateStr) return 'NULL';
  try {
    const date = dateStr instanceof Date ? dateStr : new Date(dateStr);
    if (isNaN(date.getTime())) return 'NULL';
    return `'${date.toISOString().replace('T', ' ').substring(0, 19)}'`;
  } catch {
    return 'NULL';
  }
};

const ensureNumber = (value: number | null | undefined, defaultValue: number = 0): number => {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return defaultValue;
  return Number(value);
};

const ensureDecimal = (value: number | null | undefined): string => {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '0';
  return String(value);
};

/**
 * Retorna código da empresa pelo CNPJ
 */
const retornaCodEmpresa = async (
  prisma: PrismaExecutor,
  cnpj: string | null | undefined,
): Promise<number> => {
  if (!cnpj) {
    logger.warn('CNPJ não informado, usando código padrão 300');
    return 300;
  }

  const cnpjClean = cnpj.replace(/\D/g, '').padStart(14, '0');
  const sql = `EXEC dbo.P_EMPRESA_SENIOR_POR_CNPJ_LISTAR @Cnpj = ${toSqlValue(cnpjClean)}`;

  try {
    const rows = await prisma.$queryRawUnsafe<Array<{ codEmpresa: number }>>(sql);
    if (rows && rows[0] && rows[0].codEmpresa) {
      return Number(rows[0].codEmpresa);
    }
  } catch (error: any) {
    logger.warn(
      { error: error.message, cnpj: cnpjClean },
      'Erro ao buscar código de empresa, usando padrão 300',
    );
  }
  return 300;
};

/**
 * Retorna dados do SISCliFa pelo CNPJ (para cliente)
 */
const retornaDadosSISCliFa = async (
  prisma: PrismaExecutor,
  cdInscricao: string,
): Promise<SISCliFa | null> => {
  try {
    const sql = `EXEC dbo.P_RETORNA_CODIGOS_TAB_SISCLIFA_OBTER @cdInscricao = ${toSqlValue(cdInscricao)}`;
    const rows = await prisma.$queryRawUnsafe<Array<any>>(sql);

    if (rows && rows.length > 0) {
      const row = rows[0];
      return {
        CdRepresentante: row.CdRepresentante || null,
        CdPortador: ensureNumber(row.CdPortador),
        CdCondicaoPagamento: ensureNumber(row.CdCondicaoPagamento),
        cdCentroCusto: ensureNumber(row.CdCentroCusto),
        CdEspeciePagar: ensureNumber(row.CdEspeciePagar),
        CdCarteiraPagar: ensureNumber(row.CdCarteiraPagar),
        CdPlanoContaAPagar: ensureNumber(row.CdPlanoContaAPagar),
      };
    }
  } catch (error: any) {
    logger.warn({ error: error.message, cdInscricao }, 'Erro ao buscar dados SISCliFa');
  }
  return null;
};

/**
 * Verifica se existe XML para a fatura
 * Verifica em GTCCONHE usando o banco configurado via env
 */
const verificaExisteXML = async (prisma: PrismaExecutor, document: string): Promise<boolean> => {
  try {
    // Verificar diretamente na tabela GTCCONHE usando o banco configurado
    const sql = `
      SELECT TOP 1 1 as retorno
      FROM [${SENIOR_DATABASE}]..GTCCONHE WITH (NOLOCK)
      WHERE NrDoctoFiscal = ${toSqlValue(document)}
        OR CAST(NrDoctoFiscal AS VARCHAR) = ${toSqlValue(document)}
    `;
    const rows = await prisma.$queryRawUnsafe<Array<{ retorno: number }>>(sql);
    if (rows && rows.length > 0) {
      return true;
    }
    return false;
  } catch (error: any) {
    logger.error(
      { error: error.message, document, SENIOR_DATABASE },
      'Erro ao verificar existência de XML',
    );
    return false;
  }
};

/**
 * Calcula CdTpDoctoFiscal baseado no CNPJ (mesma regra usada para NFSe)
 */
const calcularCdTpDoctoFiscal = (cnpj: string | null | undefined): number => {
  if (!cnpj) return 30; // Padrão

  const cnpjClean = cnpj.replace(/\D/g, '');

  if (cnpjClean === '59530832000162') {
    return 20;
  }

  if (cnpjClean === '16836335000184') {
    return 30;
  }

  return 30; // Padrão
};

/**
 * Retorna código empresa e sequencial controle pelo cte_key e document
 * Busca diretamente na tabela GTCCONCE usando o banco configurado via env
 * Retorna string no formato "CdEmpresa-NrSeqControle" ou null
 */
const retornaCodEmpresaSeqControle = async (
  prisma: PrismaExecutor,
  cte_key: string | null,
  document: string,
): Promise<{ cdEmpresa: string; nrSeqControle: string } | null> => {
  if (!cte_key) return null;

  try {
    // Buscar diretamente na tabela GTCCONCE usando CdChaveAcesso (que é o cte_key)
    const sql = `
      SELECT TOP 1 
        CdEmpresa,
        NrSeqControle
      FROM [${SENIOR_DATABASE}]..GTCCONCE WITH (NOLOCK)
      WHERE CdChaveAcesso = ${toSqlValue(cte_key)}
    `;
    const rows =
      await prisma.$queryRawUnsafe<Array<{ CdEmpresa: number; NrSeqControle: number }>>(sql);
    if (rows && rows.length > 0 && rows[0]) {
      const cdEmpresa = rows[0].CdEmpresa !== undefined ? String(rows[0].CdEmpresa) : '';
      const nrSeqControle =
        rows[0].NrSeqControle !== undefined ? String(rows[0].NrSeqControle) : '';

      if (cdEmpresa && nrSeqControle) {
        return {
          cdEmpresa,
          nrSeqControle,
        };
      }
    }
  } catch (error: any) {
    logger.error(
      { error: error.message, cte_key, document, SENIOR_DATABASE },
      'Erro ao buscar código empresa e sequencial controle',
    );
  }
  return null;
};

/**
 * Retorna código empresa e sequencial controle para NFSe
 * Busca diretamente na tabela GTCConhe usando os parâmetros:
 * - CdEmpresa (calculado pelo CNPJ)
 * - NrDoctoFiscal (= NumeroNfse)
 * - NrSerie (= 25, fixo)
 * - CdTpDoctoFiscal (20, 30 ou variável de ambiente, baseado no CNPJ)
 */
const retornaCodEmpresaSeqControleNFSe = async (
  prisma: PrismaExecutor,
  nfseNumber: string | number | null | undefined,
  cnpjPrestador: string | null | undefined,
  document: string,
): Promise<{ cdEmpresa: string; nrSeqControle: string } | null> => {
  if (!nfseNumber) return null;

  try {
    // Calcular CdEmpresa baseado no CNPJ (mesma regra usada para NFSe)
    const cnpjClean = cnpjPrestador ? cnpjPrestador.replace(/\D/g, '') : '';
    let cdEmpresa = 300; // Padrão
    if (cnpjClean === '59530832000162') {
      cdEmpresa = 100;
    } else if (cnpjClean === '16836335000184') {
      cdEmpresa = 300;
    }

    // Calcular CdTpDoctoFiscal
    const cdTpDoctoFiscal = calcularCdTpDoctoFiscal(cnpjPrestador);

    // NrSerie fixo = 25
    const nrSerie = 25;

    // Converter nfseNumber para número se necessário
    const numeroNfse = typeof nfseNumber === 'string' ? parseInt(nfseNumber, 10) : nfseNumber;

    if (isNaN(numeroNfse) || numeroNfse <= 0) {
      logger.warn({ nfseNumber, document }, 'NFSe number inválido');
      return null;
    }

    // Buscar na tabela GTCConhe usando os 4 parâmetros principais
    // IMPORTANTE: Verificar também CdTpDoctoFiscal = 0 como alternativa (registros inseridos manualmente)
    let sql = `
      SELECT TOP 1 
        CdEmpresa,
        NrSeqControle
      FROM [${SENIOR_DATABASE}]..GTCConhe WITH (NOLOCK)
      WHERE CdEmpresa = ${cdEmpresa}
        AND NrDoctoFiscal = ${numeroNfse}
        AND NrSerie = ${nrSerie}
        AND CdTpDoctoFiscal = ${cdTpDoctoFiscal}
    `;

    let rows =
      await prisma.$queryRawUnsafe<Array<{ CdEmpresa: number; NrSeqControle: number }>>(sql);

    // Se não encontrou, tentar com CdTpDoctoFiscal = 0 (alternativa para registros inseridos manualmente)
    if (!rows || rows.length === 0) {
      sql = `
        SELECT TOP 1 
          CdEmpresa,
          NrSeqControle
        FROM [${SENIOR_DATABASE}]..GTCConhe WITH (NOLOCK)
        WHERE CdEmpresa = ${cdEmpresa}
          AND NrDoctoFiscal = ${numeroNfse}
          AND NrSerie = ${nrSerie}
          AND CdTpDoctoFiscal = 0
        ORDER BY NrSeqControle DESC
      `;
      rows = await prisma.$queryRawUnsafe<Array<{ CdEmpresa: number; NrSeqControle: number }>>(sql);
    }

    if (rows && rows.length > 0 && rows[0]) {
      const cdEmpresaRetornado = rows[0].CdEmpresa !== undefined ? String(rows[0].CdEmpresa) : '';
      const nrSeqControle =
        rows[0].NrSeqControle !== undefined ? String(rows[0].NrSeqControle) : '';

      if (cdEmpresaRetornado && nrSeqControle) {
        logger.info(
          { nfseNumber, cdEmpresa: cdEmpresaRetornado, nrSeqControle, cdTpDoctoFiscal, document },
          'NFSe encontrada na GTCConhe',
        );
        return {
          cdEmpresa: cdEmpresaRetornado,
          nrSeqControle,
        };
      }
    }

    logger.warn(
      { nfseNumber, cdEmpresa, cdTpDoctoFiscal, nrSerie, document, SENIOR_DATABASE },
      'NFSe não encontrada na GTCConhe',
    );
  } catch (error: any) {
    logger.error(
      { error: error.message, nfseNumber, cnpjPrestador, document, SENIOR_DATABASE },
      'Erro ao buscar NFSe na GTCConhe',
    );
  }
  return null;
};

/**
 * Retorna ICMS por CTE
 */
const retornaICMSPorCte = async (
  prisma: PrismaExecutor,
  cte_key: string | null,
): Promise<number> => {
  if (!cte_key) return 0;

  try {
    const sql = `EXEC dbo.P_RETORNA_ICMS_POR_CTE_OBTER @Cte_Key = ${toSqlValue(cte_key)}`;
    const rows = await prisma.$queryRawUnsafe<Array<{ VlICMS: number }>>(sql);
    if (rows && rows[0] && rows[0].VlICMS !== undefined) {
      return ensureNumber(rows[0].VlICMS);
    }
  } catch (error: any) {
    logger.warn({ error: error.message, cte_key }, 'Erro ao buscar ICMS por CTE');
  }
  return 0;
};

/**
 * Altera InFatura no GTCCONHE após inserir item da fatura
 */
const alterarInFaturaGTCCONHE = async (
  prisma: PrismaExecutor,
  cdEmpresa: string,
  cdNroSeqControle: string,
): Promise<void> => {
  try {
    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_CR_INFATURA_GTCCONHE_ALTERAR
        @CdEmpresa = ${toSqlValue(cdEmpresa)},
        @CdNroSeqControle = ${toSqlValue(cdNroSeqControle)};
    `;
    await prisma.$executeRawUnsafe(sql);
    logger.info({ cdEmpresa, cdNroSeqControle }, 'InFatura alterado no GTCCONHE com sucesso');
  } catch (error: any) {
    logger.error(
      { error: error.message, cdEmpresa, cdNroSeqControle },
      'Erro ao alterar InFatura no GTCCONHE',
    );
    throw error;
  }
};

/**
 * Calcula CdPortador baseado em payment_method e cdEmpresa
 * Regras:
 * - payment_method = "ticket":
 *   - cdEmpresa in (300,302,303 grupo BMX) -> CDPORTADOR = 300
 *   - cdEmpresa in (100,102,103 grupo Brasilmaxi) -> CDPORTADOR = 100
 * - payment_method = "transfer":
 *   - cdEmpresa in (300,302,303 grupo BMX) -> CDPORTADOR = 888
 *   - cdEmpresa in (100,102,103 grupo Brasilmaxi) -> CDPORTADOR = 999
 * - Caso contrário, retorna null (usa valor do SISCliFa)
 */
const calcularCdPortador = (
  paymentMethod: string | null | undefined,
  cdEmpresa: number,
  cdPortadorPadrao: number,
): number => {
  if (!paymentMethod) {
    return cdPortadorPadrao;
  }

  const paymentMethodLower = paymentMethod.toLowerCase().trim();

  // Grupos de empresas
  const grupoBMX = [300, 302, 303];
  const grupoBrasilmaxi = [100, 102, 103];

  const isBMX = grupoBMX.includes(cdEmpresa);
  const isBrasilmaxi = grupoBrasilmaxi.includes(cdEmpresa);

  if (paymentMethodLower === 'ticket') {
    if (isBMX) return 300;
    if (isBrasilmaxi) return 100;
  } else if (paymentMethodLower === 'transfer') {
    if (isBMX) return 888;
    if (isBrasilmaxi) return 999;
  }

  // Se não se encaixar nas regras, retorna o valor padrão do SISCliFa
  return cdPortadorPadrao;
};

/**
 * Verifica se GTCFAT já existe
 */
const verificarGTCFATExistente = async (
  prisma: PrismaExecutor,
  cdEmpresa: number,
  cdFatura: number,
  cdParcela: number,
): Promise<boolean> => {
  try {
    const sql = `
      SELECT TOP 1 1 as existe
      FROM [${SENIOR_DATABASE}]..GTCFAT WITH (NOLOCK)
      WHERE CdEmpresa = ${cdEmpresa}
        AND CdFatura = ${cdFatura}
        AND CdParcela = ${cdParcela}
    `;
    const rows = await prisma.$queryRawUnsafe<Array<{ existe: number }>>(sql);
    return rows && rows.length > 0 && rows[0]?.existe === 1;
  } catch (error: any) {
    logger.warn(
      { error: error.message, cdEmpresa, cdFatura, cdParcela, SENIOR_DATABASE },
      'Erro ao verificar existência de GTCFAT',
    );
    return false;
  }
};

/**
 * Insere GTCFAT (Fatura de Contas a Receber)
 */
const inserirGTCFAT = async (
  prisma: PrismaExecutor,
  fatura: Fatura,
  filial: Filial,
  cliente: Cliente,
  contaContabil: ContaContabil,
  centroCusto: CentroCusto | null,
  vlICMSTotalItens: number = 0,
  paymentMethod?: string | null,
): Promise<string> => {
  const cdEmpresa = await retornaCodEmpresa(prisma, filial.cnpj);
  const ano = new Date().getFullYear().toString().slice(-2);

  // GTCFAT.CdFatura = CdEmpresa + ano + document
  let cdFatura = 0;
  if (fatura.document) {
    cdFatura = parseInt(`${cdEmpresa}${ano}${fatura.document}`);
  }

  const cdParcela = parseInt(ano);

  // Verificar se GTCFAT já existe
  const jaExiste = await verificarGTCFATExistente(prisma, cdEmpresa, cdFatura, cdParcela);
  if (jaExiste) {
    logger.info(
      { cdEmpresa, cdFatura, cdParcela, document: fatura.document },
      'GTCFAT já existe, pulando inserção',
    );
    return cdFatura.toString();
  }

  const cdInscricao = cliente.cnpj || '';

  // Buscar dados do SISCliFa
  const sISCliFa = await retornaDadosSISCliFa(prisma, cdInscricao);
  if (!sISCliFa) {
    throw new Error(`Dados SISCliFa não encontrados para CNPJ: ${cdInscricao}`);
  }

  // Calcular CdPortador baseado em payment_method e cdEmpresa
  const cdPortador = calcularCdPortador(paymentMethod, cdEmpresa, sISCliFa.CdPortador);

  // CdPlanoConta fixo em 31000000 (conforme código C#)
  const cdPlanoConta = 31000000;
  // CdCentroCusto vem do SISCliFa
  const cdCentroCusto = sISCliFa.cdCentroCusto || 0;

  const sql = `
    EXEC dbo.P_INTEGRACAO_SENIOR_CR_GTCFAT_INCLUIR
      @CdEmpresa = ${cdEmpresa},
      @CdFatura = ${cdFatura},
      @CdParcela = ${cdParcela},
      @CdInscricao = ${toSqlValue(cdInscricao)},
      @DtEmissao = ${parseDate(fatura.issue_date)},
      @DtVencimento = ${parseDate(fatura.due_date)},
      @VlDesconto = 0,
      @VlArredondamento = 0,
      @VlAcrescimo = 0,
      @InTipoFaturamento = 0,
      @CdMoeda = 1,
      @CdCondicaoVencto = ${sISCliFa.CdCondicaoPagamento},
      @VlBaseCalculo = ${ensureDecimal(fatura.value)},
      @VlICMS = ${ensureDecimal(vlICMSTotalItens)},
      @VlTotal = ${ensureDecimal(fatura.value)},
      @CdPortador = ${cdPortador},
      @CdCarteira = 1,
      @CdEspecieDocumento = 1,
      @CdInstrucao = 1,
      @CdHistorico = 1,
      @CdPlanoConta = ${cdPlanoConta},
      @DtGeracao = ${parseDate(fatura.issue_date)},
      @InProprioTerceiros = 0,
      @FgRefaturamento = 0,
      @DsUsuarioInc = ${toSqlValue('importacaoAFS')},
      @InOrigemCadastro = 2,
      @DtBase = ${parseDate(fatura.issue_date)},
      @VlEstorno = 0,
      @VlDescontoICMS = 0,
      @VlCSLL = 0,
      @VlCOFINS = 0,
      @VLPIS = 0,
      @CdCentroCusto = ${cdCentroCusto},
      @DtFatIni = ${parseDate(fatura.issue_date)},
      @DtFatFim = ${parseDate(fatura.issue_date)},
      @VlLiquidoOriginal = ${ensureDecimal(fatura.value)};
  `;

  try {
    await prisma.$executeRawUnsafe(sql);
    logger.info({ cdFatura, document: fatura.document }, 'GTCFAT inserido com sucesso');
    return cdFatura.toString();
  } catch (error: any) {
    logger.error(
      { error: error.message, cdFatura, document: fatura.document },
      'Erro ao inserir GTCFAT',
    );
    throw error;
  }
};

/**
 * Gera código do título para GFATITU
 * Para baixa: cdFatura + ano, depois PadLeft(15, '0')
 */
const gerarCdTitulo = (cdFatura: string, ano: string): string => {
  let complemento = `${cdFatura}${ano}`;
  // PadLeft(15, '0') - preencher à esquerda com zeros até 15 caracteres
  return complemento.padStart(15, '0');
};

/**
 * Verifica se GFATITU já existe
 * Chave primária: (InPagarReceber, CdInscricao, CdTitulo)
 */
const verificarGFATITUExistente = async (
  prisma: PrismaExecutor,
  inPagarReceber: number,
  cdInscricao: string,
  cdTitulo: string,
): Promise<boolean> => {
  try {
    const sql = `
      SELECT TOP 1 1 as existe
      FROM [${SENIOR_DATABASE}]..GFATITU WITH (NOLOCK)
      WHERE InPagarReceber = ${inPagarReceber}
        AND CdInscricao = ${toSqlValue(cdInscricao)}
        AND CdTitulo = ${toSqlValue(cdTitulo)}
    `;
    const rows = await prisma.$queryRawUnsafe<Array<{ existe: number }>>(sql);
    return rows && rows.length > 0 && rows[0]?.existe === 1;
  } catch (error: any) {
    logger.warn(
      { error: error.message, inPagarReceber, cdInscricao, cdTitulo, SENIOR_DATABASE },
      'Erro ao verificar existência de GFATITU',
    );
    return false;
  }
};

/**
 * Insere GFATITU (Título de Contas a Receber)
 */
const inserirGFATITU = async (
  prisma: PrismaExecutor,
  fatura: Fatura,
  filial: Filial,
  cliente: Cliente,
  contaContabil: ContaContabil,
  centroCusto: CentroCusto | null,
  cdFatura: string,
  paymentMethod?: string | null,
): Promise<void> => {
  const cdEmpresa = await retornaCodEmpresa(prisma, filial.cnpj);
  const ano = new Date().getFullYear().toString().slice(-2);
  const cdInscricao = cliente.cnpj || '';

  // Buscar dados do SISCliFa
  const sISCliFa = await retornaDadosSISCliFa(prisma, cdInscricao);
  if (!sISCliFa) {
    throw new Error(`Dados SISCliFa não encontrados para CNPJ: ${cdInscricao}`);
  }

  // Calcular CdPortador baseado em payment_method e cdEmpresa
  const cdPortador = calcularCdPortador(paymentMethod, cdEmpresa, sISCliFa.CdPortador);

  // Gerar CdTitulo: cdFatura + ano, depois PadLeft(15, '0')
  const cdTitulo = gerarCdTitulo(cdFatura, ano);
  const dsDigito = '9';
  const cdFilial = await retornaCodEmpresa(prisma, filial.cnpj);

  // Verificar se GFATITU já existe
  const inPagarReceber = 1; // Sempre 1 para contas a receber
  const jaExiste = await verificarGFATITUExistente(prisma, inPagarReceber, cdInscricao, cdTitulo);
  if (jaExiste) {
    logger.info(
      { inPagarReceber, cdInscricao, cdTitulo, cdFatura, document: fatura.document },
      'GFATITU já existe, pulando inserção',
    );
    return;
  }

  // CdPlanoConta fixo em 31000000 (conforme código C#)
  const cdPlanoConta = 31000000;
  // CdCentroCusto vem do SISCliFa
  const cdCentroCusto = sISCliFa.cdCentroCusto || 0;
  // CdEspecieDocumento sempre 1 para fatura a receber
  const cdEspecieDocumento = 1;

  const sql = `
    EXEC dbo.P_INTEGRACAO_SENIOR_CR_GFATITU_INCLUIR
      @InPagarReceber = 1,
      @CdInscricao = ${toSqlValue(cdInscricao)},
      @CdTitulo = ${toSqlValue(cdTitulo)},
      @DsDigito = ${toSqlValue(dsDigito)},
      @CdFilial = ${cdFilial},
      @CdRepresentante = ${toSqlValue(sISCliFa.CdRepresentante)},
      @CdCarteira = 1,
      @CdCentroCusto = ${cdCentroCusto},
      @CdPortador = ${cdPortador},
      @CdPlanoConta = ${cdPlanoConta},
      @CdEspecieDocumento = ${cdEspecieDocumento},
      @CdMoeda = 1,
      @NrFatura = ${parseInt(cdFatura)},
      @CdHistorico = 1,
      @CdInstrucao = 1,
      @VlOriginal = ${ensureDecimal(fatura.value)},
      @VlSaldo = ${ensureDecimal(fatura.value)},
      @DtVencimento = ${parseDate(fatura.due_date)},
      @DtEmissao = ${parseDate(fatura.issue_date)},
      @DtCompetencia = ${parseDate(fatura.issue_date)},
      @CdParcela = ${parseInt(ano)},
      @DsUsuarioInc = ${toSqlValue('importacaoAFS')},
      @DtGeracao = ${parseDateTime(fatura.issue_date)},
      @HrGeracao = ${parseDateTime(fatura.issue_date)},
      @VlLiquidoOriginal = ${ensureDecimal(fatura.value)};
  `;

  try {
    await prisma.$executeRawUnsafe(sql);
    logger.info({ cdTitulo, cdFatura, document: fatura.document }, 'GFATITU inserido com sucesso');
  } catch (error: any) {
    logger.error({ error: error.message, cdTitulo, cdFatura }, 'Erro ao inserir GFATITU');
    throw error;
  }
};

/**
 * Verifica se GTCFATIT já existe
 */
const verificarGTCFATITExistente = async (
  prisma: PrismaExecutor,
  cdEmpresa: number,
  cdFatura: number,
  cdParcela: number,
  cdEmpresaConhec: number,
  nrSeqControle: number,
): Promise<boolean> => {
  try {
    const sql = `
      SELECT TOP 1 1 as existe
      FROM [${SENIOR_DATABASE}]..GTCFATIT WITH (NOLOCK)
      WHERE CdEmpresa = ${cdEmpresa}
        AND CdFatura = ${cdFatura}
        AND CdParcela = ${cdParcela}
        AND CdEmpresaConhec = ${cdEmpresaConhec}
        AND NrSeqControle = ${nrSeqControle}
    `;
    const rows = await prisma.$queryRawUnsafe<Array<{ existe: number }>>(sql);
    return rows && rows.length > 0 && rows[0]?.existe === 1;
  } catch (error: any) {
    logger.warn(
      {
        error: error.message,
        cdEmpresa,
        cdFatura,
        cdParcela,
        cdEmpresaConhec,
        nrSeqControle,
        SENIOR_DATABASE,
      },
      'Erro ao verificar existência de GTCFATIT',
    );
    return false;
  }
};

/**
 * Insere GTCFATIT (Item da Fatura de Contas a Receber)
 */
const inserirGTCFATIT = async (
  prisma: PrismaExecutor,
  fatura: Fatura,
  faturaItem: FaturaItens,
  cdFatura: string,
  cdEmpresaFatura: string | number, // Empresa da fatura (filial)
  cdEmpresaConhecAux: string, // Empresa do conhecimento (CT-e/NFSe)
  cdNroSeqControleAux: string,
): Promise<void> => {
  // CdEmpresa é a empresa da fatura (filial), extraída do cdFatura ou passada como parâmetro
  const cdEmpresa =
    typeof cdEmpresaFatura === 'string'
      ? cdEmpresaFatura
        ? parseInt(cdEmpresaFatura)
        : 300
      : cdEmpresaFatura || 300;

  const ano = new Date().getFullYear().toString().slice(-2);
  const cdParcela = parseInt(ano);

  // CdEmpresaConhec é a empresa do conhecimento (CT-e/NFSe)
  const cdEmpresaConhec = cdEmpresaConhecAux ? parseInt(cdEmpresaConhecAux) : 300;
  const nrSeqControle = cdNroSeqControleAux ? parseInt(cdNroSeqControleAux) : 0;

  // Verificar se já existe antes de inserir
  const jaExiste = await verificarGTCFATITExistente(
    prisma,
    cdEmpresa,
    parseInt(cdFatura),
    cdParcela,
    cdEmpresaConhec,
    nrSeqControle,
  );

  if (jaExiste) {
    // Log diferenciado para CT-e e NFSe
    const itemTypeNormalized = faturaItem.type ? faturaItem.type.toLowerCase() : '';
    const tipoItem =
      itemTypeNormalized === 'nfse' || itemTypeNormalized === 'nfs' ? 'NFSe' : 'CT-e';
    const identificador =
      itemTypeNormalized === 'nfse' || itemTypeNormalized === 'nfs'
        ? { nfse_number: faturaItem.nfse_number }
        : { cte_key: faturaItem.cte_key };

    logger.info(
      {
        cdEmpresa,
        cdFatura,
        cdParcela,
        cdEmpresaConhec,
        nrSeqControle,
        tipoItem,
        ...identificador,
        document: fatura.document,
      },
      'GTCFATIT já existe, pulando inserção',
    );
    return;
  }

  // IMPORTANTE: Stored procedure específica para inserir GTCFATIT (itens da fatura)
  // Parâmetros obrigatórios:
  // - @CdEmpresa (empresa da fatura)
  // - @CdFatura (código da fatura)
  // - @CdParcela (ano em 2 dígitos, ex: 25)
  // - @CdEmpresaConhec (empresa do conhecimento/CT-e/NFSe)
  // - @NrSeqControle (sequencial do conhecimento)
  const sql = `
    EXEC dbo.P_INTEGRACAO_SENIOR_CR_GTCFATIT_INCLUIR
      @CdEmpresa = ${cdEmpresa},
      @CdFatura = ${parseInt(cdFatura)},
      @CdParcela = ${cdParcela},
      @CdEmpresaConhec = ${cdEmpresaConhec},
      @NrSeqControle = ${nrSeqControle};
  `;

  logger.debug(
    {
      cdEmpresa,
      cdFatura,
      cdParcela,
      cdEmpresaConhec,
      nrSeqControle,
      sql: sql.replace(/\s+/g, ' ').trim(),
    },
    'Executando stored procedure para inserir GTCFATIT',
  );

  try {
    await prisma.$executeRawUnsafe(sql);
    // Log diferenciado para CT-e e NFSe (normalizar tipo para comparação)
    const itemTypeNormalized = faturaItem.type ? faturaItem.type.toLowerCase() : '';
    const tipoItem =
      itemTypeNormalized === 'nfse' || itemTypeNormalized === 'nfs' ? 'NFSe' : 'CT-e';
    const identificador =
      itemTypeNormalized === 'nfse' || itemTypeNormalized === 'nfs'
        ? { nfse_number: faturaItem.nfse_number }
        : { cte_key: faturaItem.cte_key };

    logger.info(
      {
        cdFatura,
        tipoItem,
        ...identificador,
        cdEmpresaConhecimento: cdEmpresaConhec,
        nrSeqControle,
      },
      'GTCFATIT inserido com sucesso',
    );
  } catch (error: any) {
    // Se o erro for de chave duplicada (2627), apenas logar como warning e continuar
    // Isso pode acontecer em casos de concorrência
    if (error.code === 'P2010' || (error.meta && error.meta.code === 2627)) {
      // Log diferenciado para CT-e e NFSe
      const itemTypeNormalized = faturaItem.type ? faturaItem.type.toLowerCase() : '';
      const tipoItem =
        itemTypeNormalized === 'nfse' || itemTypeNormalized === 'nfs' ? 'NFSe' : 'CT-e';
      const identificador =
        itemTypeNormalized === 'nfse' || itemTypeNormalized === 'nfs'
          ? { nfse_number: faturaItem.nfse_number }
          : { cte_key: faturaItem.cte_key };

      logger.warn(
        {
          error: error.message,
          cdEmpresa,
          cdFatura,
          cdParcela,
          cdEmpresaConhec,
          nrSeqControle,
          tipoItem,
          ...identificador,
          document: fatura.document,
        },
        'GTCFATIT já existe (erro de chave duplicada), continuando',
      );
      return; // Não relançar o erro, apenas continuar
    }

    // Para outros erros, logar e relançar
    const itemTypeNormalized = faturaItem.type ? faturaItem.type.toLowerCase() : '';
    const tipoItem =
      itemTypeNormalized === 'nfse' || itemTypeNormalized === 'nfs' ? 'NFSe' : 'CT-e';
    const identificador =
      itemTypeNormalized === 'nfse' || itemTypeNormalized === 'nfs'
        ? { nfse_number: faturaItem.nfse_number }
        : { cte_key: faturaItem.cte_key };

    logger.error(
      {
        error: error.message,
        cdFatura,
        tipoItem,
        ...identificador,
      },
      'Erro ao inserir GTCFATIT',
    );
    throw error;
  }
};

/**
 * Função principal para inserir Contas a Receber no Senior
 */
export const inserirContasReceberSenior = async (
  prisma: PrismaExecutor,
  contasReceber: {
    document: string;
    Fatura: Fatura;
    Filial: Filial;
    Cliente: Cliente;
    ContaContabil: ContaContabil;
    CentroCusto: CentroCusto | null;
    FaturaItens: FaturaItens[];
    Parcelas?: Parcela[];
  },
): Promise<{ success: boolean; cdFatura?: string; error?: string }> => {
  try {
    // Verificar se existe XML
    const existeXML = await verificaExisteXML(prisma, contasReceber.document);
    if (!existeXML) {
      logger.warn(
        { document: contasReceber.document },
        'XML não encontrado para a fatura, pulando integração',
      );
      return {
        success: false,
        error: 'XML não encontrado para a fatura',
      };
    }

    // Calcular ICMS total dos itens
    let vlICMSTotalItens = 0;
    for (const item of contasReceber.FaturaItens) {
      if (item.cte_key) {
        const icms = await retornaICMSPorCte(prisma, item.cte_key);
        vlICMSTotalItens += icms;
      }
    }

    // Obter payment_method da primeira parcela (se disponível)
    const paymentMethod =
      contasReceber.Parcelas && contasReceber.Parcelas.length > 0 && contasReceber.Parcelas[0]
        ? contasReceber.Parcelas[0].payment_method
        : null;

    // Inserir GTCFAT
    const cdFatura = await inserirGTCFAT(
      prisma,
      contasReceber.Fatura,
      contasReceber.Filial,
      contasReceber.Cliente,
      contasReceber.ContaContabil,
      contasReceber.CentroCusto,
      vlICMSTotalItens,
      paymentMethod,
    );

    // Inserir GFATITU
    await inserirGFATITU(
      prisma,
      contasReceber.Fatura,
      contasReceber.Filial,
      contasReceber.Cliente,
      contasReceber.ContaContabil,
      contasReceber.CentroCusto,
      cdFatura,
      paymentMethod,
    );

    // Calcular cdEmpresa da fatura (filial) - será usado para todos os itens
    const cdEmpresaFatura = await retornaCodEmpresa(prisma, contasReceber.Filial.cnpj);

    // Inserir GTCFATIT para cada item
    logger.info(
      {
        document: contasReceber.document,
        totalItens: contasReceber.FaturaItens.length,
        itens: contasReceber.FaturaItens.map((it) => ({
          id: it.id,
          type: it.type,
          hasCteKey: !!it.cte_key,
          hasNfseNumber: !!it.nfse_number,
          cte_key: it.cte_key,
          nfse_number: it.nfse_number,
        })),
      },
      'Iniciando processamento de itens da fatura para GTCFATIT',
    );

    for (const item of contasReceber.FaturaItens) {
      // Normalizar tipo para comparação case-insensitive
      const itemTypeNormalized = item.type ? item.type.toLowerCase() : '';

      // Verificar se é NFSe (type = 'nfse' ou 'NFS', case-insensitive)
      if ((itemTypeNormalized === 'nfse' || itemTypeNormalized === 'nfs') && item.nfse_number) {
        logger.debug(
          {
            itemId: item.id,
            type: item.type,
            nfse_number: item.nfse_number,
            document: contasReceber.document,
          },
          'Processando item NFSe',
        );

        // Buscar NFSe na GTCConhe usando os parâmetros especificados
        // CdEmpresa será calculado baseado no CNPJ da filial (prestador)
        const chavesNFSe = await retornaCodEmpresaSeqControleNFSe(
          prisma,
          item.nfse_number,
          contasReceber.Filial.cnpj,
          contasReceber.document,
        );

        if (chavesNFSe) {
          await inserirGTCFATIT(
            prisma,
            contasReceber.Fatura,
            item,
            cdFatura,
            cdEmpresaFatura, // Empresa da fatura (filial)
            chavesNFSe.cdEmpresa, // Empresa do conhecimento (NFSe)
            chavesNFSe.nrSeqControle,
          );

          // Alterar InFatura no GTCConhe após inserir item (mesma função, mas para NFSe usa GTCConhe)
          await alterarInFaturaGTCCONHE(prisma, chavesNFSe.cdEmpresa, chavesNFSe.nrSeqControle);

          logger.info(
            {
              nfseNumber: item.nfse_number,
              cdFatura,
              cdEmpresaConhecimento: chavesNFSe.cdEmpresa,
              nrSeqControle: chavesNFSe.nrSeqControle,
              document: contasReceber.document,
            },
            'NFSe associada à fatura via GTCFATIT',
          );
        } else {
          logger.warn(
            {
              nfse_number: item.nfse_number,
              cnpjPrestador: contasReceber.Filial.cnpj,
              document: contasReceber.document,
            },
            'Não foi possível obter código empresa e sequencial controle para o item NFSe',
          );
        }
      }
      // Verificar se é CT-e (type não é 'nfse'/'nfs' ou não existe, e tem cte_key)
      else if (itemTypeNormalized !== 'nfse' && itemTypeNormalized !== 'nfs' && item.cte_key) {
        logger.debug(
          { itemId: item.id, cte_key: item.cte_key, document: contasReceber.document },
          'Processando item CT-e',
        );
        const chaves = await retornaCodEmpresaSeqControle(
          prisma,
          item.cte_key,
          contasReceber.document,
        );
        if (chaves) {
          await inserirGTCFATIT(
            prisma,
            contasReceber.Fatura,
            item,
            cdFatura,
            cdEmpresaFatura, // Empresa da fatura (filial)
            chaves.cdEmpresa, // Empresa do conhecimento (CT-e)
            chaves.nrSeqControle,
          );

          // Alterar InFatura no GTCCONHE após inserir item
          await alterarInFaturaGTCCONHE(prisma, chaves.cdEmpresa, chaves.nrSeqControle);
        } else {
          logger.warn(
            { cte_key: item.cte_key, document: contasReceber.document },
            'Não foi possível obter código empresa e sequencial controle para o item CT-e',
          );
        }
      } else {
        logger.debug(
          {
            itemId: item.id,
            type: item.type,
            hasCteKey: !!item.cte_key,
            hasNfseNumber: !!item.nfse_number,
            document: contasReceber.document,
          },
          'Item sem tipo CT-e ou NFSe identificável, pulando associação',
        );
      }
    }

    return {
      success: true,
      cdFatura,
    };
  } catch (error: any) {
    logger.error(
      { error: error.message, document: contasReceber.document },
      'Erro ao inserir Contas a Receber no Senior',
    );
    return {
      success: false,
      error: error.message,
    };
  }
};
