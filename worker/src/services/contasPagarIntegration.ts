import type { Prisma, PrismaClient } from '@prisma/client';
import { logger } from '../utils/logger';
import { env } from '../config/env';
import type {
  Fatura,
  Filial,
  Fornecedor,
  ContaContabil,
  CentroCusto,
  Parcela,
  FaturaItens,
  SISCliFa,
} from '../types/contasPagar';

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

// Garante que nrFatura seja passado corretamente (INT ou BIGINT dependendo do tamanho)
const ensureNrFatura = (value: number): string => {
  const intValue = Math.floor(Number(value));
  // Se o valor excede o limite de INT, usar BIGINT
  if (intValue > 2147483647 || intValue < -2147483648) {
    return `CAST(${intValue} AS BIGINT)`;
  }
  // Caso contrário, passar diretamente (a stored procedure aceita)
  return String(intValue);
};

/**
 * Verifica se já existe uma fatura (GFAFATUR) no Senior para o mesmo documento/CNPJ/CdEmpresa
 * Chave primária: cdInscricao + NrFatura + CdEmpresa
 * Retorna o NrFatura existente ou null se não encontrar.
 */
const verificarGFAFATURExistente = async (
  prisma: PrismaExecutor,
  nrFatura: number,
  fornecedorCnpj: string | null | undefined,
  cdEmpresa: number,
): Promise<number | null> => {
  if (!nrFatura || !fornecedorCnpj || !cdEmpresa) {
    return null;
  }

  try {
    const cdInscricao = fornecedorCnpj.replace(/\D/g, '').padStart(14, '0');

    const sql = `
      SELECT TOP 1 NrFatura
      FROM [${SENIOR_DATABASE}]..GFAFATUR WITH (NOLOCK)
      WHERE NrFatura = ${ensureNrFatura(nrFatura)}
        AND CdInscricao = ${toSqlValue(cdInscricao)}
        AND CdEmpresa = ${cdEmpresa}
    `;

    const rows = await prisma.$queryRawUnsafe<Array<{ NrFatura: number }>>(sql);
    if (rows && rows.length > 0 && rows[0]?.NrFatura) {
      const nr = Number(rows[0].NrFatura);
      if (!Number.isNaN(nr) && nr > 0) {
        logger.info(
          { nrFatura: nr, cdInscricao, cdEmpresa },
          'GFAFATUR existente encontrado no Senior',
        );
        return nr;
      }
    }
    return null;
  } catch (error: any) {
    logger.warn(
      { error: error.message, nrFatura, fornecedorCnpj, cdEmpresa },
      'Erro ao verificar GFAFATUR existente no Senior',
    );
    return null;
  }
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
 * Retorna dados do SISCliFa pelo CNPJ
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
        CdPortador: ensureNumber(row.CdPortadorPagar),
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
 * Completa cadastro do fornecedor na tabela SISCliFa
 */
const completarCadastroFornecedor = async (
  prisma: PrismaExecutor,
  cdInscricao: string,
): Promise<boolean> => {
  try {
    const sql = `
      EXEC dbo.P_SISCLIFA_COMPLETAR_CADASTRO
        @CdInscricao = ${toSqlValue(cdInscricao)},
        @CdPortadorPagar = 1,
        @CdCondicaoPagamento = 1,
        @CdCentroCusto = 1,
        @CdEspeciePagar = 3,
        @CdCarteiraPagar = 999,
        @CdPlanoContaAPagar = 1;
    `;
    await prisma.$executeRawUnsafe(sql);
    logger.info({ cdInscricao }, 'Cadastro do fornecedor completado com sucesso');
    return true;
  } catch (error: any) {
    logger.error({ error: error.message, cdInscricao }, 'Erro ao completar cadastro do fornecedor');
    return false;
  }
};

/**
 * Exclui GFAFATUR antes de inserir
 */
const excluirGFAFATUR = async (
  prisma: PrismaExecutor,
  cdInscricao: string,
  nrFatura: number,
): Promise<void> => {
  try {
    const sql = `
    EXEC dbo.P_INTEGRACAO_SENIOR_CP_GFAFATUR_EXCLUIR
      @CdInscricao = ${toSqlValue(cdInscricao)},
      @NrFatura = ${ensureNrFatura(nrFatura)};
  `;
    await prisma.$executeRawUnsafe(sql);
  } catch (error: any) {
    logger.warn({ error: error.message, cdInscricao, nrFatura }, 'Erro ao excluir GFAFATUR');
  }
};

/**
 * Insere GFAFATUR (Fatura Principal)
 */
const inserirGFAFATUR = async (
  prisma: PrismaExecutor,
  fatura: Fatura,
  filial: Filial,
  fornecedor: Fornecedor,
): Promise<string> => {
  if (!fornecedor.cnpj) {
    throw new Error('CNPJ do fornecedor não informado');
  }
  if (!filial.cnpj) {
    throw new Error('CNPJ da filial não informado');
  }

  const cdInscricao = fornecedor.cnpj.replace(/\D/g, '').padStart(14, '0');
  const cdEmpresa = await retornaCodEmpresa(prisma, filial.cnpj);

  // Gerar NrFatura: apenas o número do document (padrão atual do sistema)
  let nrFatura = 0;
  if (fatura.document) {
    const nrFaturaStr = String(fatura.document).replace(/\D/g, '');
    nrFatura = parseInt(nrFaturaStr, 10);
    if (isNaN(nrFatura) || nrFatura === 0) {
      throw new Error(`Erro ao gerar NrFatura a partir do document: ${fatura.document}`);
    }
  } else {
    throw new Error('Document da fatura não informado');
  }

  // Excluir antes de inserir (usar ensureNrFatura para valores grandes)
  await excluirGFAFATUR(prisma, cdInscricao, nrFatura);

  // Padrões fixos solicitados para Contas a Pagar em GFAFATUR
  const CD_PORTADOR = 999;
  const CD_ESPECIE_DOCUMENTO = 2;
  const CD_PLANO_CONTA = 21101006;
  const CD_CENTRO_CUSTO = 900121;
  const CD_CARTEIRA = 999;

  // Buscar apenas condição de pagamento no SISCliFa (mantemos essa regra)
  let sISCliFa = await retornaDadosSISCliFa(prisma, cdInscricao);
  if (!sISCliFa || sISCliFa.CdCondicaoPagamento === 0) {
    logger.warn(
      { cdInscricao },
      'Dados SISCliFa incompletos (condição pagamento), tentando completar cadastro',
    );
    if (await completarCadastroFornecedor(prisma, cdInscricao)) {
      sISCliFa = await retornaDadosSISCliFa(prisma, cdInscricao);
    }
    if (!sISCliFa || sISCliFa.CdCondicaoPagamento === 0) {
      throw new Error(`Dados SISCliFa incompletos para CNPJ ${cdInscricao} (condição pagamento)`);
    }
  }

  // Validar e preparar valor da fatura
  // REGRA: O valor deve ser gravado em VlFatura e VlBruto (mesmo valor em ambos)
  // VlFaturaAux NÃO deve receber o valor da fatura
  const vlFatura = fatura.value ?? 0;
  const vlFaturaDecimal = ensureDecimal(vlFatura);

  // Log para debug
  logger.info(
    {
      document: fatura.document,
      vlFaturaOriginal: fatura.value,
      vlFaturaProcessado: vlFatura,
      vlFaturaDecimal,
      vlBruto: vlFaturaDecimal,
      mensagem: 'Valor será gravado em VlFatura e VlBruto (mesmo valor), VlFaturaAux será NULL',
    },
    'Valores de fatura para GFAFATUR',
  );

  const sql = `
    EXEC dbo.P_INTEGRACAO_SENIOR_CP_GFAFATUR_INCLUIR
      @CdInscricao = ${toSqlValue(cdInscricao)},
      @NrFatura = ${ensureNrFatura(nrFatura)},
      @CdEmpresa = ${cdEmpresa},
      @CdCarteira = ${CD_CARTEIRA},
      @DtEmissao = ${parseDate(fatura.issue_date)},
      @DtCompetencia = ${parseDate(fatura.issue_date)},
      @CdCentroCusto = ${CD_CENTRO_CUSTO},
      @CdCondicaoVencto = ${sISCliFa.CdCondicaoPagamento},
      @CdEspecieDocumento = ${CD_ESPECIE_DOCUMENTO},
      @CdPortador = ${CD_PORTADOR},
      @CdInstrucao = 1,
      @CdHistorico = 2,
      @DsComplemento = ${toSqlValue('')},
      @CdMoeda = 1,
      @CdPlanoConta = ${CD_PLANO_CONTA},
      @VlFatura = ${vlFaturaDecimal},
      @VlBruto = ${vlFaturaDecimal},
      @VlIrrf = 0,
      @VlInss = 0,
      @VlDesconto = 0,
      @InOrigem = 0,
      @InSituacao = 0,
      @DtCancelamento = NULL,
      @VlSestSenat = 0,
      @InTipoFatura = NULL,
      @VLCOFINS = 0,
      @VlPIS = 0,
      @VlCSLL = 0,
      @VlISS = 0,
      @CdTributacao = 0,
      @VlCSL = 0,
      @InOrigemEmissao = NULL,
      @DsUsuarioAut = NULL,
      @DtInclusao = ${parseDateTime(new Date())},
      @DsUsuarioInc = ${toSqlValue('importacaoAFS')},
      @VlFaturaAux = NULL,
      @DsRefERPExterno = ${toSqlValue(fatura.document || '')},
      @DsStatusERPExterno = ${toSqlValue('PROCESSADO')},
      @DtIntERPExterno = ${parseDateTime(new Date())};
  `;

  try {
    const startTime = Date.now();
    // A procedure retorna um resultado com Status, NrFaturaGerado, LinhasAfetadas
    const result = await prisma.$queryRawUnsafe<Array<any>>(sql);
    const executionTime = Date.now() - startTime;
    if (result && result.length > 0) {
      const row = result[0];
      const status = row.Status || row.status;
      const linhasAfetadas = row.LinhasAfetadas || row.linhasAfetadas || 0;

      if (status === 'SUCCESS' && linhasAfetadas > 0) {
        // Se a procedure retornou um NrFaturaGerado, usar ele, senão usar o que geramos
        const nrFaturaGerado = row.NrFaturaGerado || row.nrFaturaGerado || nrFatura;

        logger.info(
          {
            nrFatura: nrFaturaGerado,
            cdInscricao,
            vlFatura: vlFatura,
            vlBruto: vlFatura,
            executionTimeMs: executionTime,
          },
          'GFAFATUR inserido com sucesso',
        );
        return nrFaturaGerado.toString();
      } else {
        const errorMsg = row.ErrorMessage || row.errorMessage || 'Erro desconhecido';

        // Tratamento específico: fatura já existente no Senior
        if (errorMsg.includes('Fatura já existe com número')) {
          // Verificar se existe fatura com a chave completa (CdInscricao + NrFatura + CdEmpresa)
          const sqlVerificar = `
            SELECT TOP 1 NrFatura
            FROM [${SENIOR_DATABASE}]..GFAFATUR WITH (NOLOCK)
            WHERE CdInscricao = ${toSqlValue(cdInscricao)}
              AND NrFatura = ${ensureNrFatura(nrFatura)}
              AND CdEmpresa = ${cdEmpresa}
          `;

          try {
            const rowsVerificar =
              await prisma.$queryRawUnsafe<Array<{ NrFatura: number }>>(sqlVerificar);
            if (rowsVerificar && rowsVerificar.length > 0 && rowsVerificar[0]?.NrFatura) {
              // Existe com a chave completa -> usar o NrFatura existente
              logger.info(
                { nrFatura, cdInscricao, cdEmpresa, errorMsg },
                'GFAFATUR já existia no Senior (chave completa), reutilizando NrFatura',
              );
              return nrFatura.toString();
            }

            // Se chegou aqui, a procedure retornou duplicidade mas não achamos a chave tripla.
            // Significa que existe para OUTRO fornecedor.
            // Como solicitado, não tentamos "consertar" ou reutilizar. Deixamos cair no throw error abaixo.
            logger.warn(
              { nrFatura, cdInscricao, cdEmpresa, errorMsg },
              'GFAFATUR duplicado para outro fornecedor (procedure retornou erro e chave tripla não encontrada).',
            );
          } catch (errorVerificar: any) {
            logger.warn(
              { error: errorVerificar.message, nrFatura, cdInscricao, cdEmpresa },
              'Erro ao verificar GFAFATUR existente após erro "já existe", usando NrFatura original',
            );
          }
        }

        throw new Error(`Falha ao inserir GFAFATUR: ${errorMsg}`);
      }
    } else {
      // Se não retornou resultado, assumir que foi inserido com sucesso (compatibilidade)
      logger.warn({ nrFatura, cdInscricao }, 'Procedure não retornou resultado, assumindo sucesso');
      return nrFatura.toString();
    }
  } catch (error: any) {
    // Se o erro vier diretamente da chamada SQL (ex: RAISERROR na procedure),
    // ainda tratamos o caso de fatura já existente aqui.
    const msg = error.message || '';

    // Tratamento específico para timeout
    if (
      msg.includes('timeout') ||
      msg.includes('ETIMEDOUT') ||
      msg.includes('Request timeout') ||
      error?.code === 'ETIMEDOUT' ||
      error?.code === 'P2028'
    ) {
      logger.error(
        {
          error: msg,
          nrFatura,
          cdInscricao,
          code: error?.code,
          meta: error?.meta,
        },
        'Timeout ao inserir GFAFATUR - operação demorou muito para ser concluída',
      );
      throw new Error('timeout na GFAFATUR');
    }

    if (msg.includes('Fatura já existe com número')) {
      // Verificar se existe fatura com a chave completa (CdInscricao + NrFatura + CdEmpresa)
      const sqlVerificar = `
        SELECT TOP 1 NrFatura
        FROM [${SENIOR_DATABASE}]..GFAFATUR WITH (NOLOCK)
        WHERE CdInscricao = ${toSqlValue(cdInscricao)}
          AND NrFatura = ${ensureNrFatura(nrFatura)}
          AND CdEmpresa = ${cdEmpresa}
      `;

      try {
        const rowsVerificar =
          await prisma.$queryRawUnsafe<Array<{ NrFatura: number }>>(sqlVerificar);
        if (rowsVerificar && rowsVerificar.length > 0 && rowsVerificar[0]?.NrFatura) {
          // Existe com a chave completa -> usar o NrFatura existente
          logger.info(
            { nrFatura, cdInscricao, cdEmpresa, error: msg },
            'GFAFATUR já existia no Senior (chave completa, erro no catch), reutilizando NrFatura',
          );
          return nrFatura.toString();
        }

        // Se chegou aqui, conflito real (mesmo número, outro fornecedor).
        logger.warn(
          { nrFatura, cdInscricao, cdEmpresa, error: msg },
          'GFAFATUR duplicado para outro fornecedor (erro no catch).',
        );
      } catch (errorVerificar: any) {
        logger.warn(
          { error: errorVerificar.message, nrFatura, cdInscricao, cdEmpresa },
          'Erro ao verificar GFAFATUR existente após erro "já existe" (catch), usando NrFatura original',
        );
        return nrFatura.toString();
      }
    }

    logger.error({ error: msg, nrFatura, cdInscricao }, 'Erro ao inserir GFAFATUR');
    throw error;
  }
};

/**
 * Exclui GFAFATRA antes de inserir
 */
const excluirGFAFATRA = async (
  prisma: PrismaExecutor,
  cdInscricao: string,
  nrFatura: number,
): Promise<void> => {
  try {
    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_CP_GFAFATRA_EXCLUIR
        @CdInscricao = ${toSqlValue(cdInscricao)},
        @NrFatura = ${ensureNrFatura(nrFatura)};
    `;
    await prisma.$executeRawUnsafe(sql);
  } catch (error: any) {
    logger.warn({ error: error.message, cdInscricao, nrFatura }, 'Erro ao excluir GFAFATRA');
  }
};

/**
 * Insere GFAFATRA (Rateio da Fatura)
 */
const inserirGFAFATRA = async (
  prisma: PrismaExecutor,
  fatura: Fatura,
  fornecedor: Fornecedor,
  nrFatura: number,
): Promise<void> => {
  if (!fornecedor.cnpj) {
    throw new Error('CNPJ do fornecedor não informado');
  }

  const cdInscricao = fornecedor.cnpj.replace(/\D/g, '').padStart(14, '0');

  // Excluir antes de inserir
  await excluirGFAFATRA(prisma, cdInscricao, nrFatura);

  // Padrões fixos solicitados para Contas a Pagar em GFAFATRA
  const CD_PLANO_CONTA = 21101006;
  const CD_CENTRO_CUSTO = 900121;

  const sql = `
    EXEC dbo.P_INTEGRACAO_SENIOR_CP_GFAFATRA_INCLUIR
      @CdInscricao = ${toSqlValue(cdInscricao)},
      @NrFatura = ${ensureNrFatura(nrFatura)},
      @CdPlanoConta = ${CD_PLANO_CONTA},
      @CdCentroCusto = ${CD_CENTRO_CUSTO},
      @VlLancamento = ${ensureDecimal(fatura.value)},
      @VlLancamentoAux = 0;
  `;

  await prisma.$executeRawUnsafe(sql);
  logger.info({ nrFatura, cdInscricao }, 'GFAFATRA inserido com sucesso');
};

/**
 * Exclui GFAFatNF antes de inserir
 */
const excluirGFAFatNF = async (
  prisma: PrismaExecutor,
  cdInscricao: string,
  nrFatura: number,
): Promise<void> => {
  try {
    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_CP_GFAFATNF_EXCLUIR
        @CdInscricao = ${toSqlValue(cdInscricao)},
        @NrFatura = ${ensureNrFatura(nrFatura)};
    `;
    await prisma.$executeRawUnsafe(sql);
  } catch (error: any) {
    logger.warn({ error: error.message, cdInscricao, nrFatura }, 'Erro ao excluir GFAFatNF');
  }
};

/**
 * Insere GFAFatNF (Nota Fiscal)
 */
const inserirGFAFatNF = async (
  prisma: PrismaExecutor,
  fatura: Fatura,
  filial: Filial,
  fornecedor: Fornecedor,
  nrFatura: number,
): Promise<void> => {
  if (!fornecedor.cnpj) {
    throw new Error('CNPJ do fornecedor não informado');
  }
  if (!filial.cnpj) {
    throw new Error('CNPJ da filial não informado');
  }

  const cdInscricao = fornecedor.cnpj.replace(/\D/g, '').padStart(14, '0');
  const cdEmpresa = await retornaCodEmpresa(prisma, filial.cnpj);

  // Excluir antes de inserir
  await excluirGFAFatNF(prisma, cdInscricao, nrFatura);

  // Buscar dados SISCliFa
  let sISCliFa = await retornaDadosSISCliFa(prisma, cdInscricao);

  if (!sISCliFa || sISCliFa.CdEspeciePagar === 0) {
    logger.warn({ cdInscricao }, 'Dados SISCliFa incompletos, tentando completar cadastro');
    if (await completarCadastroFornecedor(prisma, cdInscricao)) {
      sISCliFa = await retornaDadosSISCliFa(prisma, cdInscricao);
    }
    if (!sISCliFa || sISCliFa.CdEspeciePagar === 0) {
      throw new Error(`Dados SISCliFa incompletos para GFAFatNF (CNPJ: ${cdInscricao})`);
    }
  }

  const sql = `
    EXEC dbo.P_INTEGRACAO_SENIOR_CP_GFAFATNF_INCLUIR
      @CdInscricao = ${toSqlValue(cdInscricao)},
      @NrFatura = ${ensureNrFatura(nrFatura)},
      @CdEmpresa = ${cdEmpresa},
      @CdInscricaoDocto = ${toSqlValue(cdInscricao)},
      @CdTipoDocumento = ${sISCliFa.CdEspeciePagar},
      @NrSerieDocumento = ${toSqlValue('UNI')},
      @NrDocumento = ${ensureNrFatura(nrFatura)};
  `;

  await prisma.$executeRawUnsafe(sql);
  logger.info({ nrFatura, cdInscricao }, 'GFAFatNF inserido com sucesso');
};

/**
 * Exclui GFATITU antes de inserir
 */
const excluirGFATITU = async (
  prisma: PrismaExecutor,
  cdInscricao: string,
  nrFatura: number,
): Promise<void> => {
  try {
    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_CR_GFATITU_EXCLUIR
        @CdInscricao = ${toSqlValue(cdInscricao)},
        @NrFatura = ${ensureNrFatura(nrFatura)};
    `;
    await prisma.$executeRawUnsafe(sql);
  } catch (error: any) {
    logger.warn({ error: error.message, cdInscricao, nrFatura }, 'Erro ao excluir GFATITU');
  }
};

/**
 * Gera CdTitulo para Contas a Pagar.
 *
 * Regra (padrão produção):
 *   CdTitulo = NumeroFatura + SeqParcela(2 dígitos)
 * Exemplo:
 *   document = 23198, seqParcela = 1  => CdTitulo = 2319801
 */
const gerarCdTitulo = (
  document: string | null | undefined,
  nrFatura: number,
  seqParcela: number,
): string => {
  const baseNumero =
    (document && document.toString().replace(/\D/g, '')) || nrFatura.toString().replace(/\D/g, '');

  const parcelaStr = seqParcela.toString().padStart(2, '0');

  return `${baseNumero}${parcelaStr}`;
};

/**
 * Insere GFATITU (Título)
 */
const inserirGFATITU = async (
  prisma: PrismaExecutor,
  fatura: Fatura,
  filial: Filial,
  fornecedor: Fornecedor,
  nrFatura: number,
  seqParcela: number,
  vlDesconto: number = 0,
): Promise<string> => {
  if (!fornecedor.cnpj) {
    throw new Error('CNPJ do fornecedor não informado');
  }
  if (!filial.cnpj) {
    throw new Error('CNPJ da filial não informado');
  }

  const cdInscricao = fornecedor.cnpj.replace(/\D/g, '').padStart(14, '0');
  const cdEmpresa = await retornaCodEmpresa(prisma, filial.cnpj);
  const cdFilial = cdEmpresa;

  // Excluir antes de inserir
  await excluirGFATITU(prisma, cdInscricao, nrFatura);

  // Gerar CdTitulo conforme padrão do Contas a Pagar:
  // Numero da fatura (document) + 2 dígitos da parcela (ex: 23198 + 01 = 2319801)
  const cdTitulo = gerarCdTitulo(fatura.document || null, nrFatura, seqParcela);

  // Padrões fixos solicitados para Contas a Pagar
  const CD_PORTADOR = 999;
  const CD_ESPECIE_DOCUMENTO = 2;
  const CD_PLANO_CONTA = 21101006;
  const CD_CENTRO_CUSTO = 900121;
  const CD_CARTEIRA = 999;

  // Calcular saldo considerando desconto
  const vlOriginal = ensureNumber(fatura.value);
  const vlSaldo = Math.max(0, vlOriginal - vlDesconto);

  const sql = `
    EXEC dbo.P_INTEGRACAO_SENIOR_CR_GFATITU_INCLUIR
      @InPagarReceber = 0,
      @CdInscricao = ${toSqlValue(cdInscricao)},
      @CdTitulo = ${toSqlValue(cdTitulo)},
      @CdFilial = ${cdFilial},
      @CdCarteira = ${CD_CARTEIRA},
      @CdCentroCusto = ${CD_CENTRO_CUSTO},
      @CdPortador = ${CD_PORTADOR},
      @CdSituacao = 1,
      @CdPlanoConta = ${CD_PLANO_CONTA},
      @CdEspecieDocumento = ${CD_ESPECIE_DOCUMENTO},
      @CdMoeda = 1,
      @NrFatura = ${ensureNrFatura(nrFatura)},
      @CdHistorico = 2,
      @DsComplemento = ${toSqlValue('')},
      @CdInstrucao = 1,
      @VlOriginal = ${ensureDecimal(vlOriginal)},
      @VlSaldo = ${ensureDecimal(vlSaldo)},
      @InRejeitado = 0,
      @DtVencimento = ${parseDate(fatura.due_date)},
      @DtEmissao = ${parseDate(fatura.issue_date)},
      @DtCompetencia = ${parseDate(fatura.issue_date)},
      @DtPagto = NULL,
      @InSituacao = 0,
      @DsUsuarioInc = ${toSqlValue('importacaoAFS')},
      @DtGeracao = ${parseDate(fatura.issue_date)},
      @HrGeracao = ${parseDate(fatura.issue_date)},
      @InTpTitulo = NULL;
  `;

  await prisma.$executeRawUnsafe(sql);
  logger.info({ cdTitulo, nrFatura, cdInscricao, vlSaldo }, 'GFATITU inserido com sucesso');
  return cdTitulo;
};

/**
 * Insere GFATITRA (Rateio do Título)
 */
const inserirGFATITRA = async (
  prisma: PrismaExecutor,
  fatura: Fatura,
  fornecedor: Fornecedor,
  nrFatura: number,
  cdTitulo: string,
  vlDesconto: number = 0,
): Promise<void> => {
  if (!fornecedor.cnpj) {
    throw new Error('CNPJ do fornecedor não informado');
  }
  if (!cdTitulo) {
    throw new Error('CdTitulo não informado');
  }

  const cdInscricao = fornecedor.cnpj.replace(/\D/g, '').padStart(14, '0');

  // Padrões fixos solicitados para Contas a Pagar (mesmos valores de GFAFATRA e GFATITU)
  const CD_PLANO_CONTA = 21101006;
  const CD_CENTRO_CUSTO = 900121;

  // Calcular saldo considerando desconto
  const vlRealizado = ensureNumber(fatura.value);
  const vlSaldo = Math.max(0, vlRealizado - vlDesconto);

  const sql = `
    EXEC dbo.P_INTEGRACAO_SENIOR_CP_GFATITRA_INCLUIR
      @InPagarReceber = 0,
      @CdInscricao = ${toSqlValue(cdInscricao)},
      @CdTitulo = ${toSqlValue(cdTitulo)},
      @CdPlanoConta = ${CD_PLANO_CONTA},
      @CdCentroCusto = ${CD_CENTRO_CUSTO},
      @VlRealizado = ${ensureDecimal(vlRealizado)},
      @VlPago = 0,
      @VlSaldo = ${ensureDecimal(vlSaldo)},
      @VlAcrescimo = 0,
      @VlDesconto = ${ensureDecimal(vlDesconto)},
      @VlRealizadoAux = 0;
  `;

  await prisma.$executeRawUnsafe(sql);
  logger.info(
    { cdTitulo, nrFatura, cdInscricao, vlSaldo, vlDesconto },
    'GFATITRA inserido com sucesso',
  );
};

/**
 * Insere GFAMovTi (Movimento do Título - Desconto)
 */
const inserirGFAMovTi = async (
  prisma: PrismaExecutor,
  fatura: Fatura,
  filial: Filial,
  fornecedor: Fornecedor,
  nrFatura: number,
  cdTitulo: string,
  vlDesconto: number,
  observacao: string,
): Promise<void> => {
  if (!fornecedor.cnpj) {
    throw new Error('CNPJ do fornecedor não informado');
  }
  if (!cdTitulo) {
    throw new Error('CdTitulo não informado');
  }

  const cdInscricao = fornecedor.cnpj.replace(/\D/g, '').padStart(14, '0');
  const cdEmpresa = await retornaCodEmpresa(prisma, filial.cnpj);
  const cdFilial = cdEmpresa;

  // Padrões fixos
  const CD_TIPO_LANCAMENTO = 5; // Desconto
  const CD_HISTORICO = 2;

  const sql = `
    INSERT INTO [${SENIOR_DATABASE}]..GFAMovTi (
      CdEmpresa,
      CdFilial,
      CdInscricao,
      CdTitulo,
      CdTipoLancamento,
      DtMovimento,
      VlMovimento,
      CdHistorico,
      DsObservacao,
      DtInclusao,
      DsUsuarioInc
    ) VALUES (
      ${cdEmpresa},
      ${cdFilial},
      ${toSqlValue(cdInscricao)},
      ${toSqlValue(cdTitulo)},
      ${CD_TIPO_LANCAMENTO},
      ${parseDate(fatura.issue_date)},
      ${ensureDecimal(vlDesconto)},
      ${CD_HISTORICO},
      ${toSqlValue(observacao)},
      ${parseDateTime(new Date())},
      ${toSqlValue('importacaoAFS')}
    )
  `;

  await prisma.$executeRawUnsafe(sql);
  logger.info(
    { cdTitulo, nrFatura, cdInscricao, vlDesconto },
    'GFAMovTi (Desconto) inserido com sucesso',
  );
};

/**
 * Função principal para inserir Contas a Pagar no Senior
 */
export async function inserirContasPagarSenior(
  prisma: PrismaExecutor,
  fatura: Fatura,
  filial: Filial,
  fornecedor: Fornecedor,
  contaContabil: ContaContabil,
  centroCusto: CentroCusto | null,
  parcelas: Parcela[],
  faturaItens: FaturaItens[],
): Promise<{
  success: boolean;
  error?: string;
  nrFatura?: number;
  cdTitulo?: string;
  tabelasInseridas?: string[];
  tabelasFalhadas?: Array<{ tabela: string; erro: string }>;
}> {
  const startTime = Date.now();
  const tabelasInseridas: string[] = [];
  const tabelasFalhadas: Array<{ tabela: string; erro: string }> = [];

  try {
    // 1. Obter / Inserir GFAFATUR (crítico - precisa do nrFatura para as próximas operações)
    let nrFatura: number;

    // Obter CdEmpresa para verificação (chave: cdInscricao + NrFatura + CdEmpresa)
    const cdEmpresa = await retornaCodEmpresa(prisma, filial.cnpj);

    // Gerar NrFatura a partir do document para verificação
    const nrFaturaBase = fatura.document
      ? parseInt(String(fatura.document).replace(/\D/g, ''), 10)
      : 0;

    // Verificar se já existe (com a combinação correta: NrFatura + CdEmpresa + CdInscricao)
    const existingNrFatura = await verificarGFAFATURExistente(
      prisma,
      nrFaturaBase,
      fornecedor.cnpj,
      cdEmpresa,
    );

    if (existingNrFatura) {
      nrFatura = existingNrFatura;
      tabelasInseridas.push('GFAFATUR (já existia)');
      logger.info(
        { nrFatura, cdInscricao: fornecedor.cnpj, cdEmpresa },
        'GFAFATUR já existia (verificação prévia), pulando inserção',
      );
    } else {
      // 1.1 Tentar inserir/atualizar GFAFATUR normalmente (sempre, para garantir update)
      try {
        const nrFaturaStr = await inserirGFAFATUR(prisma, fatura, filial, fornecedor);
        nrFatura = parseInt(nrFaturaStr, 10);

        if (!nrFatura || Number.isNaN(nrFatura) || nrFatura === 0) {
          throw new Error('NrFatura não foi gerado corretamente');
        }

        tabelasInseridas.push('GFAFATUR');
      } catch (error: any) {
        const errorMsg = error.message || 'Erro desconhecido';
        const isTimeout =
          errorMsg.includes('timeout') ||
          errorMsg.includes('ETIMEDOUT') ||
          errorMsg === 'timeout na GFAFATUR' ||
          error?.code === 'ETIMEDOUT' ||
          error?.code === 'P2028';

        const erroFormatado = isTimeout ? 'timeout na GFAFATUR' : errorMsg;

        tabelasFalhadas.push({ tabela: 'GFAFATUR', erro: erroFormatado });
        logger.error(
          {
            error: errorMsg,
            tabela: 'GFAFATUR',
            isTimeout,
            code: error?.code,
          },
          'Erro ao inserir GFAFATUR',
        );

        return {
          success: false,
          error: erroFormatado,
          tabelasInseridas,
          tabelasFalhadas,
        };
      }
    }

    // 2. Inserir GFAFATRA (Rateio da Fatura)
    try {
      await inserirGFAFATRA(prisma, fatura, fornecedor, nrFatura);
      tabelasInseridas.push('GFAFATRA');
    } catch (error: any) {
      const msgErro = error?.message || '';

      // Tratamento idempotente: se já existe registro em GFAFATRA (PK duplicada), considerar sucesso
      if (msgErro.includes('2627') && msgErro.includes('GFAFATRA')) {
        tabelasInseridas.push('GFAFATRA (já existia)');
        logger.info(
          { document: fatura.document, nrFatura, cdInscricao: fornecedor.cnpj },
          'GFAFATRA já existia no Senior, tratando como sucesso',
        );
      } else {
        tabelasFalhadas.push({ tabela: 'GFAFATRA', erro: msgErro || 'Erro desconhecido' });
        logger.error({ error: msgErro, tabela: 'GFAFATRA' }, 'Erro ao inserir GFAFATRA');
      }
    }

    // 3. Inserir GFAFatNF (Nota Fiscal)
    try {
      await inserirGFAFatNF(prisma, fatura, filial, fornecedor, nrFatura);
      tabelasInseridas.push('GFAFatNF');
    } catch (error: any) {
      const msgErro = error?.message || '';

      // Tratamento idempotente: se já existe registro em GFAFatNF (PK duplicada), considerar sucesso
      if (msgErro.includes('2627') && msgErro.includes('GFAFatNF0')) {
        tabelasInseridas.push('GFAFatNF (já existia)');
        logger.info(
          { document: fatura.document, nrFatura, cdInscricao: fornecedor.cnpj },
          'GFAFatNF já existia no Senior, tratando como sucesso',
        );
      } else {
        tabelasFalhadas.push({ tabela: 'GFAFatNF', erro: msgErro || 'Erro desconhecido' });
        logger.error({ error: msgErro, tabela: 'GFAFatNF' }, 'Erro ao inserir GFAFatNF');
      }
    }

    // 4. Inserir GFATITU (Título) - sempre 1 parcela (crítico)
    let cdTitulo: string | undefined;

    // Obter desconto total da primeira parcela (assumindo 1 parcela principal)
    const parcelaPrincipal = parcelas && parcelas.length > 0 ? parcelas[0] : null;
    const vlDesconto = parcelaPrincipal ? ensureNumber(parcelaPrincipal.discount_value) : 0;
    const observacao = parcelaPrincipal?.comments || '';

    try {
      const seqParcela = 1;
      // Passar desconto para função
      cdTitulo = await inserirGFATITU(
        prisma,
        fatura,
        filial,
        fornecedor,
        nrFatura,
        seqParcela,
        vlDesconto,
      );

      if (!cdTitulo) {
        throw new Error('CdTitulo não foi gerado corretamente');
      }

      tabelasInseridas.push('GFATITU');
    } catch (error: any) {
      const msgErro = error?.message || '';

      // Tratamento idempotente: se já existe registro em GFATITU (PK duplicada), considerar sucesso
      if (msgErro.includes('2627') && msgErro.includes('GFATitu0')) {
        const seqParcela = 1;
        cdTitulo = gerarCdTitulo(fatura.document || null, nrFatura, seqParcela);
        tabelasInseridas.push('GFATITU (já existia)');
        logger.info(
          { document: fatura.document, nrFatura, cdTitulo, cdInscricao: fornecedor.cnpj },
          'GFATITU já existia no Senior, tratando como sucesso',
        );
      } else {
        tabelasFalhadas.push({ tabela: 'GFATITU', erro: msgErro || 'Erro desconhecido' });
        logger.error({ error: msgErro, tabela: 'GFATITU' }, 'Erro ao inserir GFATITU');
        // GFATITU é crítico, mas continuamos para tentar inserir GFATITRA
      }
    }

    // 5. Inserir GFATITRA (Rateio do Título) - só se GFATITU foi inserido
    if (cdTitulo) {
      try {
        await inserirGFATITRA(prisma, fatura, fornecedor, nrFatura, cdTitulo, vlDesconto);
        tabelasInseridas.push('GFATITRA');

        // 6. Inserir GFAMovTi (Movimento de Desconto) - se houver desconto
        if (vlDesconto > 0) {
          try {
            await inserirGFAMovTi(
              prisma,
              fatura,
              filial,
              fornecedor,
              nrFatura,
              cdTitulo,
              vlDesconto,
              observacao,
            );
            tabelasInseridas.push('GFAMovTi (Desconto)');
          } catch (error: any) {
            // Falha ao inserir movimento não deve travar todo o processo, mas logamos
            tabelasFalhadas.push({
              tabela: 'GFAMovTi',
              erro: error.message || 'Erro desconhecido',
            });
            logger.error(
              { error: error.message, tabela: 'GFAMovTi' },
              'Erro ao inserir GFAMovTi (Desconto)',
            );
          }
        }
      } catch (error: any) {
        const msgErro = error?.message || '';

        // Tratamento idempotente: se já existe registro em GFATITRA (PK duplicada), considerar sucesso
        if (msgErro.includes('2627') && msgErro.includes('GFATITRA0')) {
          tabelasInseridas.push('GFATITRA (já existia)');
          logger.info(
            { document: fatura.document, nrFatura, cdTitulo, cdInscricao: fornecedor.cnpj },
            'GFATITRA já existia no Senior, tratando como sucesso',
          );
        } else {
          tabelasFalhadas.push({ tabela: 'GFATITRA', erro: msgErro || 'Erro desconhecido' });
          logger.error({ error: msgErro, tabela: 'GFATITRA' }, 'Erro ao inserir GFATITRA');
        }
      }
    }

    const integrationTimeMs = Date.now() - startTime;

    // Verificar se as tabelas críticas foram tratadas com sucesso (incluindo casos "já existia")
    const temGFAFATUR = tabelasInseridas.some((t) => t.startsWith('GFAFATUR'));
    const temGFATITU = tabelasInseridas.some((t) => t.startsWith('GFATITU'));

    // Se alguma tabela crítica não foi inserida/tratada, retornar erro
    if (!temGFAFATUR || !temGFATITU) {
      const detalhesFalhas = tabelasFalhadas.map((t) => `${t.tabela} (${t.erro})`).join(', ');
      const erroCompleto = detalhesFalhas
        ? `Erro ao inserir contas a pagar: ${detalhesFalhas}`
        : 'Erro ao inserir contas a pagar: Tabelas críticas não inseridas (GFAFATUR ou GFATITU)';
      logger.error(
        {
          document: fatura.document,
          tabelasInseridas,
          tabelasFalhadas,
          integrationTimeMs,
        },
        erroCompleto,
      );
      return {
        success: false,
        error: erroCompleto,
        tabelasInseridas,
        tabelasFalhadas,
      };
    }

    logger.info(
      {
        document: fatura.document,
        nrFatura,
        cdTitulo,
        integrationTimeMs,
        tabelasInseridas,
        tabelasFalhadas: tabelasFalhadas.length > 0 ? tabelasFalhadas : undefined,
      },
      'Contas a pagar inserida no Senior',
    );

    const resultado: {
      success: boolean;
      nrFatura: number;
      cdTitulo?: string;
      tabelasInseridas: string[];
      tabelasFalhadas?: Array<{ tabela: string; erro: string }>;
    } = {
      success: true,
      nrFatura,
      tabelasInseridas,
    };

    if (cdTitulo) {
      resultado.cdTitulo = cdTitulo;
    }

    if (tabelasFalhadas.length > 0) {
      resultado.tabelasFalhadas = tabelasFalhadas;
    }

    return resultado;
  } catch (error: any) {
    const integrationTimeMs = Date.now() - startTime;
    logger.error(
      {
        error: error.message,
        document: fatura.document,
        integrationTimeMs,
        tabelasInseridas,
        tabelasFalhadas,
      },
      'Erro ao inserir contas a pagar no Senior',
    );

    const resultado: {
      success: boolean;
      error: string;
      tabelasInseridas?: string[];
      tabelasFalhadas: Array<{ tabela: string; erro: string }>;
    } = {
      success: false,
      error: error.message || 'Erro desconhecido',
      tabelasFalhadas,
    };

    if (tabelasInseridas.length > 0) {
      resultado.tabelasInseridas = tabelasInseridas;
    }

    return resultado;
  }
}
