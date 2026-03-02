import type { Prisma, PrismaClient } from '@prisma/client';
import { logger } from '../utils/logger';
import type { CteData } from '../types/cte';
import { obterProximoNrSeqControle } from '../utils/nrSeqControle';
import { env } from '../config/env';
import {
  extrairCepRemetente,
  extrairCepDestinatario,
  extrairPesoReal,
  extrairPesoCubado,
  extrairQuantidadeVolumes,
  extrairValorCarga,
  extrairComponentesFrete,
  extrairIERemetente,
  extrairIEDestinatario,
  extrairCFOP,
  extrairNaturezaOperacao,
} from './cteIntegrationHelper';

type PrismaExecutor = PrismaClient | Prisma.TransactionClient;

// Banco de dados Senior configurado via variável de ambiente
const SENIOR_DATABASE = env.SENIOR_DATABASE;
// Banco de dados AFS_Integrador (onde está a tabela EmpresaSenior)
const AFS_INTEGRADOR_DATABASE = 'AFS_Integrador';
const IS_POSTGRES = env.DATABASE_URL.startsWith('postgresql://');
const shouldBypassCteSeniorLegacy = (): boolean => IS_POSTGRES && !env.ENABLE_SQLSERVER_LEGACY;

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

const ensureNumber = (value: number | null | undefined): number => {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return 0;
  return Number(value);
};

/**
 * Normaliza valor monetário para a stored procedure (multiplica por 100)
 * A procedure divide por 100 antes de inserir
 */
const normalizeValorMonetario = (value: number | null | undefined): string => {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '0';
  // Multiplicar por 100 para que a procedure divida e obtenha o valor correto
  return String(Math.round((value || 0) * 100));
};

/**
 * Normaliza peso/volume para a stored procedure
 * IMPORTANTE: Valores muito grandes podem exceder DECIMAL(18,4) quando multiplicados por 10000
 * DECIMAL(18,4) máximo: 999,999,999,999,999.9999
 * Quando multiplicamos por 10000, o máximo seguro é: 99,999,999,999.9999
 *
 * Estratégia: Se o valor * 10000 exceder o limite seguro, passar o valor direto como DECIMAL
 * Caso contrário, multiplicar por 10000 (se a procedure esperar isso)
 */
const normalizePesoVolume = (value: number | null | undefined): string => {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '0';

  const valorFinal = Number(value) || 0;
  const multiplicador = 10000;

  // Limite seguro: DECIMAL(18,4) máximo é 999,999,999,999,999.9999
  // Quando multiplicamos por 10000, o máximo seguro é: 99,999,999,999.9999
  // Mas valores como 2142453.3900 * 10000 = 21,424,533,900 excedem isso
  // Por segurança, se o valor original > 1000000, passar direto como DECIMAL
  const limiteSeguro = 1000000; // Valores acima disso podem causar overflow

  // Se o valor for muito grande, passar direto como DECIMAL (sem multiplicar)
  if (Math.abs(valorFinal) > limiteSeguro) {
    logger.warn(
      {
        valorOriginal: valorFinal,
        valorMultiplicado: valorFinal * multiplicador,
        limiteSeguro,
        acao: 'Passando valor direto como DECIMAL para evitar overflow',
      },
      'Valor de peso/volume muito grande, passando direto sem multiplicar',
    );
    // Passar como DECIMAL direto (com até 4 casas decimais, sem aspas para SQL)
    const valorFormatado = valorFinal.toFixed(4);
    // Garantir que não seja NaN ou Infinity
    if (!Number.isFinite(Number(valorFormatado))) {
      logger.error(
        { valorFinal, valorFormatado },
        'Valor inválido detectado em normalizePesoVolume, usando 0',
      );
      return '0';
    }
    return valorFormatado;
  }

  // Valores dentro do limite: multiplicar por 10000
  const valorNormalizado = Math.round(valorFinal * multiplicador);
  // Garantir que não seja NaN ou Infinity
  if (!Number.isFinite(valorNormalizado)) {
    logger.error(
      { valorFinal, valorNormalizado },
      'Valor normalizado inválido detectado, usando 0',
    );
    return '0';
  }
  return String(valorNormalizado);
};

const ensureDecimal = (value: number | null | undefined): string => {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '0';
  return String(value);
};

/**
 * Formata valor numérico para SQL, garantindo que seja um número válido
 * Remove notação científica e garante formato numérico válido para SQL Server
 */
const toSqlNumeric = (value: string | number | null | undefined): string => {
  if (value === null || value === undefined) return '0';

  let numValue: number;
  if (typeof value === 'string') {
    // Remover espaços e converter para número
    const cleanValue = value.trim();
    if (cleanValue === '' || cleanValue === 'NULL') return '0';
    numValue = parseFloat(cleanValue);
  } else {
    numValue = value;
  }

  // Validar se é um número válido
  if (Number.isNaN(numValue) || !Number.isFinite(numValue)) {
    logger.warn({ value, numValue }, 'Valor numérico inválido em toSqlNumeric, usando 0');
    return '0';
  }

  // Para valores muito grandes, usar notação sem exponencial
  // SQL Server aceita números até DECIMAL(38,0) aproximadamente
  if (Math.abs(numValue) > Number.MAX_SAFE_INTEGER) {
    // Usar toFixed para evitar notação científica
    return numValue.toFixed(0);
  }

  // Retornar como número puro (sem aspas) para SQL
  // Remover notação científica se houver
  const strValue = String(numValue);
  if (strValue.includes('e') || strValue.includes('E')) {
    // Converter notação científica para número normal
    return numValue.toFixed(0);
  }

  return strValue;
};

const padLeft = (value: string | null | undefined, length: number): string => {
  if (!value) return ''.padStart(length, '0');
  const onlyDigits = value.replace(/\D/g, '');
  return onlyDigits.padStart(length, '0');
};

/**
 * Retorna código de empresa baseado no CNPJ
 */
export const buildCdEmpresaFromCnpj = async (
  prisma: PrismaExecutor,
  cnpj: string | null | undefined,
): Promise<number> => {
  if (IS_POSTGRES && !env.ENABLE_SQLSERVER_LEGACY) {
    logger.debug(
      { cnpj, isPostgres: IS_POSTGRES, enableSqlServerLegacy: env.ENABLE_SQLSERVER_LEGACY },
      'Lookup de empresa Senior por CNPJ desativado em modo PostgreSQL; usando fallback 300',
    );
    return 300;
  }

  if (!cnpj) {
    return 300;
  }
  const cnpjClean = padLeft(cnpj, 14);
  const sql = `EXEC dbo.P_EMPRESA_SENIOR_POR_CNPJ_LISTAR @Cnpj = ${toSqlValue(cnpjClean)}`;
  try {
    const rows = await prisma.$queryRawUnsafe<Array<{ codEmpresa: number }>>(sql);
    if (rows && rows[0] && rows[0].codEmpresa) {
      return Number(rows[0].codEmpresa);
    }
  } catch (error: any) {
    logger.warn(
      { error: error.message, cnpj: cnpjClean },
      'Erro ao buscar código de empresa na Senior, usando padrão 300',
    );
  }
  return 300;
};

/**
 * Retorna CdTpDoctoFiscal baseado no cdEmpresa consultando a tabela EmpresaSenior
 * REGRA:
 * - Empresas BMX (300, 302, 303, 304, 305, 306, 307) -> cdTpDoctoFiscal = 300
 * - Empresas BRASILMAXI (100, 105, 103, 108) -> cdTpDoctoFiscal = 100
 * - Outros -> usar o próprio cdEmpresa
 */
export const obterCdTpDoctoFiscalPorEmpresa = async (
  prisma: PrismaExecutor,
  cdEmpresa: number,
): Promise<number> => {
  logger.info({ cdEmpresa }, 'Calculando cdTpDoctoFiscal para CT-e');

  if (IS_POSTGRES && !env.ENABLE_SQLSERVER_LEGACY) {
    // Em modo PostgreSQL, evitar leitura no banco legado e aplicar regra direta por cdEmpresa.
    if ([300, 302, 303, 304, 305, 306, 307].includes(cdEmpresa)) {
      return 300;
    }
    if ([100, 105, 103, 108].includes(cdEmpresa)) {
      return 100;
    }
    return cdEmpresa;
  }

  try {
    // Consultar tabela EmpresaSenior no banco AFS_Integrador
    const sql = `
      SELECT TOP 1 CodEmpresaSenior, CodEmpresa, Nome, Cnpj
      FROM [${AFS_INTEGRADOR_DATABASE}]..EmpresaSenior
      WHERE CodEmpresa = ${cdEmpresa}
    `;

    logger.debug({ sql, cdEmpresa }, 'Consultando tabela EmpresaSenior no banco AFS_Integrador');

    const rows = await prisma.$queryRawUnsafe<
      Array<{
        CodEmpresaSenior: number;
        CodEmpresa: number;
        Nome: string;
        Cnpj: string;
      }>
    >(sql);

    if (rows && rows.length > 0 && rows[0]) {
      const empresa = rows[0];
      logger.info(
        {
          cdEmpresa,
          codEmpresaSenior: empresa.CodEmpresaSenior,
          nome: empresa.Nome,
          cnpj: empresa.Cnpj,
        },
        'Empresa encontrada na tabela EmpresaSenior',
      );

      // BMX: CodEmpresaSenior 1-6, 307 -> cdTpDoctoFiscal = 300
      if (
        (empresa.CodEmpresaSenior >= 1 && empresa.CodEmpresaSenior <= 6) ||
        empresa.CodEmpresaSenior === 307
      ) {
        logger.info(
          { cdEmpresa, codEmpresaSenior: empresa.CodEmpresaSenior, nome: empresa.Nome },
          'Empresa BMX detectada, usando cdTpDoctoFiscal = 300',
        );
        return 300;
      }

      // BRASILMAXI: CodEmpresaSenior 7-10 -> cdTpDoctoFiscal = 100
      if (empresa.CodEmpresaSenior >= 7 && empresa.CodEmpresaSenior <= 10) {
        logger.info(
          { cdEmpresa, codEmpresaSenior: empresa.CodEmpresaSenior, nome: empresa.Nome },
          'Empresa BRASILMAXI detectada, usando cdTpDoctoFiscal = 100',
        );
        return 100;
      }

      // Outros: usar o próprio cdEmpresa
      logger.info(
        { cdEmpresa, codEmpresaSenior: empresa.CodEmpresaSenior, nome: empresa.Nome },
        'Empresa não mapeada, usando cdTpDoctoFiscal = cdEmpresa',
      );
      return cdEmpresa;
    }

    logger.warn(
      { cdEmpresa },
      'Empresa não encontrada na tabela EmpresaSenior, usando regra baseada em cdEmpresa',
    );

    // Se não encontrou na tabela, usar regra baseada no cdEmpresa diretamente
    // BMX: 300, 302, 303, 304, 305, 306, 307
    if ([300, 302, 303, 304, 305, 306, 307].includes(cdEmpresa)) {
      logger.info(
        { cdEmpresa },
        'Empresa BMX detectada (por cdEmpresa), usando cdTpDoctoFiscal = 300',
      );
      return 300;
    }

    // BRASILMAXI: 100, 105, 103, 108
    if ([100, 105, 103, 108].includes(cdEmpresa)) {
      logger.info(
        { cdEmpresa },
        'Empresa BRASILMAXI detectada (por cdEmpresa), usando cdTpDoctoFiscal = 100',
      );
      return 100;
    }

    // Outros: usar o próprio cdEmpresa
    logger.info({ cdEmpresa }, 'Empresa não mapeada, usando cdTpDoctoFiscal = cdEmpresa');
    return cdEmpresa;
  } catch (error: any) {
    logger.error(
      { error: error.message, cdEmpresa, stack: error.stack },
      'Erro ao consultar EmpresaSenior, usando regra baseada em cdEmpresa',
    );

    // Fallback: usar regra baseada no cdEmpresa diretamente
    if ([300, 302, 303, 304, 305, 306, 307].includes(cdEmpresa)) {
      logger.info({ cdEmpresa }, 'Fallback: Empresa BMX detectada, usando cdTpDoctoFiscal = 300');
      return 300;
    }
    if ([100, 105, 103, 108].includes(cdEmpresa)) {
      logger.info(
        { cdEmpresa },
        'Fallback: Empresa BRASILMAXI detectada, usando cdTpDoctoFiscal = 100',
      );
      return 100;
    }
    logger.info({ cdEmpresa }, 'Fallback: Empresa não mapeada, usando cdTpDoctoFiscal = cdEmpresa');
    return cdEmpresa;
  }
};

/**
 * Retorna próximo número de sequência de controle
 * Busca o MAX global entre GTCCONCE (CT-e) e GTCConhe (NFSe) para garantir sequência única
 */
const retornaNrSeqControle = async (
  prisma: PrismaExecutor,
  cdEmpresa: number,
  seniorDatabase: string = SENIOR_DATABASE,
): Promise<number> => {
  try {
    // Usar função utilitária que busca MAX entre ambas as tabelas com locks adequados
    const nrSeqControle = await obterProximoNrSeqControle(prisma, seniorDatabase);
    logger.debug({ cdEmpresa, nrSeqControle, seniorDatabase }, 'NrSeqControle obtido');
    return nrSeqControle;
  } catch (error: any) {
    logger.warn(
      { error: error.message, cdEmpresa, seniorDatabase },
      'Erro ao buscar número de sequência de controle, usando 1 como fallback',
    );
    // Retornar 1 como fallback seguro (não 0, pois 0 pode causar problemas)
    return 1;
  }
};

/**
 * Extrai CNPJ do XML do CT-e
 * Garante que busca apenas dentro do bloco da tag especificada
 */
export const extrairCnpjDoXml = (xml: string, tag: string): string | null => {
  try {
    // Primeiro, capturar o bloco completo da tag para garantir que busca apenas dentro dele
    const tagBlockMatch = xml.match(new RegExp(`<${tag}[^>]*>([\\s\\S]*?)</${tag}>`, 'i'));
    if (tagBlockMatch && tagBlockMatch[1]) {
      const tagContent = tagBlockMatch[1];

      // Buscar CNPJ dentro do conteúdo da tag
      const cnpjMatch = tagContent.match(/<CNPJ>(.*?)<\/CNPJ>/i);
      if (cnpjMatch && cnpjMatch[1]) {
        return cnpjMatch[1].trim();
      }

      // Buscar CPF dentro do conteúdo da tag
      const cpfMatch = tagContent.match(/<CPF>(.*?)<\/CPF>/i);
      if (cpfMatch && cpfMatch[1]) {
        return cpfMatch[1].trim();
      }
    }
  } catch (error: any) {
    logger.warn({ error: error.message, tag }, 'Erro ao extrair CNPJ/CPF do XML');
  }
  return null;
};

/**
 * Extrai valor do XML do CT-e
 */
const extrairValorDoXml = (xml: string, tag: string): number => {
  try {
    const match = xml.match(new RegExp(`<${tag}[^>]*>(.*?)</${tag}>`, 's'));
    if (match && match[1]) {
      const valor = parseFloat(match[1].trim());
      if (!Number.isNaN(valor)) {
        return valor;
      }
    }
  } catch (error: any) {
    logger.warn({ error: error.message, tag }, 'Erro ao extrair valor do XML');
  }
  return 0;
};

/**
 * Extrai texto do XML do CT-e
 */
export const extrairTextoDoXml = (xml: string, tag: string): string | null => {
  try {
    const match = xml.match(new RegExp(`<${tag}[^>]*>(.*?)</${tag}>`, 's'));
    if (match && match[1]) {
      return match[1].trim();
    }
  } catch (error: any) {
    logger.warn({ error: error.message, tag }, 'Erro ao extrair texto do XML');
  }
  return null;
};

/**
 * Extrai chave de acesso do XML do CT-e
 */
const extrairChaveAcesso = (xml: string): string | null => {
  try {
    // Buscar Id="CTe..." ou chCTe
    const idMatch = xml.match(/Id="CTe(\d{44})"/);
    if (idMatch && idMatch[1]) {
      return idMatch[1];
    }
    const chMatch = xml.match(/<chCTe>(.*?)<\/chCTe>/);
    if (chMatch && chMatch[1]) {
      return chMatch[1].trim();
    }
  } catch (error: any) {
    logger.warn({ error: error.message }, 'Erro ao extrair chave de acesso do XML');
  }
  return null;
};

/**
 * Extrai número do CT-e da chave de acesso
 */
const extrairNumeroCTe = (chaveAcesso: string | null): number => {
  if (!chaveAcesso || chaveAcesso.length < 44) {
    return 0;
  }
  // Últimos 8 dígitos da chave de acesso
  const numeroStr = chaveAcesso.substring(35, 43);
  return parseInt(numeroStr, 10) || 0;
};

/**
 * Obtém CNPJ do tomador do XML
 * Lógica complexa baseada no código C#: verifica toma3, toma4, etc.
 */
const obterCnpjTomador = (xml: string, emitCnpj: string): string => {
  try {
    // Tentar extrair toma3 ou toma4 do XML
    const toma3Match = xml.match(/<toma3>.*?<toma>(.*?)<\/toma>/s);
    if (toma3Match && toma3Match[1]) {
      const toma = toma3Match[1].trim();
      if (toma === '0') {
        // Remetente
        return extrairCnpjDoXml(xml, 'rem') || emitCnpj;
      } else if (toma === '1') {
        // Expedidor
        return extrairCnpjDoXml(xml, 'exped') || emitCnpj;
      } else if (toma === '2') {
        // Recebedor
        return extrairCnpjDoXml(xml, 'receb') || emitCnpj;
      } else if (toma === '3') {
        // Destinatário
        return extrairCnpjDoXml(xml, 'dest') || emitCnpj;
      }
    }

    // Tentar toma4
    const toma4Match = xml.match(/<toma4>.*?<toma>(.*?)<\/toma>/s);
    if (toma4Match && toma4Match[1]) {
      const toma = toma4Match[1].trim();
      if (toma === '4') {
        // Outros - buscar CNPJ dentro do toma4
        const cnpjToma4 = xml.match(/<toma4>.*?<CNPJ>(.*?)<\/CNPJ>/s);
        if (cnpjToma4 && cnpjToma4[1]) {
          return cnpjToma4[1].trim();
        }
        const cpfToma4 = xml.match(/<toma4>.*?<CPF>(.*?)<\/CPF>/s);
        if (cpfToma4 && cpfToma4[1]) {
          return cpfToma4[1].trim();
        }
      } else {
        // 0, 1, 2 ou 3 - buscar nos parceiros
        if (toma === '0') return extrairCnpjDoXml(xml, 'rem') || emitCnpj;
        if (toma === '1') return extrairCnpjDoXml(xml, 'exped') || emitCnpj;
        if (toma === '2') return extrairCnpjDoXml(xml, 'receb') || emitCnpj;
        if (toma === '3') return extrairCnpjDoXml(xml, 'dest') || emitCnpj;
      }
    }

    // Fallback: usar emitente
    return emitCnpj;
  } catch (error: any) {
    logger.warn({ error: error.message }, 'Erro ao obter CNPJ do tomador, usando emitente');
    return emitCnpj;
  }
};

/**
 * Exclui GTCCONCE antes de inserir (idempotência)
 */
const excluirGTCCONCE = async (prisma: PrismaExecutor, cdChaveAcesso: string): Promise<void> => {
  try {
    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_CR_GTCCONCE_EXCLUIR
        @CdChaveAcesso = ${toSqlValue(cdChaveAcesso)};
    `;
    await prisma.$executeRawUnsafe(sql);
  } catch (error: any) {
    logger.warn({ error: error.message, cdChaveAcesso }, 'Erro ao excluir GTCCONCE (não crítico)');
  }
};

/**
 * Insere GTCCONCE
 */
const inserirGTCCONCE = async (
  prisma: PrismaExecutor,
  xml: string,
  cdEmpresa: number,
  nrSeqControle: number,
): Promise<void> => {
  const cdChaveAcesso = extrairChaveAcesso(xml);
  if (!cdChaveAcesso) {
    throw new Error('Chave de acesso não encontrada no XML');
  }

  const cdChaveCTe = extrairNumeroCTe(cdChaveAcesso);
  const nProt = extrairTextoDoXml(xml, 'nProt') || '';
  const cStat = extrairTextoDoXml(xml, 'cStat') || '100';
  const xMotivo = extrairTextoDoXml(xml, 'xMotivo') || '';

  const inSituacaoSefaz = parseInt(cStat, 10) || 100;

  const sql = `
    EXEC dbo.P_INTEGRACAO_SENIOR_CR_GTCCONCE_INCLUIR
      @CdEmpresa = ${cdEmpresa},
      @NrSeqControle = ${nrSeqControle},
      @CdChaveCTe = ${cdChaveCTe},
      @InSituacaoSefaz = ${inSituacaoSefaz},
      @CdChaveAcesso = ${toSqlValue(cdChaveAcesso)},
      @NrProtocoloCTe = ${toSqlValue(nProt)},
      @DsSituacaoSefaz = ${toSqlValue(xMotivo)},
      @InSituacaoOperacao = 2,
      @InConhecimento = 0,
      @DsVersaoLayout = '3.00',
      @InSitAverbAuto = 2,
      @InSitDownloadXMLPDF = 2;
  `;

  try {
    await prisma.$executeRawUnsafe(sql);
    logger.info({ cdEmpresa, nrSeqControle, cdChaveCTe }, 'GTCCONCE inserido com sucesso');
  } catch (error: any) {
    // Tratar erro de chave primária duplicada (2627)
    if (
      error?.code === 'P2010' ||
      error?.message?.includes('2627') ||
      error?.message?.includes('PRIMARY KEY constraint')
    ) {
      // Verificar se o registro realmente existe
      try {
        const checkSql = `
          SELECT TOP 1 NrSeqControle
          FROM [${SENIOR_DATABASE}].dbo.GTCCONCE WITH (NOLOCK)
          WHERE CdEmpresa = ${cdEmpresa} AND NrSeqControle = ${nrSeqControle};
        `;
        const existing = await prisma.$queryRawUnsafe<Array<{ NrSeqControle: number }>>(checkSql);

        if (existing && existing.length > 0) {
          logger.warn(
            { cdEmpresa, nrSeqControle, cdChaveCTe },
            'GTCCONCE já existe (chave duplicada detectada) - continuando processamento',
          );
          return; // Registro já existe, continuar sem erro
        }
      } catch (checkError: any) {
        logger.warn({ error: checkError.message }, 'Erro ao verificar GTCCONCE existente');
      }

      // Se não encontrou o registro, relançar o erro original
      throw error;
    }

    // Para outros erros, relançar
    throw error;
  }
};

/**
 * Exclui GTCCONHE antes de inserir (idempotência)
 * IMPORTANTE: O índice único iCdEmpresaLigada é composto por:
 *   1. CdEmpresaLigada
 *   2. NrDoctoFiscal
 *   3. NrSerie
 *   4. CdTpDoctoFiscal
 *
 * Portanto, além de excluir por CdEmpresa e NrSeqControle (padrão),
 * também exclui por esses campos do índice único para evitar conflitos.
 */
const excluirGTCCONHE = async (
  prisma: PrismaExecutor,
  cdEmpresa: number,
  nrSeqControle: number,
  cdChaveAcesso?: string | null,
  nrDoctoFiscal?: number | null,
  nrSerie?: string | null,
  cdTpDoctoFiscal?: number | null,
): Promise<void> => {
  // Exclusão padrão: seguindo o padrão do código C# original
  // Exclui apenas por CdEmpresa e NrSeqControle via procedure
  try {
    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_CR_GTCCONHE_EXCLUIR
        @CdEmpresa = ${cdEmpresa},
        @NrSeqControle = ${nrSeqControle};
    `;
    await prisma.$executeRawUnsafe(sql);
    logger.debug({ cdEmpresa, nrSeqControle }, 'GTCCONHE excluído via procedure');
  } catch (error: any) {
    // Erro não crítico - registro pode não existir (primeira inserção)
    logger.warn(
      { error: error.message, cdEmpresa, nrSeqControle },
      'Erro ao excluir GTCCONHE via procedure (não crítico - registro pode não existir)',
    );
  }

  // Exclusão adicional por chave de acesso: busca todos os NrSeqControle relacionados
  if (cdChaveAcesso) {
    try {
      const nrSeqControles = await prisma.$queryRawUnsafe<Array<{ NrSeqControle: number }>>(`
        SELECT DISTINCT NrSeqControle 
        FROM [${SENIOR_DATABASE}]..GTCCONCE WITH (NOLOCK)
        WHERE CdChaveAcesso = ${toSqlValue(cdChaveAcesso)}
          AND CdEmpresa = ${cdEmpresa}
      `);

      for (const row of nrSeqControles || []) {
        if (row.NrSeqControle && row.NrSeqControle !== nrSeqControle) {
          try {
            const sqlByNrSeq = `
              EXEC dbo.P_INTEGRACAO_SENIOR_CR_GTCCONHE_EXCLUIR
                @CdEmpresa = ${cdEmpresa},
                @NrSeqControle = ${row.NrSeqControle};
            `;
            await prisma.$executeRawUnsafe(sqlByNrSeq);
            logger.debug(
              { cdEmpresa, nrSeqControle: row.NrSeqControle, cdChaveAcesso },
              'GTCCONHE excluído via procedure (por chave de acesso)',
            );
          } catch (error: any) {
            logger.debug(
              { error: error.message, cdEmpresa, nrSeqControle: row.NrSeqControle, cdChaveAcesso },
              'Erro ao excluir GTCCONHE por NrSeqControle encontrado via chave de acesso (não crítico)',
            );
          }
        }
      }
    } catch (error: any) {
      logger.debug(
        { error: error.message, cdEmpresa, cdChaveAcesso },
        'Erro ao buscar NrSeqControle por chave de acesso para exclusão (não crítico)',
      );
    }
  }

  // Exclusão adicional por índice único: previne conflito no índice iCdEmpresaLigada
  // O índice único é: (CdEmpresaLigada, NrDoctoFiscal, NrSerie, CdTpDoctoFiscal)
  // IMPORTANTE: Buscar TODOS os registros, incluindo o nrSeqControle atual, pois pode haver
  // um registro com o mesmo índice único mas NrSeqControle diferente
  if (nrDoctoFiscal && nrSerie !== undefined && cdTpDoctoFiscal !== undefined) {
    try {
      logger.info(
        {
          cdEmpresa,
          nrSeqControle,
          nrDoctoFiscal,
          nrSerie,
          cdTpDoctoFiscal,
          cdEmpresaLigada: cdEmpresa,
        },
        'Buscando registros conflitantes pelo índice único iCdEmpresaLigada',
      );

      // Buscar TODOS os registros que violam o índice único (mesma combinação de campos)
      // IMPORTANTE: NÃO filtrar por CdTpDoctoFiscal, pois pode existir registro antigo com valor diferente
      // O índice único é (CdEmpresaLigada, NrDoctoFiscal, NrSerie, CdTpDoctoFiscal)
      // Mas se existe um registro com mesmo (CdEmpresaLigada, NrDoctoFiscal, NrSerie) e CdTpDoctoFiscal diferente,
      // isso pode causar conflito dependendo da regra de negócio. Por segurança, vamos excluir TODOS os registros
      // com a mesma combinação dos 3 primeiros campos, independente do CdTpDoctoFiscal
      const registrosConflitantes = await prisma.$queryRawUnsafe<
        Array<{
          NrSeqControle: number;
          CdEmpresa: number;
          NrDoctoFiscal: number;
          NrSerie: string;
          CdTpDoctoFiscal: number;
        }>
      >(`
        SELECT DISTINCT 
          gh.NrSeqControle,
          gh.CdEmpresa,
          gh.NrDoctoFiscal,
          gh.NrSerie,
          gh.CdTpDoctoFiscal
        FROM [${SENIOR_DATABASE}]..GTCCONHE gh WITH (NOLOCK)
        WHERE gh.CdEmpresaLigada = ${cdEmpresa}
          AND gh.NrDoctoFiscal = ${nrDoctoFiscal}
          AND CAST(gh.NrSerie AS VARCHAR) = CAST(${toSqlValue(nrSerie)} AS VARCHAR)
        -- Removido filtro por CdTpDoctoFiscal para encontrar registros antigos com valores diferentes
      `);

      logger.info(
        {
          cdEmpresa,
          nrSeqControle,
          nrDoctoFiscal,
          nrSerie,
          cdTpDoctoFiscal,
          registrosEncontrados: registrosConflitantes?.length || 0,
        },
        'Registros conflitantes encontrados pelo índice único',
      );

      if (registrosConflitantes && registrosConflitantes.length > 0) {
        logger.warn(
          {
            cdEmpresa,
            registrosEncontrados: registrosConflitantes.length,
            registros: registrosConflitantes.map((r) => ({
              NrSeqControle: r.NrSeqControle,
              CdEmpresa: r.CdEmpresa,
              NrDoctoFiscal: r.NrDoctoFiscal,
              NrSerie: r.NrSerie,
              CdTpDoctoFiscal: r.CdTpDoctoFiscal,
            })),
          },
          'Registros conflitantes encontrados - excluindo via procedure',
        );

        for (const row of registrosConflitantes) {
          if (row.NrSeqControle) {
            try {
              const sqlByIndice = `
                EXEC dbo.P_INTEGRACAO_SENIOR_CR_GTCCONHE_EXCLUIR
                  @CdEmpresa = ${row.CdEmpresa || cdEmpresa},
                  @NrSeqControle = ${row.NrSeqControle};
              `;
              await prisma.$executeRawUnsafe(sqlByIndice);
              logger.info(
                {
                  cdEmpresa: row.CdEmpresa || cdEmpresa,
                  nrSeqControle: row.NrSeqControle,
                  nrDoctoFiscal,
                  nrSerie,
                  cdTpDoctoFiscal,
                },
                'GTCCONHE excluído via procedure (por índice único iCdEmpresaLigada)',
              );
            } catch (error: any) {
              logger.warn(
                {
                  error: error.message,
                  cdEmpresa: row.CdEmpresa || cdEmpresa,
                  nrSeqControle: row.NrSeqControle,
                  nrDoctoFiscal,
                  nrSerie,
                  cdTpDoctoFiscal,
                },
                'Erro ao excluir GTCCONHE por índice único via procedure - tentando DELETE direto',
              );

              // Fallback: DELETE direto se a procedure falhar
              try {
                const deleteSql = `
                  DELETE FROM [${SENIOR_DATABASE}]..GTCCONHE
                  WHERE CdEmpresa = ${row.CdEmpresa || cdEmpresa}
                    AND NrSeqControle = ${row.NrSeqControle};
                `;
                await prisma.$executeRawUnsafe(deleteSql);
                logger.info(
                  {
                    cdEmpresa: row.CdEmpresa || cdEmpresa,
                    nrSeqControle: row.NrSeqControle,
                  },
                  'GTCCONHE excluído via DELETE direto (fallback após falha da procedure)',
                );
              } catch (deleteError: any) {
                logger.error(
                  {
                    error: deleteError.message,
                    cdEmpresa: row.CdEmpresa || cdEmpresa,
                    nrSeqControle: row.NrSeqControle,
                  },
                  'Erro ao excluir GTCCONHE via DELETE direto (fallback)',
                );
              }
            }
          }
        }
      } else {
        logger.debug(
          {
            cdEmpresa,
            nrSeqControle,
            nrDoctoFiscal,
            nrSerie,
            cdTpDoctoFiscal,
          },
          'Nenhum registro conflitante encontrado pelo índice único',
        );
      }
    } catch (error: any) {
      logger.error(
        {
          error: error.message,
          stack: error.stack,
          cdEmpresa,
          nrDoctoFiscal,
          nrSerie,
          cdTpDoctoFiscal,
        },
        'Erro ao buscar registros conflitantes pelo índice único',
      );
    }
  } else {
    logger.warn(
      {
        cdEmpresa,
        nrSeqControle,
        nrDoctoFiscal: nrDoctoFiscal ?? 'null',
        nrSerie: nrSerie ?? 'null',
        cdTpDoctoFiscal: cdTpDoctoFiscal ?? 'null',
      },
      'Campos do índice único não estão completos - exclusão por índice único não será executada',
    );
  }
};

/**
 * Insere GTCCONHE (tabela principal)
 * Esta é a função mais complexa, com muitos campos
 */
const inserirGTCCONHE = async (
  prisma: PrismaExecutor,
  xml: string,
  cdEmpresa: number,
  nrSeqControle: number,
  cdInscricao: string,
): Promise<void> => {
  // Extrair dados do XML e normalizar CNPJ/CPF (remover formatação)
  const emitCnpj = padLeft(extrairCnpjDoXml(xml, 'emit'), 14) || '';
  const remCnpj = padLeft(extrairCnpjDoXml(xml, 'rem') || emitCnpj, 14);

  // Extrair destinatário - pode ser CPF ou CNPJ
  // A função extrairCnpjDoXml já garante que busca apenas dentro do bloco <dest>
  const destDoc = extrairCnpjDoXml(xml, 'dest');
  const destCnpj = padLeft(destDoc || emitCnpj, 14);

  const dhEmi = extrairTextoDoXml(xml, 'dhEmi');
  const dtEmissao = dhEmi ? new Date(dhEmi) : new Date();

  const nCT = extrairTextoDoXml(xml, 'nCT') || '0';
  const nrDoctoFiscal = ensureNumber(parseInt(nCT, 10));

  const serie = extrairTextoDoXml(xml, 'serie') || '';

  // CEPs - extrair do contexto correto e normalizar (apenas dígitos, 8 caracteres)
  // IMPORTANTE: Na tabela GTCCONHE, NrCepColeta, NrCepEntrega e NrCepCalcAte são do tipo INT
  // Precisamos converter para número inteiro, não string
  const cepRem = extrairCepRemetente(xml);
  const cepDest = extrairCepDestinatario(xml);
  // Garantir que CEP sempre tenha 8 dígitos com zeros à esquerda
  const cepRemClean = cepRem.replace(/\D/g, '');
  const cepDestClean = cepDest.replace(/\D/g, '');
  const nrCepColetaStr = cepRemClean.padStart(8, '0').substring(0, 8) || '00000000';
  const nrCepEntregaStr = cepDestClean.padStart(8, '0').substring(0, 8) || '00000000';
  const nrCepCalcAteStr = nrCepEntregaStr || nrCepColetaStr || '00000000';
  // Converter para número inteiro (tipo INT na tabela)
  const nrCepColeta = ensureNumber(parseInt(nrCepColetaStr, 10));
  const nrCepEntrega = ensureNumber(parseInt(nrCepEntregaStr, 10));
  const nrCepCalcAte = ensureNumber(parseInt(nrCepCalcAteStr, 10));

  // Carga - extrair do contexto correto (infQ dentro de infCarga)
  const qtPeso = extrairPesoReal(xml);
  const qtPesoCubado = extrairPesoCubado(xml);
  const qtVolume = extrairQuantidadeVolumes(xml);

  // Log dos valores extraídos para debug (especialmente para valores grandes)
  logger.info(
    {
      qtPeso,
      qtPesoCubado,
      qtVolume,
      qtPesoNormalizado: normalizePesoVolume(qtPeso),
      qtPesoCubadoNormalizado: normalizePesoVolume(qtPesoCubado),
      qtVolumeNormalizado: normalizePesoVolume(qtVolume),
    },
    'Valores de peso/volume extraídos do XML e normalizados',
  );

  const vlMercadoria = extrairValorCarga(xml);
  const vlNFCobrada = vlMercadoria;

  // Prestação de serviço
  const vlTotalPrestacao = extrairValorDoXml(xml, 'vRec');
  const vlLiquido = vlTotalPrestacao;

  // ICMS - extrair de ICMS00, ICMS20, ICMS60, etc.
  let vlBaseCalculo = 0;
  let vlICMS = 0;
  let vlAliqICMS = 0;
  let vlBaseCalcComissao = 0;

  // Tentar extrair ICMS00, ICMS20, ICMS60, etc.
  // Buscar cada campo individualmente pois a ordem pode variar no XML
  const icms00Block = xml.match(/<ICMS00>[\s\S]*?<\/ICMS00>/i);
  if (icms00Block) {
    const vBCMatch = icms00Block[0].match(/<vBC>(.*?)<\/vBC>/i);
    const vICMSMatch = icms00Block[0].match(/<vICMS>(.*?)<\/vICMS>/i);
    const pICMSMatch = icms00Block[0].match(/<pICMS>(.*?)<\/pICMS>/i);

    if (vBCMatch && vBCMatch[1]) {
      vlBaseCalculo = parseFloat(vBCMatch[1].trim()) || 0;
      vlBaseCalcComissao = vlBaseCalculo;
    }
    if (vICMSMatch && vICMSMatch[1]) {
      vlICMS = parseFloat(vICMSMatch[1].trim()) || 0;
    }
    if (pICMSMatch && pICMSMatch[1]) {
      vlAliqICMS = parseFloat(pICMSMatch[1].trim()) || 0;
    }
  }

  // Componentes do frete - extrair de <Comp> dentro de <vPrest>
  const componentesFrete = extrairComponentesFrete(xml);
  const vlFretePeso = componentesFrete.vlFretePeso;
  const vlFreteValor = componentesFrete.vlFreteValor; // Ad Valorem
  const vlPedagio = componentesFrete.vlPedagio;
  const vlOutros = componentesFrete.vlOutros;

  // Se o ICMS não veio de ICMS00, pode vir de Comp (alguns casos)
  if (vlICMS === 0 && componentesFrete.vlICMS > 0) {
    vlICMS = componentesFrete.vlICMS;
  }

  const vlDespacho = 0; // Não há componente específico para despacho
  const vlSUFRAMA = 0; // Não há componente específico para SUFRAMA
  const vlGris = 0; // Não há componente específico para GRIS (pode estar em Outros)

  const dsComentario = extrairTextoDoXml(xml, 'xObs');

  // Inscrições estaduais - extrair do contexto correto
  // A procedure converte "ISENTO" para NULL, então tratamos aqui também
  let nrInscEstadualRem = extrairIERemetente(xml);
  let nrInscEstadualDest = extrairIEDestinatario(xml);
  if (nrInscEstadualRem?.toUpperCase() === 'ISENTO') nrInscEstadualRem = null;
  if (nrInscEstadualDest?.toUpperCase() === 'ISENTO') nrInscEstadualDest = null;

  // CFOP e Natureza da Operação
  const cfop = extrairCFOP(xml);
  const naturezaOp = extrairNaturezaOperacao(xml);

  // CdOperacao = CFOP concatenado com pICMS (sem decimais)
  // Exemplo: CFOP 6353 + pICMS 12.00 = 635312
  let cdOperacao = ensureNumber(cfop) || 535312; // Fallback para CFOP ou 535312
  if (cfop > 0 && vlAliqICMS > 0) {
    // Remove decimais do pICMS (12.00 vira 12)
    const pICMSInteiro = Math.round(vlAliqICMS);
    // Concatena CFOP + pICMS: "6353" + "12" = "635312"
    const cdOperacaoStr = String(ensureNumber(cfop)) + String(ensureNumber(pICMSInteiro));
    cdOperacao = ensureNumber(parseInt(cdOperacaoStr, 10)) || ensureNumber(cfop) || 535312;
  }

  // Tipo de CT-e
  const tpCTe = extrairTextoDoXml(xml, 'tpCTe') || '0';
  const inTpCTE = ensureNumber(parseInt(tpCTe, 10));

  // Calcular CdTpDoctoFiscal baseado no cdEmpresa consultando a tabela EmpresaSenior
  // REGRA:
  // - Empresas BMX (300, 302, 303, 304, 305, 306, 307) -> cdTpDoctoFiscal = 300
  // - Empresas BRASILMAXI (100, 105, 103, 108) -> cdTpDoctoFiscal = 100
  // - Outros -> usar o próprio cdEmpresa
  const cdTpDoctoFiscal = await obterCdTpDoctoFiscalPorEmpresa(prisma, cdEmpresa);
  logger.info(
    { cdEmpresa, cdTpDoctoFiscal, nrSeqControle },
    'cdTpDoctoFiscal calculado para CT-e - será usado na stored procedure',
  );

  // Garantir que todos os valores numéricos sejam inteiros válidos
  const cdEmpresaInt = ensureNumber(cdEmpresa);
  const nrSeqControleInt = ensureNumber(nrSeqControle);
  const cdTpDoctoFiscalInt = ensureNumber(cdTpDoctoFiscal);
  const cdOperacaoInt = ensureNumber(cdOperacao);
  const inTpCTEInt = ensureNumber(inTpCTE);
  const nrDoctoFiscalInt = ensureNumber(nrDoctoFiscal);

  // Validar todos os valores monetários e de peso/volume
  const qtPesoNormalizado = normalizePesoVolume(qtPeso);
  const qtVolumeNormalizado = normalizePesoVolume(qtVolume);

  logger.info(
    {
      cdEmpresa: cdEmpresaInt,
      nrSeqControle: nrSeqControleInt,
      cdTpDoctoFiscal: cdTpDoctoFiscalInt,
      cdOperacao: cdOperacaoInt,
      inTpCTE: inTpCTEInt,
      nrDoctoFiscal: nrDoctoFiscalInt,
      qtPeso: qtPeso,
      qtPesoNormalizado: qtPesoNormalizado,
      qtVolume: qtVolume,
      qtVolumeNormalizado: qtVolumeNormalizado,
      vlMercadoria: vlMercadoria,
      vlTotalPrestacao: vlTotalPrestacao,
    },
    'Valores numéricos validados antes de chamar stored procedure',
  );

  // Validar que todos os valores monetários são números válidos (antes de construir o SQL)
  const valoresMonetarios = {
    vlMercadoria: normalizeValorMonetario(vlMercadoria),
    vlNFCobrada: normalizeValorMonetario(vlNFCobrada),
    vlTotalPrestacao: normalizeValorMonetario(vlTotalPrestacao),
    vlBaseCalculo: normalizeValorMonetario(vlBaseCalculo),
    vlICMS: normalizeValorMonetario(vlICMS),
    vlLiquido: normalizeValorMonetario(vlLiquido),
    vlFretePeso: normalizeValorMonetario(vlFretePeso),
    vlFreteValor: normalizeValorMonetario(vlFreteValor),
    vlPedagio: normalizeValorMonetario(vlPedagio),
    vlOutros: normalizeValorMonetario(vlOutros),
    vlDespacho: normalizeValorMonetario(vlDespacho),
    vlGris: normalizeValorMonetario(vlGris),
    vlAliqICMS: normalizeValorMonetario(vlAliqICMS),
    vlBaseCalcComissao: normalizeValorMonetario(vlBaseCalcComissao),
  };

  const sql = `
    EXEC dbo.P_INTEGRACAO_SENIOR_CR_GTCCONHE_INCLUIR
      @CdEmpresa = ${cdEmpresaInt},
      @NrSeqControle = ${nrSeqControleInt},
      @CdInscricao = ${toSqlValue(cdInscricao)},
      @DtEmissao = ${toSqlValue(dtEmissao.toISOString().split('T')[0])},
      @InTipoFatura = 0,
      @InTipoFrete = 1,
      @CdRemetente = ${toSqlValue(remCnpj)},
      @CdDestinatario = ${toSqlValue(destCnpj)},
      @NrCepColeta = ${toSqlNumeric(nrCepColeta)},
      @NrCepEntrega = ${toSqlNumeric(nrCepEntrega)},
      @NrCepCalcAte = ${toSqlNumeric(nrCepCalcAte)},
      @CdNatureza = 0,
      @CdEspecie = 0,
      @CdTabelaPreco = 0,
      @CdTarifa = 0,
      @QtPeso = ${toSqlNumeric(qtPesoNormalizado)},
      @QtPesoCubado = NULL,
      @QtVolume = ${toSqlNumeric(qtVolumeNormalizado)},
      @VlMercadoria = ${toSqlNumeric(valoresMonetarios.vlMercadoria)},
      @VlNFCobrada = ${toSqlNumeric(valoresMonetarios.vlNFCobrada)},
      @CdTransporte = 1,
      @CdOperacao = ${cdOperacaoInt},
      @InICMS = 0,
      @VlTotalPrestacao = ${toSqlNumeric(valoresMonetarios.vlTotalPrestacao)},
      @VlBaseCalculo = ${toSqlNumeric(valoresMonetarios.vlBaseCalculo)},
      @VlICMS = ${toSqlNumeric(valoresMonetarios.vlICMS)},
      @VlLiquido = ${toSqlNumeric(valoresMonetarios.vlLiquido)},
      @InConhecimento = 0,
      @InFatura = 0,
      @VlSeguro = 0,
      @VlFretePeso = ${toSqlNumeric(valoresMonetarios.vlFretePeso)},
      @VlFreteValor = ${toSqlNumeric(valoresMonetarios.vlFreteValor)},
      @VlADEME = 0,
      @VlITR = 0,
      @VlSECCAT = 0,
      @VlDespacho = ${toSqlNumeric(valoresMonetarios.vlDespacho)},
      @VlPedagio = ${toSqlNumeric(valoresMonetarios.vlPedagio)},
      @VlSUFRAMA = ${toSqlNumeric(normalizeValorMonetario(vlSUFRAMA))},
      @VlBalsa = 0,
      @VlOutros = ${toSqlNumeric(valoresMonetarios.vlOutros)},
      @DsComentario = ${toSqlValue(dsComentario)},
      @NrSerie = ${toSqlValue(serie)},
      @FgICMSIncluso = 0,
      @InTipoFreteRedespacho = 0,
      @VlGris = ${toSqlNumeric(valoresMonetarios.vlGris)},
      @VlAliqICMS = ${toSqlNumeric(valoresMonetarios.vlAliqICMS)},
      @CdCotacao = 0,
      @QtKmPercurso = 0,
      @dsusuario = ${toSqlValue(extrairTextoDoXml(xml, 'xEmi'))},
      @dtdigitacao = ${toSqlValue(dtEmissao.toISOString().split('T')[0])},
      @QtEntregas = 1,
      @InOrigemConhec = 0,
      @QtPesoTotalLote = 0,
      @QtKmTotalLote = 0,
      @InICMSDestacado = 1,
      @CdEmpresaTabela = 100,
      @QtPares = 0,
      @InTipoEmissao = 0,
      @CdPercursoComercial = 0,
      @InImpressao = 1,
      @CdEmpresaDestino = 0,
      @DtImpressao = ${toSqlValue(dtEmissao.toISOString().split('T')[0])},
      @InSeguroFrete = 1,
      @InISS = 0,
      @CdEmpresaColeta = ${cdEmpresaInt},
      @CdEmpresaLigada = ${cdEmpresaInt},
      @VlBaseCalcComissao = ${toSqlNumeric(valoresMonetarios.vlBaseCalcComissao)},
      @InCalcMultiplasNat = 0,
      @CdGerenciadorRisco = 0,
      @VlTDE = 0,
      @VlMercadoriasTotalLote = 0,
      @VlICMSCredPres = 0,
      @VlDesc = 0,
      @DsLocalEntrega = ${toSqlValue('')},
      @VlCAP = 0,
      @VlCAD = 0,
      @VlTRT = 0,
      @VlTEP = 0,
      @NrDoctoFiscal = ${nrDoctoFiscalInt},
      @CdTpDoctoFiscal = ${cdTpDoctoFiscalInt},
      @InFOBDirigido = 0,
      @InTpCTE = ${inTpCTEInt},
      @NrInscEstadualRem = ${toSqlValue(nrInscEstadualRem)},
      @NrInscEstadualDest = ${toSqlValue(nrInscEstadualDest)};
  `;

  try {
    // Log detalhado de todos os valores numéricos antes de executar
    logger.debug(
      {
        cdEmpresa: cdEmpresaInt,
        nrSeqControle: nrSeqControleInt,
        cdTpDoctoFiscal: cdTpDoctoFiscalInt,
        cdOperacao: cdOperacaoInt,
        inTpCTE: inTpCTEInt,
        nrDoctoFiscal: nrDoctoFiscalInt,
        qtPeso: qtPeso,
        qtPesoNormalizado: qtPesoNormalizado,
        qtVolume: qtVolume,
        qtVolumeNormalizado: qtVolumeNormalizado,
        nrCepColeta: nrCepColeta,
        nrCepEntrega: nrCepEntrega,
        nrCepCalcAte: nrCepCalcAte,
        vlMercadoria: vlMercadoria,
        vlTotalPrestacao: vlTotalPrestacao,
        vlBaseCalculo: vlBaseCalculo,
        vlICMS: vlICMS,
        vlLiquido: vlLiquido,
        vlFretePeso: vlFretePeso,
        vlFreteValor: vlFreteValor,
        vlPedagio: vlPedagio,
        vlOutros: vlOutros,
        vlDespacho: vlDespacho,
        vlGris: vlGris,
        vlAliqICMS: vlAliqICMS,
        vlBaseCalcComissao: vlBaseCalcComissao,
        valoresMonetarios,
      },
      'Valores finais antes de executar stored procedure GTCCONHE',
    );

    logger.debug({ valoresMonetarios }, 'Valores monetários normalizados para stored procedure');

    // Log do SQL completo para debug (apenas em caso de erro)
    // Não logar sempre para não poluir os logs
    const sqlPreview = sql.substring(0, 500) + '...';
    logger.debug({ sqlPreview }, 'SQL preview (primeiros 500 caracteres)');

    await prisma.$executeRawUnsafe(sql);
    logger.info({ cdEmpresa, nrSeqControle }, 'GTCCONHE inserido com sucesso');
  } catch (error: any) {
    // Tratar erro de chave primária duplicada (2627) - registro já existe
    if (
      error?.code === 'P2010' ||
      error?.message?.includes('2627') ||
      error?.message?.includes('PRIMARY KEY constraint')
    ) {
      // Verificar se o registro realmente existe
      try {
        const checkSql = `
          SELECT TOP 1 NrSeqControle
          FROM [${SENIOR_DATABASE}].dbo.GTCCONHE WITH (NOLOCK)
          WHERE CdEmpresa = ${cdEmpresa} AND NrSeqControle = ${nrSeqControle};
        `;
        const existing = await prisma.$queryRawUnsafe<Array<{ NrSeqControle: number }>>(checkSql);

        if (existing && existing.length > 0) {
          logger.warn(
            { cdEmpresa, nrSeqControle },
            'GTCCONHE já existe (chave duplicada detectada) - continuando processamento',
          );
          return; // Registro já existe, continuar sem erro
        }
      } catch (checkError: any) {
        logger.warn({ error: checkError.message }, 'Erro ao verificar GTCCONHE existente');
      }

      // Se não encontrou o registro, relançar o erro original
      // (pode ser outro tipo de erro de constraint)
    }

    // Verificar se o erro é relacionado a peso/volume ou conversão numérica
    const erroPeso =
      error?.message?.includes('peso') ||
      error?.message?.includes('Peso') ||
      error?.message?.includes('peso cubado') ||
      error?.message?.includes('Peso cubado') ||
      error?.message?.includes('QtPeso') ||
      error?.message?.includes('QtPesoCubado') ||
      error?.message?.includes('QtVolume') ||
      error?.message?.includes('8114') || // Erro de conversão numérica
      (error?.code === 'P2010' && error?.meta?.code === '8114');

    // Log completo do SQL em caso de erro para facilitar debug
    logger.error(
      {
        error: error.message,
        errorCode: error?.code,
        errorMeta: error?.meta,
        stack: error.stack,
        cdEmpresa,
        nrSeqControle,
        cdInscricao,
        qtPeso,
        qtPesoCubado,
        qtVolume,
        qtPesoNormalizado,
        qtVolumeNormalizado,
        nrCepColeta,
        nrCepEntrega,
        nrCepCalcAte,
        cdEmpresaInt,
        nrSeqControleInt,
        cdTpDoctoFiscalInt,
        cdOperacaoInt,
        inTpCTEInt,
        nrDoctoFiscalInt,
        valoresMonetarios,
        sql: sql.substring(0, 2000), // Primeiros 2000 caracteres do SQL
        erroPeso,
      },
      erroPeso
        ? 'Erro relacionado a peso/volume/conversão numérica ao inserir GTCCONHE'
        : 'Erro ao inserir GTCCONHE',
    );

    // Se for erro de conversão numérica (8114), adicionar informações específicas
    if (erroPeso || (error?.code === 'P2010' && error?.meta?.code === '8114')) {
      const erroDetalhado = `Erro de conversão numérica (8114) ao inserir GTCCONHE: ${error.message}. Verifique os valores numéricos: Peso real: ${qtPeso}, Peso normalizado: ${qtPesoNormalizado}, Volume: ${qtVolume}, Volume normalizado: ${qtVolumeNormalizado}, CEPs: Coleta=${nrCepColeta}, Entrega=${nrCepEntrega}, CalcAte=${nrCepCalcAte}. SQL: ${sql.substring(0, 1000)}`;
      throw new Error(erroDetalhado);
    }

    throw error;
  }
};

/**
 * Exclui GTCCONSF antes de inserir (idempotência)
 */
const excluirGTCCONSF = async (
  prisma: PrismaExecutor,
  cdEmpresa: number,
  nrSeqControle: number,
): Promise<void> => {
  try {
    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_CR_GTCCONSF_EXCLUIR
        @CdEmpresa = ${cdEmpresa},
        @NrSeqControle = ${nrSeqControle};
    `;
    await prisma.$executeRawUnsafe(sql);
  } catch (error: any) {
    logger.warn(
      { error: error.message, cdEmpresa, nrSeqControle },
      'Erro ao excluir GTCCONSF (não crítico)',
    );
  }
};

/**
 * Insere GTCCONSF
 */
const inserirGTCCONSF = async (
  prisma: PrismaExecutor,
  xml: string,
  cdEmpresa: number,
  nrSeqControle: number,
): Promise<void> => {
  const dhEmi = extrairTextoDoXml(xml, 'dhEmi');
  const dtGeracao = dhEmi ? new Date(dhEmi) : new Date();
  const dtIntegracao = dtGeracao;

  const cStat = extrairTextoDoXml(xml, 'cStat') || '100';
  const inSituacaoSefaz = parseInt(cStat, 10) || 100;
  const dsSitSEFAZ = extrairTextoDoXml(xml, 'xMotivo') || '';

  const sql = `
    EXEC dbo.P_INTEGRACAO_SENIOR_CR_GTCCONSF_INCLUIR
      @CdEmpresa = ${cdEmpresa},
      @NrSeqControle = ${nrSeqControle},
      @CdSequencia = 1,
      @InRemessaRetorno = 0,
      @DtGeracao = ${toSqlValue(dtGeracao.toISOString().split('T')[0])},
      @DsRecibo = ${toSqlValue(xml)},
      @InSituacaoSefaz = ${inSituacaoSefaz},
      @DtIntegracao = ${toSqlValue(dtIntegracao.toISOString().split('T')[0])},
      @InTpArquivo = 1,
      @DsSitSEFAZ = ${toSqlValue(dsSitSEFAZ)};
  `;

  try {
    await prisma.$executeRawUnsafe(sql);
    logger.info({ cdEmpresa, nrSeqControle }, 'GTCCONSF inserido com sucesso');
  } catch (error: any) {
    // Tratar erro de chave primária duplicada (2627)
    // Chave primária: (CdEmpresa, NrSeqControle, CdSequencia)
    if (
      error?.code === 'P2010' ||
      error?.message?.includes('2627') ||
      error?.message?.includes('PRIMARY KEY constraint')
    ) {
      // Verificar se o registro realmente existe
      try {
        const checkSql = `
          SELECT TOP 1 NrSeqControle
          FROM [${SENIOR_DATABASE}].dbo.GTCCONSF WITH (NOLOCK)
          WHERE CdEmpresa = ${cdEmpresa} AND NrSeqControle = ${nrSeqControle};
        `;
        const existing = await prisma.$queryRawUnsafe<Array<{ NrSeqControle: number }>>(checkSql);

        if (existing && existing.length > 0) {
          logger.warn(
            { cdEmpresa, nrSeqControle },
            'GTCCONSF já existe (chave duplicada detectada) - continuando processamento',
          );
          return; // Registro já existe, continuar sem erro
        }
      } catch (checkError: any) {
        logger.warn(
          { error: checkError.message, cdEmpresa, nrSeqControle },
          'Erro ao verificar GTCCONSF existente (não crítico)',
        );
      }

      // Se não encontrou, pode ser que a exclusão falhou - tentar novamente
      logger.warn(
        { cdEmpresa, nrSeqControle, error: error.message },
        'GTCCONSF chave duplicada - tentando excluir e reinserir',
      );

      // Tentar excluir novamente
      try {
        await excluirGTCCONSF(prisma, cdEmpresa, nrSeqControle);
        // Tentar inserir novamente
        await prisma.$executeRawUnsafe(sql);
        logger.info({ cdEmpresa, nrSeqControle }, 'GTCCONSF inserido com sucesso após retry');
        return;
      } catch (retryError: any) {
        // Se ainda falhar, verificar se existe
        const checkSql = `
          SELECT TOP 1 NrSeqControle
          FROM [${SENIOR_DATABASE}].dbo.GTCCONSF WITH (NOLOCK)
          WHERE CdEmpresa = ${cdEmpresa} AND NrSeqControle = ${nrSeqControle};
        `;
        const existing = await prisma.$queryRawUnsafe<Array<{ NrSeqControle: number }>>(checkSql);
        if (existing && existing.length > 0) {
          logger.warn(
            { cdEmpresa, nrSeqControle },
            'GTCCONSF já existe após retry - continuando processamento',
          );
          return; // Registro já existe, continuar sem erro
        }
        // Se não existe e ainda falhou, relançar o erro
        throw retryError;
      }
    }

    logger.error(
      {
        error: error.message,
        stack: error.stack,
        cdEmpresa,
        nrSeqControle,
      },
      'Erro ao inserir GTCCONSF',
    );
    throw error;
  }
};

/**
 * Marca CT-e como processado
 */
const alterarXMLProcessado = async (prisma: PrismaExecutor, external_id: number): Promise<void> => {
  if (shouldBypassCteSeniorLegacy()) {
    await prisma.cte.updateMany({
      where: { external_id },
      data: { processed: true },
    });
    logger.info(
      { external_id, isPostgres: IS_POSTGRES, enableSqlServerLegacy: env.ENABLE_SQLSERVER_LEGACY },
      'CT-e marcado como processado no banco local (modo PostgreSQL sem legado)',
    );
    return;
  }

  const sql = `
    EXEC dbo.P_ALTERAR_XML_PROCESSADO_CR_SENIOR
      @External_id = ${external_id};
  `;
  await prisma.$executeRawUnsafe(sql);
  logger.info({ external_id }, 'CT-e marcado como processado');
};

/**
 * Cancela CT-e nas tabelas GTCCONHE e GTCCONCE
 * GTCCONHE: Atualiza InConhecimento = 1 e DtCancelamento
 * GTCCONCE: Atualiza InSituacaoSefaz = 135, DsSituacaoSefaz = 'Cancelamento' e InConhecimento = 1
 */
export async function cancelarCte(
  prisma: PrismaExecutor,
  xml: string,
  cdEmpresa: number,
  nrSeqControle?: number,
): Promise<{ success: boolean; error?: string; nrSeqControle?: number }> {
  if (shouldBypassCteSeniorLegacy()) {
    logger.info(
      {
        cdEmpresa,
        nrSeqControle,
        isPostgres: IS_POSTGRES,
        enableSqlServerLegacy: env.ENABLE_SQLSERVER_LEGACY,
      },
      'Cancelamento de CT-e no Senior ignorado em modo PostgreSQL sem legado',
    );
    return nrSeqControle !== undefined ? { success: true, nrSeqControle } : { success: true };
  }

  try {
    logger.info(
      { cdEmpresa, nrSeqControle },
      'Iniciando cancelamento de CT-e nas tabelas GTCCONHE e GTCCONCE',
    );

    // Se nrSeqControle não foi fornecido, buscar pela chave de acesso
    if (!nrSeqControle) {
      const cdChaveAcesso = extrairChaveAcesso(xml);
      if (cdChaveAcesso) {
        try {
          // Primeiro tentar buscar em GTCCONCE pela chave de acesso
          const rowsGTCCONCE = await prisma.$queryRawUnsafe<Array<{ NrSeqControle: number }>>(`
            SELECT TOP 1 NrSeqControle
            FROM [${SENIOR_DATABASE}]..GTCCONCE WITH (NOLOCK)
            WHERE CdChaveAcesso = ${toSqlValue(cdChaveAcesso)}
              AND CdEmpresa = ${cdEmpresa}
          `);
          if (rowsGTCCONCE && rowsGTCCONCE.length > 0 && rowsGTCCONCE[0]?.NrSeqControle) {
            nrSeqControle = rowsGTCCONCE[0].NrSeqControle;
          } else {
            // Se não encontrou em GTCCONCE, tentar buscar em GTCCONHE pelo índice único
            const nrDoctoFiscal = extrairNumeroCTe(cdChaveAcesso);
            const serie =
              extrairTextoDoXml(xml, 'serie') || extrairTextoDoXml(xml, 'NrSerie') || '';
            const cdTpDoctoFiscalOriginal = cdEmpresa;
            const cdTpDoctoFiscalTransformado = parseInt('1' + String(cdEmpresa), 10);

            if (nrDoctoFiscal > 0 && serie) {
              const rowsGTCCONHE = await prisma.$queryRawUnsafe<Array<{ NrSeqControle: number }>>(`
                SELECT TOP 1 NrSeqControle
                FROM [${SENIOR_DATABASE}]..GTCCONHE WITH (NOLOCK)
                WHERE CdEmpresaLigada = ${cdEmpresa}
                  AND NrDoctoFiscal = ${nrDoctoFiscal}
                  AND CAST(NrSerie AS VARCHAR) = CAST(${toSqlValue(serie)} AS VARCHAR)
                  AND (CdTpDoctoFiscal = ${cdTpDoctoFiscalOriginal} OR CdTpDoctoFiscal = ${cdTpDoctoFiscalTransformado})
              `);
              if (rowsGTCCONHE && rowsGTCCONHE.length > 0 && rowsGTCCONHE[0]?.NrSeqControle) {
                nrSeqControle = rowsGTCCONHE[0].NrSeqControle;
              }
            }
          }
        } catch (error: any) {
          logger.warn({ error: error.message }, 'Erro ao buscar NrSeqControle por chave de acesso');
        }
      }
    }

    if (!nrSeqControle) {
      const cdChaveAcesso = extrairChaveAcesso(xml);
      throw new Error(
        `Impossível cancelar CT-e: registro não encontrado no banco Senior. ` +
          `Chave de acesso: ${cdChaveAcesso || 'não identificada'}. ` +
          `Empresa: ${cdEmpresa}. ` +
          `Verifique se o CT-e foi inserido corretamente antes de tentar cancelar.`,
      );
    }

    // Extrair data/hora do evento de cancelamento do XML
    const dhRegEvento = extrairTextoDoXml(xml, 'dhRegEvento');
    const dtCancelamento = dhRegEvento ? new Date(dhRegEvento) : new Date();

    // 1. Atualizar GTCCONHE: InConhecimento = 1 e DtCancelamento
    const updateGTCCONHESql = `
      UPDATE [${SENIOR_DATABASE}]..GTCCONHE
      SET 
        InConhecimento = 1,
        DtCancelamento = ${toSqlValue(dtCancelamento.toISOString().split('T')[0])}
      WHERE CdEmpresa = ${cdEmpresa}
        AND NrSeqControle = ${nrSeqControle};
    `;

    await prisma.$executeRawUnsafe(updateGTCCONHESql);
    logger.info(
      { cdEmpresa, nrSeqControle, dtCancelamento },
      'CT-e cancelado com sucesso na tabela GTCCONHE',
    );

    // 2. Atualizar GTCCONCE: InSituacaoSefaz = 135, DsSituacaoSefaz = 'Cancelamento' e InConhecimento = 1
    const updateGTCCONCESql = `
      UPDATE [${SENIOR_DATABASE}]..GTCCONCE
      SET 
        InSituacaoSefaz = 135,
        DsSituacaoSefaz = ${toSqlValue('Cancelamento')},
        InConhecimento = 1
      WHERE CdEmpresa = ${cdEmpresa}
        AND NrSeqControle = ${nrSeqControle};
    `;

    try {
      await prisma.$executeRawUnsafe(updateGTCCONCESql);
      logger.info({ cdEmpresa, nrSeqControle }, 'CT-e cancelado com sucesso na tabela GTCCONCE');
    } catch (gtcconceError: any) {
      // Se GTCCONCE não existir ou não puder ser atualizado, logar como warning mas não falhar
      // (pode ser que o CT-e não tenha sido inserido em GTCCONCE ainda)
      logger.warn(
        {
          error: gtcconceError.message,
          cdEmpresa,
          nrSeqControle,
        },
        'Aviso: Não foi possível atualizar GTCCONCE (pode não existir para este CT-e)',
      );
    }

    return {
      success: true,
      nrSeqControle,
    };
  } catch (error: any) {
    logger.error(
      {
        error: error.message,
        stack: error.stack,
        cdEmpresa,
        nrSeqControle,
      },
      'Erro ao cancelar CT-e nas tabelas GTCCONHE/GTCCONCE',
    );

    const retorno: { success: false; error: string; nrSeqControle?: number } = {
      success: false,
      error: error.message || 'Erro desconhecido',
    };
    if (nrSeqControle) {
      retorno.nrSeqControle = nrSeqControle;
    }
    return retorno;
  }
}

/**
 * Verifica se CT-e já existe nas tabelas Senior
 * Retorna informações sobre existência em cada tabela
 */
export interface VerificacaoCteExistente {
  existeGTCCONCE: boolean;
  existeGTCCONHE: boolean;
  existeGTCCONSF: boolean;
  nrSeqControleGTCCONCE?: number;
  nrSeqControleGTCCONHE?: number;
  nrSeqControleGTCCONSF?: number;
  nCT?: string;
}

export const verificarCteExistente = async (
  prisma: PrismaExecutor,
  xml: string,
  cdEmpresa: number,
): Promise<VerificacaoCteExistente> => {
  if (shouldBypassCteSeniorLegacy()) {
    const nCT = extrairTextoDoXml(xml, 'nCT') || undefined;
    return {
      existeGTCCONCE: false,
      existeGTCCONHE: false,
      existeGTCCONSF: false,
      ...(nCT ? { nCT } : {}),
    };
  }

  const resultado: VerificacaoCteExistente = {
    existeGTCCONCE: false,
    existeGTCCONHE: false,
    existeGTCCONSF: false,
  };

  try {
    // Extrair dados do XML
    const cdChaveAcesso = extrairChaveAcesso(xml);
    const nCT = extrairTextoDoXml(xml, 'nCT') || '0';
    const serie = extrairTextoDoXml(xml, 'serie') || '';
    const nrDoctoFiscal = parseInt(nCT, 10) || 0;
    const cdTpDoctoFiscalTransformado = parseInt('1' + String(cdEmpresa), 10);

    resultado.nCT = nCT;

    // Verificar GTCCONCE por chave de acesso
    if (cdChaveAcesso) {
      try {
        const gtcconceRows = await prisma.$queryRawUnsafe<Array<{ NrSeqControle: number }>>(`
          SELECT TOP 1 NrSeqControle
          FROM [${SENIOR_DATABASE}]..GTCCONCE WITH (NOLOCK)
          WHERE CdChaveAcesso = ${toSqlValue(cdChaveAcesso)}
            AND CdEmpresa = ${cdEmpresa}
        `);
        if (gtcconceRows && gtcconceRows.length > 0 && gtcconceRows[0]?.NrSeqControle) {
          resultado.existeGTCCONCE = true;
          resultado.nrSeqControleGTCCONCE = gtcconceRows[0].NrSeqControle;
        }
      } catch (error: any) {
        logger.debug({ error: error.message }, 'Erro ao verificar GTCCONCE (não crítico)');
      }
    }

    // Verificar GTCCONHE por índice único
    if (nrDoctoFiscal > 0 && serie) {
      try {
        const gtcconheRows = await prisma.$queryRawUnsafe<Array<{ NrSeqControle: number }>>(`
          SELECT TOP 1 NrSeqControle
          FROM [${SENIOR_DATABASE}]..GTCCONHE WITH (NOLOCK)
          WHERE CdEmpresaLigada = ${cdEmpresa}
            AND NrDoctoFiscal = ${nrDoctoFiscal}
            AND CAST(NrSerie AS VARCHAR) = CAST(${toSqlValue(serie)} AS VARCHAR)
            AND (CdTpDoctoFiscal = ${cdEmpresa} OR CdTpDoctoFiscal = ${cdTpDoctoFiscalTransformado})
        `);
        if (gtcconheRows && gtcconheRows.length > 0 && gtcconheRows[0]?.NrSeqControle) {
          resultado.existeGTCCONHE = true;
          resultado.nrSeqControleGTCCONHE = gtcconheRows[0].NrSeqControle;
        }
      } catch (error: any) {
        logger.debug({ error: error.message }, 'Erro ao verificar GTCCONHE (não crítico)');
      }
    }

    // Verificar GTCCONSF usando NrSeqControle do GTCCONCE ou GTCCONHE
    const nrSeqControleParaVerificar =
      resultado.nrSeqControleGTCCONCE || resultado.nrSeqControleGTCCONHE;
    if (nrSeqControleParaVerificar) {
      try {
        const gtcconsfRows = await prisma.$queryRawUnsafe<Array<{ NrSeqControle: number }>>(`
          SELECT TOP 1 NrSeqControle
          FROM [${SENIOR_DATABASE}]..GTCCONSF WITH (NOLOCK)
          WHERE CdEmpresa = ${cdEmpresa}
            AND NrSeqControle = ${nrSeqControleParaVerificar}
        `);
        if (gtcconsfRows && gtcconsfRows.length > 0 && gtcconsfRows[0]?.NrSeqControle) {
          resultado.existeGTCCONSF = true;
          resultado.nrSeqControleGTCCONSF = gtcconsfRows[0].NrSeqControle;
        }
      } catch (error: any) {
        logger.debug({ error: error.message }, 'Erro ao verificar GTCCONSF (não crítico)');
      }
    }

    return resultado;
  } catch (error: any) {
    logger.error({ error: error.message }, 'Erro ao verificar CT-e existente');
    return resultado;
  }
};

/**
 * Função principal de integração de CT-e
 * Orquestra todas as inserções nas tabelas Senior
 * AGORA: Verifica existência antes de inserir. Se existir, marca como processado e cria evento.
 */
export async function inserirContasReceberCTe(
  prisma: PrismaExecutor,
  cte: CteData,
): Promise<{
  success: boolean;
  error?: string;
  nrSeqControle?: number;
  jaExistia?: boolean;
  tabelasInseridas?: string[];
  tabelasFalhadas?: Array<{ tabela: string; erro: string }>;
}> {
  const integrationStartTime = Date.now();
  let nrSeqControle = 0;

  if (shouldBypassCteSeniorLegacy()) {
    await alterarXMLProcessado(prisma, cte.external_id);
    logger.info(
      {
        external_id: cte.external_id,
        authorization_number: cte.authorization_number,
        isPostgres: IS_POSTGRES,
        enableSqlServerLegacy: env.ENABLE_SQLSERVER_LEGACY,
      },
      'Integração CT-e com Senior desativada em PostgreSQL; CT-e marcado como processado localmente',
    );

    return {
      success: true,
      tabelasInseridas: ['LOCAL_POSTGRES'],
    };
  }

  try {
    logger.info(
      { external_id: cte.external_id, authorization_number: cte.authorization_number },
      'Iniciando integração de CT-e nas tabelas Senior',
    );

    // 1. Extrair CNPJ do emitente do XML
    const emitCnpj = extrairCnpjDoXml(cte.xml, 'emit');
    if (!emitCnpj) {
      throw new Error('CNPJ do emitente não encontrado no XML');
    }

    // 2. Obter código de empresa
    const cdEmpresa = await buildCdEmpresaFromCnpj(prisma, emitCnpj);
    logger.info({ cdEmpresa, emitCnpj }, 'Código de empresa obtido');

    // 3. Obter CNPJ do tomador (pagador)
    const cdInscricao = obterCnpjTomador(cte.xml, emitCnpj);
    logger.info({ cdInscricao }, 'CNPJ do tomador obtido');

    // 4. VERIFICAR SE CT-E JÁ EXISTE NAS TABELAS SENIOR (ANTES DE GERAR NOVO nrSeqControle)
    // IMPORTANTE: Se o CT-e já existe em alguma tabela, usar o nrSeqControle existente
    // para garantir que todas as tabelas usem o mesmo código
    const verificacao = await verificarCteExistente(prisma, cte.xml, cdEmpresa);

    // Se encontrou nrSeqControle em alguma tabela existente, usar esse
    const nrSeqControleExistente =
      verificacao.nrSeqControleGTCCONCE ||
      verificacao.nrSeqControleGTCCONHE ||
      verificacao.nrSeqControleGTCCONSF;

    if (nrSeqControleExistente) {
      // CT-e já existe em pelo menos uma tabela, usar o nrSeqControle existente
      nrSeqControle = nrSeqControleExistente;
      logger.info(
        {
          cdEmpresa,
          nrSeqControle,
          tabelaOrigem: verificacao.nrSeqControleGTCCONCE
            ? 'GTCCONCE'
            : verificacao.nrSeqControleGTCCONHE
              ? 'GTCCONHE'
              : 'GTCCONSF',
        },
        'Usando nrSeqControle existente das tabelas Senior',
      );
    } else {
      // CT-e não existe em nenhuma tabela, gerar novo nrSeqControle
      const seniorDatabase = SENIOR_DATABASE;
      nrSeqControle = await retornaNrSeqControle(prisma, cdEmpresa, seniorDatabase);
      // A função já retorna no mínimo 1, mas manter verificação por segurança
      if (nrSeqControle === 0) {
        nrSeqControle = 1;
      }
      logger.info(
        { cdEmpresa, nrSeqControle, seniorDatabase },
        'Novo número de sequência de controle gerado',
      );
    }

    // Usar nrSeqControle (seja existente ou gerado)
    const nrSeqControleFinal = nrSeqControle;

    // Se CT-e existe em TODAS as tabelas, marcar como processado e retornar
    if (verificacao.existeGTCCONCE && verificacao.existeGTCCONHE && verificacao.existeGTCCONSF) {
      logger.info(
        {
          external_id: cte.external_id,
          existeGTCCONCE: verificacao.existeGTCCONCE,
          existeGTCCONHE: verificacao.existeGTCCONHE,
          existeGTCCONSF: verificacao.existeGTCCONSF,
          nCT: verificacao.nCT,
          nrSeqControle: nrSeqControleFinal,
        },
        'CT-e já existe em todas as tabelas Senior, marcando como processado',
      );

      // Marcar como processado na tabela ctes
      await alterarXMLProcessado(prisma, cte.external_id);

      // Retornar informações sobre existência (será usado para criar evento)
      return {
        success: true,
        nrSeqControle: nrSeqControleFinal,
        jaExistia: true,
      };
    }

    // 6. CT-E EXISTE PARCIALMENTE OU NÃO EXISTE - INSERIR NAS TABELAS FALTANTES
    // IMPORTANTE: Usar o mesmo nrSeqControle para todas as tabelas
    // Se já existe em alguma tabela, usar o nrSeqControle dela
    // Se não existe em nenhuma, usar o nrSeqControle gerado
    const tabelasInseridas: string[] = [];
    const tabelasFalhadas: Array<{ tabela: string; erro: string }> = [];

    // Usar nrSeqControle (já definido acima como existente ou gerado)
    const nrSeqControleParaInsercao = nrSeqControleFinal;

    logger.info(
      {
        external_id: cte.external_id,
        nrSeqControle: nrSeqControleParaInsercao,
        existeGTCCONCE: verificacao.existeGTCCONCE,
        existeGTCCONHE: verificacao.existeGTCCONHE,
        existeGTCCONSF: verificacao.existeGTCCONSF,
        usandoNrSeqControleExistente: !!(
          verificacao.nrSeqControleGTCCONCE ||
          verificacao.nrSeqControleGTCCONHE ||
          verificacao.nrSeqControleGTCCONSF
        ),
      },
      'Iniciando inserção nas tabelas faltantes (usando mesmo nrSeqControle para todas)',
    );

    const cdChaveAcesso = extrairChaveAcesso(cte.xml);

    // Inserir GTCCONCE apenas se não existir
    if (!verificacao.existeGTCCONCE && cdChaveAcesso) {
      try {
        await inserirGTCCONCE(prisma, cte.xml, cdEmpresa, nrSeqControleParaInsercao);
        tabelasInseridas.push('GTCCONCE');
        logger.info(
          { cdEmpresa, nrSeqControle: nrSeqControleParaInsercao },
          'GTCCONCE inserido (não existia)',
        );
      } catch (error: any) {
        tabelasFalhadas.push({ tabela: 'GTCCONCE', erro: error.message || 'Erro desconhecido' });
        logger.error({ error: error.message, tabela: 'GTCCONCE' }, 'Erro ao inserir GTCCONCE');
      }
    } else if (verificacao.existeGTCCONCE) {
      logger.debug(
        { cdEmpresa, nrSeqControle: nrSeqControleParaInsercao },
        'GTCCONCE já existe, pulando inserção',
      );
    }

    // Inserir GTCCONHE apenas se não existir
    if (!verificacao.existeGTCCONHE) {
      try {
        await inserirGTCCONHE(prisma, cte.xml, cdEmpresa, nrSeqControleParaInsercao, cdInscricao);
        tabelasInseridas.push('GTCCONHE');
        logger.info(
          { cdEmpresa, nrSeqControle: nrSeqControleParaInsercao },
          'GTCCONHE inserido (não existia)',
        );
      } catch (error: any) {
        tabelasFalhadas.push({ tabela: 'GTCCONHE', erro: error.message || 'Erro desconhecido' });
        logger.error({ error: error.message, tabela: 'GTCCONHE' }, 'Erro ao inserir GTCCONHE');
      }
    } else {
      logger.debug(
        { cdEmpresa, nrSeqControle: nrSeqControleParaInsercao },
        'GTCCONHE já existe, pulando inserção',
      );
    }

    // Inserir GTCCONSF apenas se não existir
    if (!verificacao.existeGTCCONSF) {
      try {
        // Excluir antes de inserir para evitar duplicatas
        await excluirGTCCONSF(prisma, cdEmpresa, nrSeqControleParaInsercao);
        await inserirGTCCONSF(prisma, cte.xml, cdEmpresa, nrSeqControleParaInsercao);
        tabelasInseridas.push('GTCCONSF');
        logger.info(
          { cdEmpresa, nrSeqControle: nrSeqControleParaInsercao },
          'GTCCONSF inserido (não existia)',
        );
      } catch (error: any) {
        // Se for erro de chave duplicada e o registro já existe, considerar sucesso
        if (
          error?.code === 'P2010' ||
          error?.message?.includes('2627') ||
          error?.message?.includes('PRIMARY KEY constraint')
        ) {
          try {
            const checkSql = `
              SELECT TOP 1 NrSeqControle
              FROM [${SENIOR_DATABASE}].dbo.GTCCONSF WITH (NOLOCK)
              WHERE CdEmpresa = ${cdEmpresa} AND NrSeqControle = ${nrSeqControleParaInsercao};
            `;
            const existing =
              await prisma.$queryRawUnsafe<Array<{ NrSeqControle: number }>>(checkSql);
            if (existing && existing.length > 0) {
              logger.warn(
                { cdEmpresa, nrSeqControle: nrSeqControleParaInsercao },
                'GTCCONSF já existe, considerando sucesso',
              );
              tabelasInseridas.push('GTCCONSF');
            } else {
              tabelasFalhadas.push({
                tabela: 'GTCCONSF',
                erro: error.message || 'Erro desconhecido',
              });
              logger.error(
                { error: error.message, tabela: 'GTCCONSF' },
                'Erro ao inserir GTCCONSF',
              );
            }
          } catch (checkError: any) {
            tabelasFalhadas.push({
              tabela: 'GTCCONSF',
              erro: error.message || 'Erro desconhecido',
            });
            logger.error({ error: error.message, tabela: 'GTCCONSF' }, 'Erro ao inserir GTCCONSF');
          }
        } else {
          tabelasFalhadas.push({ tabela: 'GTCCONSF', erro: error.message || 'Erro desconhecido' });
          logger.error({ error: error.message, tabela: 'GTCCONSF' }, 'Erro ao inserir GTCCONSF');
        }
      }
    } else {
      logger.debug(
        { cdEmpresa, nrSeqControle: nrSeqControleParaInsercao },
        'GTCCONSF já existe, pulando inserção',
      );
    }

    // Se todas as tabelas falharam, retornar erro
    if (tabelasInseridas.length === 0 && tabelasFalhadas.length > 0) {
      const erroCompleto = `Erro ao inserir CT-e em todas as tabelas: ${tabelasFalhadas.map((t) => `${t.tabela} (${t.erro})`).join(', ')}`;
      throw new Error(erroCompleto);
    }

    // 9. Marcar como processado
    await alterarXMLProcessado(prisma, cte.external_id);

    // Identificar tabelas que já existiam
    const tabelasJaExistentes: string[] = [];
    if (verificacao.existeGTCCONCE) tabelasJaExistentes.push('GTCCONCE');
    if (verificacao.existeGTCCONHE) tabelasJaExistentes.push('GTCCONHE');
    if (verificacao.existeGTCCONSF) tabelasJaExistentes.push('GTCCONSF');

    const integrationTimeMs = Date.now() - integrationStartTime;
    logger.info(
      {
        external_id: cte.external_id,
        cdEmpresa,
        nrSeqControle: nrSeqControleFinal,
        integrationTimeMs,
        tabelasInseridas,
        tabelasJaExistentes: tabelasJaExistentes.length > 0 ? tabelasJaExistentes : undefined,
        tabelasFalhadas: tabelasFalhadas.length > 0 ? tabelasFalhadas : undefined,
      },
      'CT-e integrado nas tabelas Senior',
    );

    const resultado: {
      success: boolean;
      nrSeqControle: number;
      tabelasInseridas: string[];
      tabelasFalhadas?: Array<{ tabela: string; erro: string }>;
      jaExistia?: boolean;
    } = {
      success: true,
      nrSeqControle: nrSeqControleFinal,
      tabelasInseridas,
    };

    // Se algumas tabelas já existiam, marcar como jaExistia
    if (tabelasJaExistentes.length > 0) {
      resultado.jaExistia = true;
    }

    if (tabelasFalhadas.length > 0) {
      resultado.tabelasFalhadas = tabelasFalhadas;
    }

    return resultado;
  } catch (error: any) {
    const integrationTimeMs = Date.now() - integrationStartTime;
    logger.error(
      {
        error: error.message,
        stack: error.stack,
        external_id: cte.external_id,
        nrSeqControle,
        integrationTimeMs,
      },
      'Erro ao integrar CT-e nas tabelas Senior',
    );

    return {
      success: false,
      error: error.message || 'Erro desconhecido',
      nrSeqControle,
    };
  }
}
