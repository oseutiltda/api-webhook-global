import type { PrismaClient } from '@prisma/client';
import { env } from '../config/env';
import { logger } from '../utils/logger';
import { inserirContasReceberSenior } from './contasReceberIntegration';
import type {
  Fatura,
  Filial,
  Cliente,
  ContaContabil,
  CentroCusto,
  Parcela,
  FaturaItens,
} from '../types/contasReceber';

const CONTAS_RECEBER_BATCH_SIZE = Number(process.env.CONTAS_RECEBER_WORKER_BATCH_SIZE ?? '5');
const IS_POSTGRES = env.DATABASE_URL.startsWith('postgresql://');
const ENABLE_SQLSERVER_LEGACY = env.ENABLE_SQLSERVER_LEGACY;

const toSqlValue = (value: any): string => {
  if (value === null || value === undefined) return 'NULL';
  if (typeof value === 'string') {
    const escaped = value.replace(/'/g, "''");
    return `'${escaped}'`;
  }
  return `'${String(value)}'`;
};

const parseDate = (dateStr: string | null | undefined): Date | null => {
  if (!dateStr) return null;
  try {
    const date = new Date(dateStr);
    if (isNaN(date.getTime())) return null;
    return date;
  } catch {
    return null;
  }
};

const toNumber = (value: any): number => {
  if (value === null || value === undefined) return 0;
  const num = Number(value);
  return isNaN(num) ? 0 : num;
};

/**
 * Verifica se o robô está em execução
 */
const verificarExecucaoRobo = async (prisma: PrismaClient): Promise<number> => {
  try {
    const sql = `EXEC dbo.P_VERIFICAR_ROBO_INTEGRADOR_EXECUCAO_CR`;
    const result = await prisma.$queryRawUnsafe<Array<{ result: number }>>(sql);
    if (result && result.length > 0 && result[0]?.result !== undefined) {
      return Number(result[0].result);
    }
    return 0;
  } catch (error: any) {
    logger.error({ error: error.message }, 'Erro ao verificar execução do robô CR');
    return 0;
  }
};

/**
 * Altera status de execução do robô
 */
const alterarExecucaoRobo = async (prisma: PrismaClient, roboExecutando: number): Promise<void> => {
  try {
    const sql = `
      EXEC dbo.P_ALTERAR_ROBO_INTEGRADOR_EXECUCAO_CR
        @RoboExecutando = ${roboExecutando};
    `;
    await prisma.$executeRawUnsafe(sql);
  } catch (error: any) {
    logger.error({ error: error.message, roboExecutando }, 'Erro ao alterar execução do robô CR');
    throw error;
  }
};

/**
 * Lista contas a receber não integradas
 */
const listarContasReceberNaoIntegradas = async (prisma: PrismaClient): Promise<string[]> => {
  try {
    const sql = `EXEC dbo.P_INTEGRACAO_SENIOR_CR_FECHAMENTO_NAO_INTEGRADOS_LISTAR`;
    const result = await prisma.$queryRawUnsafe<Array<{ document: string }>>(sql);
    if (result && result.length > 0) {
      return result.map((r) => r.document).filter((d) => d);
    }
    return [];
  } catch (error: any) {
    logger.error({ error: error.message }, 'Erro ao listar contas a receber não integradas');
    return [];
  }
};

/**
 * Lê dados da fatura
 */
const lerDadosFatura = async (prisma: PrismaClient, document: string): Promise<Fatura | null> => {
  try {
    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_CR_DADOS_FATURA_OBTER
        @document = ${toSqlValue(document)};
    `;
    const result = await prisma.$queryRawUnsafe<Array<any>>(sql);
    if (result && result.length > 0) {
      const row = result[0];
      return {
        id: toNumber(row.id),
        external_id: toNumber(row.external_id),
        type: row.type || null,
        document: row.document || null,
        issue_date: parseDate(row.issue_date),
        due_date: parseDate(row.due_date),
        value: toNumber(row.value),
        installment_period: row.installment_period || null,
        comments: row.comments || null,
        installment_count: toNumber(row.installment_count),
      };
    }
    return null;
  } catch (error: any) {
    logger.error({ error: error.message, document }, 'Erro ao ler dados da fatura CR');
    return null;
  }
};

/**
 * Lê dados da filial
 */
const lerDadosFilialFatura = async (
  prisma: PrismaClient,
  document: string,
): Promise<Filial | null> => {
  try {
    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_CR_DADOS_FILIAL_FATURA_OBTER
        @document = ${toSqlValue(document)};
    `;
    const result = await prisma.$queryRawUnsafe<Array<any>>(sql);
    if (result && result.length > 0) {
      const row = result[0];
      return {
        id: toNumber(row.id),
        external_id: toNumber(row.external_id),
        person_id: row.person_id ? toNumber(row.person_id) : null,
        nickname: row.nickname || null,
        cnpj: row.cnpj || null,
      };
    }
    return null;
  } catch (error: any) {
    logger.error({ error: error.message, document }, 'Erro ao ler dados da filial CR');
    return null;
  }
};

/**
 * Lê dados do cliente
 */
const lerDadosClienteFatura = async (
  prisma: PrismaClient,
  document: string,
): Promise<Cliente | null> => {
  try {
    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_CR_DADOS_CLIENTE_FATURA_OBTER
        @document = ${toSqlValue(document)};
    `;
    const result = await prisma.$queryRawUnsafe<Array<any>>(sql);
    if (result && result.length > 0) {
      const row = result[0];
      return {
        id: toNumber(row.id),
        external_id: toNumber(row.external_id),
        name: row.name || null,
        type: row.type || null,
        cnpj: row.cnpj || null,
        cpf: row.cpf || null,
        person_type: row.person_type || null,
        email: row.email || null,
        phone: row.phone || null,
      };
    }
    return null;
  } catch (error: any) {
    logger.error({ error: error.message, document }, 'Erro ao ler dados do cliente CR');
    return null;
  }
};

/**
 * Lê dados da conta contábil
 */
const lerDadosContaContabilFatura = async (
  prisma: PrismaClient,
  document: string,
): Promise<ContaContabil | null> => {
  try {
    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_CR_DADOS_CONTACONTABIL_FATURA_OBTER
        @document = ${toSqlValue(document)};
    `;
    const result = await prisma.$queryRawUnsafe<Array<any>>(sql);
    if (result && result.length > 0) {
      const row = result[0];
      return {
        id: toNumber(row.id),
        external_id: toNumber(row.external_id),
        name: row.name || null,
        code_cahe: row.code_cache || null,
      };
    }
    return null;
  } catch (error: any) {
    logger.error({ error: error.message, document }, 'Erro ao ler dados da conta contábil CR');
    return null;
  }
};

/**
 * Lê dados do centro de custo (pode retornar null)
 */
const lerDadosCentroCustoFatura = async (
  _prisma: PrismaClient,
  _document: string,
): Promise<CentroCusto | null> => {
  // Para Contas a Receber, centro de custo não é obrigatório
  // Retornar null se não houver procedure específica
  return null;
};

/**
 * Lê dados das parcelas
 */
const lerDadosParcelaFatura = async (
  prisma: PrismaClient,
  document: string,
): Promise<Parcela[]> => {
  try {
    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_CR_DADOS_PARCELA_FATURA_OBTER
        @document = ${toSqlValue(document)};
    `;
    const result = await prisma.$queryRawUnsafe<Array<any>>(sql);
    if (result && result.length > 0) {
      return result.map((row) => ({
        id: toNumber(row.id),
        external_id: toNumber(row.external_id),
        credit_invoice_id: row.credit_invoice_id ? toNumber(row.credit_invoice_id) : null,
        position: row.position ? toNumber(row.position) : null,
        due_date: parseDate(row.due_date),
        value: toNumber(row.value),
        interest_value: toNumber(row.interest_value),
        discount_value: toNumber(row.discount_value),
        payment_method: row.payment_method || null,
        comments: row.comments || null,
        status: row.status || null,
        payment_date: parseDate(row.payment_date),
      }));
    }
    return [];
  } catch (error: any) {
    logger.error({ error: error.message, document }, 'Erro ao ler dados das parcelas CR');
    return [];
  }
};

/**
 * Lê dados dos itens da fatura
 */
const lerDadosItensFaturaFatura = async (
  prisma: PrismaClient,
  document: string,
): Promise<FaturaItens[]> => {
  try {
    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_CR_DADOS_ITENS_FATURA_OBTER
        @document = ${toSqlValue(document)};
    `;
    const result = await prisma.$queryRawUnsafe<Array<any>>(sql);
    if (result && result.length > 0) {
      return result.map((row) => ({
        id: toNumber(row.id),
        external_id: toNumber(row.external_id),
        credit_invoice_id: row.credit_invoice_id ? toNumber(row.credit_invoice_id) : null,
        freight_id: row.freight_id ? toNumber(row.freight_id) : null,
        cte_key: row.cte_key || null,
        cte_number: row.cte_number ? toNumber(row.cte_number) : null,
        cte_series: row.cte_series ? toNumber(row.cte_series) : null,
        payer_name: row.payer_name || null,
        draft_number: row.draft_number || null,
        nfse_number: row.nfse_number || null,
        nfse_series: row.nfse_series || null,
        total: toNumber(row.total),
        type: row.type || null,
      }));
    }
    return [];
  } catch (error: any) {
    logger.error({ error: error.message, document }, 'Erro ao ler dados dos itens da fatura CR');
    return [];
  }
};

/**
 * Marca conta a receber como processada
 */
const alterarContasReceberParaProcessado = async (
  prisma: PrismaClient,
  document: string,
): Promise<void> => {
  try {
    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_CR_PROCESSADO_ALTERAR
        @document = ${toSqlValue(document)};
    `;
    await prisma.$executeRawUnsafe(sql);
    logger.info({ document }, 'Conta a receber marcada como processada');
  } catch (error: any) {
    logger.error(
      { error: error.message, document },
      'Erro ao marcar conta a receber como processada',
    );
    throw error;
  }
};

/**
 * Processa uma conta a receber
 */
const processarContasReceber = async (prisma: PrismaClient, document: string): Promise<void> => {
  const processingStartTime = Date.now();
  const eventId = `contas-receber-${document}`;

  try {
    logger.info({ document }, 'Iniciando processamento de conta a receber');

    // Buscar todos os dados
    const fatura = await lerDadosFatura(prisma, document);
    if (!fatura) {
      throw new Error('Fatura não encontrada');
    }

    const filial = await lerDadosFilialFatura(prisma, document);
    if (!filial) {
      throw new Error('Filial não encontrada');
    }

    const cliente = await lerDadosClienteFatura(prisma, document);
    if (!cliente) {
      throw new Error('Cliente não encontrado');
    }

    const contaContabil = await lerDadosContaContabilFatura(prisma, document);
    if (!contaContabil) {
      throw new Error('Conta contábil não encontrada');
    }

    const centroCusto = await lerDadosCentroCustoFatura(prisma, document);
    const parcelas = await lerDadosParcelaFatura(prisma, document);
    const faturaItens = await lerDadosItensFaturaFatura(prisma, document);

    // Criar ou atualizar evento WebhookEvent
    let webhookEvent = null;
    try {
      webhookEvent = await prisma.webhookEvent.upsert({
        where: { id: eventId },
        create: {
          id: eventId,
          source: '/api/ContasReceber/InserirContasReceber',
          status: 'processing',
          tipoIntegracao: 'Worker',
          metadata: JSON.stringify({
            document,
            id: fatura.id,
            etapa: 'processamento',
          }),
        },
        update: {
          status: 'processing',
          metadata: JSON.stringify({
            document,
            id: fatura.id,
            etapa: 'processamento',
          }),
        },
      });
    } catch (error: any) {
      logger.warn(
        { error: error?.message, document, eventId },
        'Erro ao criar/atualizar WebhookEvent',
      );
    }

    // Processar integração com Senior
    logger.info(
      { document },
      'Processando conta a receber nas tabelas da Senior via stored procedures',
    );

    const resultado = await inserirContasReceberSenior(prisma, {
      document,
      Fatura: fatura,
      Filial: filial,
      Cliente: cliente,
      ContaContabil: contaContabil,
      CentroCusto: centroCusto,
      FaturaItens: faturaItens,
      Parcelas: parcelas,
    });

    const processingTimeMs = Date.now() - processingStartTime;

    if (resultado.success) {
      // Marcar como processado
      await alterarContasReceberParaProcessado(prisma, document);

      // Atualizar WebhookEvent
      if (webhookEvent) {
        await prisma.webhookEvent.update({
          where: { id: eventId },
          data: {
            status: 'processed',
            processedAt: new Date(),
            integrationStatus: 'integrated',
            processingTimeMs,
            integrationTimeMs: processingTimeMs,
            seniorId: resultado.cdFatura ? `CdFatura-${resultado.cdFatura}` : null,
            metadata: JSON.stringify({
              document,
              id: fatura.id,
              cdFatura: resultado.cdFatura,
            }),
          },
        });
      }

      logger.info(
        {
          document,
          cdFatura: resultado.cdFatura,
          processingTimeMs,
        },
        'Conta a receber processada com sucesso',
      );
    } else {
      // Atualizar WebhookEvent com erro
      if (webhookEvent) {
        await prisma.webhookEvent.update({
          where: { id: eventId },
          data: {
            status: 'failed',
            processedAt: new Date(),
            integrationStatus: 'failed',
            errorMessage: resultado.error || 'Erro desconhecido',
            processingTimeMs,
            metadata: JSON.stringify({
              document,
              id: fatura.id,
              error: resultado.error,
            }),
          },
        });
      }

      logger.error(
        {
          document,
          error: resultado.error,
          processingTimeMs,
        },
        'Falha ao processar conta a receber',
      );
    }
  } catch (error: any) {
    const processingTimeMs = Date.now() - processingStartTime;
    const msgErro = error.message || 'Erro desconhecido';

    try {
      // Atualizar WebhookEvent
      await prisma.webhookEvent
        .update({
          where: { id: eventId },
          data: {
            status: 'failed',
            processedAt: new Date(),
            integrationStatus: 'failed',
            errorMessage: msgErro,
            processingTimeMs,
            metadata: JSON.stringify({
              document,
              error: msgErro,
            }),
          },
        })
        .catch(() => {
          // Ignorar erro se o evento não existir
        });
    } catch (updateError: any) {
      logger.error({ error: updateError.message, document }, 'Erro ao atualizar status de erro');
    }

    logger.error(
      {
        document,
        error: error.message,
        stack: error.stack,
        processingTimeMs,
      },
      'Erro ao processar conta a receber',
    );
  }
};

const processPendingContasReceberPostgres = async (prisma: PrismaClient): Promise<void> => {
  const pendentes = await prisma.webhookEvent.findMany({
    where: {
      source: {
        in: ['/api/ContasReceber/InserirContasReceber', '/webhooks/faturas/receber/criar'],
      },
      status: 'pending',
    },
    orderBy: { receivedAt: 'asc' },
    take: CONTAS_RECEBER_BATCH_SIZE,
  });

  if (pendentes.length === 0) {
    logger.debug('Nenhuma conta a receber pendente para processamento local');
    return;
  }

  logger.info({ count: pendentes.length }, 'Processando contas a receber em modo PostgreSQL local');

  for (const event of pendentes) {
    const start = Date.now();
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
      data: {
        status: 'processed',
        processedAt: new Date(),
        integrationStatus: 'integrated',
        processingTimeMs: Date.now() - start,
        metadata: JSON.stringify({
          ...metadataBase,
          workerMode: 'postgres_local_sem_legacy',
          workerService: 'contasReceberSync',
          observacao:
            'Integracao externa desativada; conta a receber registrada/localmente processada no PostgreSQL.',
        }).substring(0, 2000),
      },
    });
  }
};

/**
 * Processa contas a receber pendentes
 */
export async function processPendingContasReceber(prisma: PrismaClient): Promise<void> {
  if (IS_POSTGRES && !ENABLE_SQLSERVER_LEGACY) {
    try {
      await processPendingContasReceberPostgres(prisma);
    } catch (error: any) {
      logger.error(
        { error: error.message },
        'Erro ao processar contas a receber em modo PostgreSQL local',
      );
    }
    return;
  }

  try {
    // Verificar se o robô está em execução
    const roboExecutando = await verificarExecucaoRobo(prisma);
    if (roboExecutando === 1) {
      logger.debug('Robô CR já está em execução, aguardando...');
      return;
    }

    // Marcar robô como em execução
    await alterarExecucaoRobo(prisma, 1);

    try {
      // Listar contas a receber não integradas
      const documentos = await listarContasReceberNaoIntegradas(prisma);

      if (documentos.length === 0) {
        logger.debug('Nenhuma conta a receber pendente encontrada');
        return;
      }

      logger.info({ count: documentos.length }, 'Contas a receber pendentes encontradas');

      // Processar em lotes
      const batch = documentos.slice(0, CONTAS_RECEBER_BATCH_SIZE);
      for (const document of batch) {
        try {
          await processarContasReceber(prisma, document);
        } catch (error: any) {
          logger.error(
            { error: error.message, document },
            'Erro ao processar conta a receber individual',
          );
          // Continuar processando outras contas mesmo se uma falhar
        }
      }
    } finally {
      // Sempre marcar robô como não executando
      await alterarExecucaoRobo(prisma, 0);
    }
  } catch (error: any) {
    logger.error({ error: error.message }, 'Erro ao processar contas a receber pendentes');
    // Tentar marcar robô como não executando mesmo em caso de erro
    try {
      await alterarExecucaoRobo(prisma, 0);
    } catch (updateError: any) {
      logger.error({ error: updateError.message }, 'Erro ao desmarcar execução do robô CR');
    }
  }
}
