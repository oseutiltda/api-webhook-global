import type { PrismaClient } from '@prisma/client';
import { env } from '../config/env';
import { logger } from '../utils/logger';
import { inserirContasPagarSenior } from './contasPagarIntegration';
import type {
  Fatura,
  Filial,
  Fornecedor,
  ContaContabil,
  CentroCusto,
  Parcela,
  FaturaItens,
  ContasPagarCompleto,
} from '../types/contasPagar';

const CONTAS_PAGAR_BATCH_SIZE = Number(process.env.CONTAS_PAGAR_WORKER_BATCH_SIZE ?? '5');
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
    const sql = `EXEC dbo.P_VERIFICAR_ROBO_INTEGRADOR_EXECUCAO_CP`;
    const result = await prisma.$queryRawUnsafe<Array<{ result: number }>>(sql);
    if (result && result.length > 0 && result[0]?.result !== undefined) {
      return Number(result[0].result);
    }
    return 0;
  } catch (error: any) {
    logger.error({ error: error.message }, 'Erro ao verificar execução do robô');
    return 0;
  }
};

/**
 * Altera status de execução do robô
 */
const alterarExecucaoRobo = async (prisma: PrismaClient, roboExecutando: number): Promise<void> => {
  try {
    const sql = `
      EXEC dbo.P_ALTERAR_ROBO_INTEGRADOR_EXECUCAO_CP
        @RoboExecutando = ${roboExecutando};
    `;
    await prisma.$executeRawUnsafe(sql);
  } catch (error: any) {
    logger.error({ error: error.message, roboExecutando }, 'Erro ao alterar execução do robô');
    throw error;
  }
};

/**
 * Lista contas a pagar não integradas
 */
const listarContasPagarNaoIntegradas = async (prisma: PrismaClient): Promise<string[]> => {
  try {
    const sql = `EXEC dbo.P_INTEGRACAO_SENIOR_CP_NAO_INTEGRADOS_LISTAR`;
    const result = await prisma.$queryRawUnsafe<Array<{ document: string }>>(sql);
    if (result && result.length > 0) {
      return result.map((r) => r.document).filter((d) => d);
    }
    return [];
  } catch (error: any) {
    logger.error({ error: error.message }, 'Erro ao listar contas a pagar não integradas');
    return [];
  }
};

/**
 * Lê dados da fatura
 */
const lerDadosFatura = async (prisma: PrismaClient, document: string): Promise<Fatura | null> => {
  try {
    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_CP_DADOS_FATURA_OBTER
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
    logger.error({ error: error.message, document }, 'Erro ao ler dados da fatura');
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
      EXEC dbo.P_INTEGRACAO_SENIOR_CP_DADOS_FILIAL_FATURA_OBTER
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
    logger.error({ error: error.message, document }, 'Erro ao ler dados da filial');
    return null;
  }
};

/**
 * Lê dados do fornecedor
 */
const lerDadosFornecedorFatura = async (
  prisma: PrismaClient,
  document: string,
): Promise<Fornecedor | null> => {
  try {
    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_CP_DADOS_FORNECEDOR_FATURA_OBTER
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
    logger.error({ error: error.message, document }, 'Erro ao ler dados do fornecedor');
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
      EXEC dbo.P_INTEGRACAO_SENIOR_CP_DADOS_CONTACONTABIL_FATURA_OBTER
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
    logger.error({ error: error.message, document }, 'Erro ao ler dados da conta contábil');
    return null;
  }
};

/**
 * Lê dados do centro de custo
 */
const lerDadosCentroCustoFatura = async (
  prisma: PrismaClient,
  document: string,
): Promise<CentroCusto | null> => {
  try {
    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_CP_DADOS_CENTROCUSTO_FATURA_OBTER
        @document = ${toSqlValue(document)};
    `;
    const result = await prisma.$queryRawUnsafe<Array<any>>(sql);
    if (result && result.length > 0) {
      const row = result[0];
      return {
        id: toNumber(row.id),
        external_id: toNumber(row.external_id),
        debit_invoice_id: row.debit_invoice_id ? toNumber(row.debit_invoice_id) : null,
        cost_center_id: row.cost_center_id ? toNumber(row.cost_center_id) : null,
        name: row.name || null,
      };
    }
    return null;
  } catch (error: any) {
    logger.error({ error: error.message, document }, 'Erro ao ler dados do centro de custo');
    return null;
  }
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
      EXEC dbo.P_INTEGRACAO_SENIOR_CP_DADOS_PARCELA_FATURA_OBTER
        @document = ${toSqlValue(document)};
    `;
    const result = await prisma.$queryRawUnsafe<Array<any>>(sql);
    if (result && result.length > 0) {
      return result.map((row) => ({
        id: toNumber(row.id),
        external_id: toNumber(row.external_id),
        debit_invoice_id: row.debit_invoice_id ? toNumber(row.debit_invoice_id) : null,
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
    logger.error({ error: error.message, document }, 'Erro ao ler dados das parcelas');
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
      EXEC dbo.P_INTEGRACAO_SENIOR_CP_DADOS_ITENS_FATURA_OBTER
        @document = ${toSqlValue(document)};
    `;
    const result = await prisma.$queryRawUnsafe<Array<any>>(sql);
    if (result && result.length > 0) {
      return result.map((row) => ({
        id: toNumber(row.id),
        external_id: toNumber(row.external_id),
        debit_invoice_id: row.debit_invoice_id ? toNumber(row.debit_invoice_id) : null,
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
    logger.error({ error: error.message, document }, 'Erro ao ler dados dos itens da fatura');
    return [];
  }
};

/**
 * Marca conta a pagar como processada
 */
const alterarContasPagarParaProcessado = async (
  prisma: PrismaClient,
  document: string,
): Promise<void> => {
  try {
    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_CP_PROCESSADO_ALTERAR
        @document = ${toSqlValue(document)};
    `;
    await prisma.$executeRawUnsafe(sql);
    logger.info({ document }, 'Conta a pagar marcada como processada');
  } catch (error: any) {
    logger.error(
      { error: error.message, document },
      'Erro ao marcar conta a pagar como processada',
    );
    throw error;
  }
};

/**
 * Marca conta a pagar como processada com erro
 */
const alterarContasPagarParaProcessadoComErro = async (
  prisma: PrismaClient,
  document: string,
  codErro: string,
  msgErro: string,
): Promise<void> => {
  try {
    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_CP_PROCESSADO_COM_ERRO_ALTERAR
        @document = ${toSqlValue(document)},
        @codErro = ${toSqlValue(codErro)},
        @msgErro = ${toSqlValue(msgErro)};
    `;
    await prisma.$executeRawUnsafe(sql);
    logger.warn({ document, codErro, msgErro }, 'Conta a pagar marcada como processada com erro');
  } catch (error: any) {
    logger.error(
      { error: error.message, document, codErro, msgErro },
      'Erro ao marcar conta a pagar como processada com erro',
    );
    throw error;
  }
};

/**
 * Processa uma conta a pagar
 */
const processarContasPagar = async (prisma: PrismaClient, document: string): Promise<void> => {
  const processingStartTime = Date.now();
  const eventId = `contas-pagar-${document}`;

  try {
    logger.info({ document }, 'Iniciando processamento de conta a pagar');

    // Buscar todos os dados
    const fatura = await lerDadosFatura(prisma, document);
    if (!fatura) {
      throw new Error('Fatura não encontrada');
    }

    const filial = await lerDadosFilialFatura(prisma, document);
    if (!filial) {
      throw new Error('Filial não encontrada');
    }

    const fornecedor = await lerDadosFornecedorFatura(prisma, document);
    if (!fornecedor) {
      throw new Error('Fornecedor não encontrado');
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
          source: '/api/ContasPagar/InserirContasPagar',
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
      'Processando conta a pagar nas tabelas da Senior via stored procedures',
    );

    const resultado = await inserirContasPagarSenior(
      prisma,
      fatura,
      filial,
      fornecedor,
      contaContabil,
      centroCusto,
      parcelas,
      faturaItens,
    );

    const processingTimeMs = Date.now() - processingStartTime;

    if (resultado.success) {
      // Marcar como processado
      await alterarContasPagarParaProcessado(prisma, document);

      // Preparar metadata com informações detalhadas
      const tabelasInseridas = resultado.tabelasInseridas || [];
      const tabelasFalhadas = resultado.tabelasFalhadas || [];
      const temFalhas = tabelasFalhadas.length > 0;

      const metadata: any = {
        document,
        id: fatura.id,
        nrFatura: resultado.nrFatura,
        etapa: 'concluido',
        tabelasInseridas,
        resumo: {
          totalTabelas: tabelasInseridas.length + tabelasFalhadas.length,
          sucesso: tabelasInseridas.length,
          falhas: tabelasFalhadas.length,
        },
      };

      if (resultado.cdTitulo) {
        metadata.cdTitulo = resultado.cdTitulo;
      }

      if (temFalhas) {
        metadata.tabelasFalhadas = tabelasFalhadas;
        const mensagemErro = `Tabelas inseridas: ${tabelasInseridas.join(', ')}. Tabelas com erro: ${tabelasFalhadas.map((t) => `${t.tabela} (${t.erro})`).join(', ')}.`;
        metadata.mensagemErro = mensagemErro;
      }

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
            seniorId: resultado.nrFatura ? `NrFatura-${resultado.nrFatura}` : null,
            errorMessage: temFalhas ? metadata.mensagemErro : null,
            metadata: JSON.stringify(metadata),
          },
        });
      }

      logger.info(
        {
          document,
          nrFatura: resultado.nrFatura,
          cdTitulo: resultado.cdTitulo,
          processingTimeMs,
          tabelasInseridas,
          tabelasFalhadas: temFalhas ? tabelasFalhadas : undefined,
        },
        'Conta a pagar processada com sucesso',
      );
    } else {
      // Marcar como processado com erro
      const codErro = '5'; // Código genérico de erro
      let msgErro = resultado.error || 'Erro desconhecido';

      // Formatar mensagem de timeout de forma mais clara
      if (msgErro.includes('timeout') || msgErro === 'timeout na GFAFATUR') {
        msgErro = 'timeout na GFAFATUR';
      }

      await alterarContasPagarParaProcessadoComErro(prisma, document, codErro, msgErro);

      // Preparar metadata com informações de erro
      const tabelasInseridas = resultado.tabelasInseridas || [];
      const tabelasFalhadas = resultado.tabelasFalhadas || [];

      const metadata: any = {
        document,
        id: fatura.id,
        error: msgErro,
        etapa: 'erro',
        tabelasInseridas,
        resumo: {
          totalTabelas: tabelasInseridas.length + tabelasFalhadas.length,
          sucesso: tabelasInseridas.length,
          falhas: tabelasFalhadas.length,
        },
      };

      if (tabelasFalhadas.length > 0) {
        metadata.tabelasFalhadas = tabelasFalhadas;
      }

      // Atualizar WebhookEvent
      if (webhookEvent) {
        await prisma.webhookEvent.update({
          where: { id: eventId },
          data: {
            status: 'failed',
            processedAt: new Date(),
            integrationStatus: 'failed',
            errorMessage: msgErro,
            processingTimeMs,
            metadata: JSON.stringify(metadata),
          },
        });
      }

      logger.error(
        {
          document,
          error: msgErro,
          processingTimeMs,
          tabelasInseridas,
          tabelasFalhadas,
        },
        'Falha ao processar conta a pagar',
      );
    }
  } catch (error: any) {
    const processingTimeMs = Date.now() - processingStartTime;
    const codErro = '6'; // Código genérico de erro
    const msgErro = error.message || 'Erro desconhecido';

    try {
      await alterarContasPagarParaProcessadoComErro(prisma, document, codErro, msgErro);

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
      'Erro ao processar conta a pagar',
    );
  }
};

const processPendingContasPagarPostgres = async (prisma: PrismaClient): Promise<void> => {
  const pendentes = await prisma.webhookEvent.findMany({
    where: {
      source: { in: ['/api/ContasPagar/InserirContasPagar', '/webhooks/faturas/pagar/criar'] },
      status: 'pending',
    },
    orderBy: { receivedAt: 'asc' },
    take: CONTAS_PAGAR_BATCH_SIZE,
  });

  if (pendentes.length === 0) {
    logger.debug('Nenhuma conta a pagar pendente para processamento local');
    return;
  }

  logger.info({ count: pendentes.length }, 'Processando contas a pagar em modo PostgreSQL local');

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
          workerService: 'contasPagarSync',
          observacao:
            'Integracao externa desativada; conta a pagar registrada/localmente processada no PostgreSQL.',
        }).substring(0, 2000),
      },
    });
  }
};

/**
 * Processa contas a pagar pendentes
 */
export async function processPendingContasPagar(prisma: PrismaClient): Promise<void> {
  if (IS_POSTGRES && !ENABLE_SQLSERVER_LEGACY) {
    try {
      await processPendingContasPagarPostgres(prisma);
    } catch (error: any) {
      logger.error(
        { error: error.message },
        'Erro ao processar contas a pagar em modo PostgreSQL local',
      );
    }
    return;
  }

  try {
    // Verificar se o robô está em execução
    const roboExecutando = await verificarExecucaoRobo(prisma);
    if (roboExecutando === 1) {
      logger.debug('Robô já está em execução, aguardando...');
      return;
    }

    // Marcar robô como em execução
    await alterarExecucaoRobo(prisma, 1);

    try {
      // Listar contas a pagar não integradas
      const documentos = await listarContasPagarNaoIntegradas(prisma);

      if (documentos.length === 0) {
        logger.debug('Nenhuma conta a pagar pendente encontrada');
        return;
      }

      logger.info({ count: documentos.length }, 'Contas a pagar pendentes encontradas');

      // Processar em lotes
      const batch = documentos.slice(0, CONTAS_PAGAR_BATCH_SIZE);
      for (const document of batch) {
        try {
          await processarContasPagar(prisma, document);
        } catch (error: any) {
          logger.error(
            { error: error.message, document },
            'Erro ao processar conta a pagar individual',
          );
          // Continuar processando outras contas mesmo se uma falhar
        }
      }
    } finally {
      // Sempre marcar robô como não executando
      await alterarExecucaoRobo(prisma, 0);
    }
  } catch (error: any) {
    logger.error({ error: error.message }, 'Erro ao processar contas a pagar pendentes');
    // Tentar marcar robô como não executando mesmo em caso de erro
    try {
      await alterarExecucaoRobo(prisma, 0);
    } catch (updateError: any) {
      logger.error({ error: updateError.message }, 'Erro ao desmarcar execução do robô');
    }
  }
}
