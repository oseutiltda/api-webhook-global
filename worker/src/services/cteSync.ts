import type { PrismaClient } from '@prisma/client';
import { env } from '../config/env';
import { logger } from '../utils/logger';
import {
  inserirContasReceberCTe,
  cancelarCte,
  extrairTextoDoXml,
  extrairCnpjDoXml,
  buildCdEmpresaFromCnpj,
  verificarCteExistente,
} from './cteIntegration';
import type { CteData } from '../types/cte';

const CTE_BATCH_SIZE = Number(process.env.CTE_WORKER_BATCH_SIZE ?? '5');
const CTE_SOURCE_DATABASE = process.env.CTE_SOURCE_DATABASE || 'AFS_INTEGRADOR';
const CTE_TABLE = `[${CTE_SOURCE_DATABASE}].[dbo].[ctes]`;
const IS_POSTGRES = env.DATABASE_URL.startsWith('postgresql://');

const shouldBypassCteLegacyFlow = (): boolean => {
  return IS_POSTGRES && !env.ENABLE_SQLSERVER_LEGACY;
};

const marcarCteProcessadoPorId = async (prisma: PrismaClient, cteId: number): Promise<void> => {
  if (IS_POSTGRES) {
    await prisma.cte.update({
      where: { id: cteId },
      data: { processed: true },
    });
    return;
  }

  const updateSql = `
    UPDATE ${CTE_TABLE}
    SET processed = 1
    WHERE id = ${cteId};
  `;
  await prisma.$executeRawUnsafe(updateSql);
};

/**
 * Lista CT-e não integrados usando stored procedure
 * Segue o padrão do código C# original: ListarContasReceberNaoIntegradas()
 * Procedure: P_INTEGRACAO_SENIOR_CR_NAO_INTEGRADOS_LISTAR
 */
const listarCtesNaoIntegrados = async (
  prisma: PrismaClient,
): Promise<
  Array<{
    id: number;
    external_id: number;
    Status: string;
    XML: string;
    Processado: boolean;
  }>
> => {
  try {
    if (IS_POSTGRES) {
      const rows = await prisma.cte.findMany({
        where: {
          processed: false,
          status: { not: 'canceled' },
        },
        orderBy: { id: 'asc' },
        take: CTE_BATCH_SIZE,
        select: {
          id: true,
          external_id: true,
          status: true,
          xml: true,
          processed: true,
        },
      });

      return rows.map((row) => ({
        id: row.id,
        external_id: row.external_id,
        Status: row.status,
        XML: row.xml,
        Processado: row.processed,
      }));
    }

    const sql = `EXEC dbo.P_INTEGRACAO_SENIOR_CR_NAO_INTEGRADOS_LISTAR`;
    const rows = await prisma.$queryRawUnsafe<
      Array<{
        id: number;
        external_id: number;
        Status: string;
        XML: string;
        Processado: boolean;
      }>
    >(sql);

    // Limitar o número de registros processados por batch
    return rows.slice(0, CTE_BATCH_SIZE);
  } catch (error: any) {
    logger.error({ error: error.message }, 'Erro ao listar CT-e não integrados');
    return [];
  }
};

/**
 * Busca CT-e pendentes (processed = 0) da tabela final
 * Agora usa stored procedure seguindo o padrão do código C# original
 */
const buscarCtesPendentes = async (prisma: PrismaClient): Promise<number[]> => {
  try {
    const ctes = await listarCtesNaoIntegrados(prisma);
    return ctes.map((cte) => cte.id);
  } catch (error: any) {
    logger.error({ error: error.message }, 'Erro ao buscar CT-e pendentes');
    return [];
  }
};

/**
 * Busca dados completos de um CT-e
 * Primeiro tenta buscar via stored procedure (padrão C#), depois via Prisma como fallback
 */
const buscarCtePorId = async (prisma: PrismaClient, cteId: number): Promise<CteData | null> => {
  try {
    // Primeiro tentar buscar via stored procedure (se disponível no futuro)
    // Por enquanto, usar Prisma como no código original mantido
    const cte = await prisma.cte.findUnique({
      where: { id: cteId },
    });

    if (!cte) {
      return null;
    }

    return {
      id: cte.id,
      external_id: cte.external_id,
      authorization_number: cte.authorization_number,
      status: cte.status,
      xml: cte.xml,
      event_xml: cte.event_xml,
      processed: cte.processed,
      created_at: cte.created_at,
      updated_at: cte.updated_at,
    };
  } catch (error: any) {
    logger.error({ error: error.message, cteId }, 'Erro ao buscar CT-e por ID');
    return null;
  }
};

/**
 * Processa um único CT-e
 */
const processarCte = async (prisma: PrismaClient, cteId: number) => {
  const eventIdBase = `cte-${cteId}`;
  const processingStartTime = Date.now();
  let webhookEvent = null;
  let payload: CteData | null = null;

  const eventId = eventIdBase;

  // Tentar encontrar evento existente criado pelo backend
  try {
    const existingEvent = await prisma.webhookEvent.findUnique({
      where: { id: eventId },
    });

    if (existingEvent) {
      webhookEvent = existingEvent;
      logger.debug({ cteId, eventId }, 'Reutilizando evento WebhookEvent criado pelo backend');
    } else {
      logger.debug({ cteId, eventId }, 'Evento não encontrado, será criado pelo worker');
    }
  } catch (error: any) {
    logger.warn({ error: error?.message, cteId, eventId }, 'Erro ao buscar evento existente');
  }

  try {
    logger.info({ cteId }, 'Iniciando processamento do CT-e');

    // Buscar dados do CT-e
    payload = await buscarCtePorId(prisma, cteId);
    if (!payload) {
      throw new Error(`CT-e ${cteId} não encontrado`);
    }

    // Criar ou atualizar evento WebhookEvent
    await prisma.webhookEvent.upsert({
      where: { id: eventId },
      create: {
        id: eventId,
        source: 'worker/cte',
        status: 'processing',
        tipoIntegracao: 'Worker',
        metadata: JSON.stringify({
          cteId,
          external_id: payload.external_id,
          authorization_number: payload.authorization_number,
          status: payload.status,
          etapa: 'processamento',
        }),
      },
      update: {
        status: 'processing',
        tipoIntegracao: 'Worker',
        metadata: JSON.stringify({
          cteId,
          external_id: payload.external_id,
          authorization_number: payload.authorization_number,
          status: payload.status,
          etapa: 'processamento',
        }),
      },
    });

    // Processar integração com Senior
    logger.info(
      { cteId, external_id: payload.external_id },
      'Processando CT-e nas tabelas da Senior via stored procedures',
    );

    const resultado = await inserirContasReceberCTe(prisma, payload);

    const processingTimeMs = Date.now() - processingStartTime;

    if (resultado.success) {
      // Marcar CT-e como processado usando stored procedure (padrão C#)
      await alterarXMLProcessado(prisma, payload.external_id);

      // Atualizar WebhookEvent
      await prisma.webhookEvent.update({
        where: { id: eventId },
        data: {
          status: 'processed',
          processedAt: new Date(),
          processingTimeMs,
          integrationTimeMs: processingTimeMs,
          integrationStatus: 'integrated',
          seniorId: resultado.nrSeqControle ? String(resultado.nrSeqControle) : null,
          metadata: JSON.stringify({
            cteId,
            external_id: payload.external_id,
            authorization_number: payload.authorization_number,
            status: payload.status,
            nrSeqControle: resultado.nrSeqControle,
            etapa: 'concluido',
          }),
        },
      });

      logger.info(
        {
          cteId,
          external_id: payload.external_id,
          nrSeqControle: resultado.nrSeqControle,
          processingTimeMs,
        },
        'CT-e processado com sucesso',
      );
    } else {
      // Atualizar WebhookEvent com erro
      await prisma.webhookEvent.update({
        where: { id: eventId },
        data: {
          status: 'failed',
          processedAt: new Date(),
          errorMessage: resultado.error || 'Erro desconhecido',
          processingTimeMs,
          integrationTimeMs: processingTimeMs,
          integrationStatus: 'failed',
          metadata: JSON.stringify({
            cteId,
            external_id: payload.external_id,
            authorization_number: payload.authorization_number,
            status: payload.status,
            erro: resultado.error,
            etapa: 'falha',
          }),
        },
      });

      logger.error(
        {
          cteId,
          external_id: payload.external_id,
          error: resultado.error,
          processingTimeMs,
        },
        'Erro ao processar CT-e',
      );
    }
  } catch (error: any) {
    const processingTimeMs = Date.now() - processingStartTime;

    logger.error(
      {
        error: error.message,
        stack: error.stack,
        cteId,
        processingTimeMs,
      },
      'Erro ao processar CT-e',
    );

    // Atualizar WebhookEvent com erro
    try {
      const metadata: any = {
        cteId,
        external_id: payload?.external_id || null,
        authorization_number: payload?.authorization_number || null,
        status: payload?.status || null,
        erro: error.message || 'Erro desconhecido',
        etapa: 'erro',
      };

      await prisma.webhookEvent.upsert({
        where: { id: eventId },
        create: {
          id: eventId,
          source: 'worker/cte',
          status: 'failed',
          processedAt: new Date(),
          errorMessage: error.message || 'Erro desconhecido',
          processingTimeMs,
          integrationStatus: 'failed',
          tipoIntegracao: 'Worker',
          metadata: JSON.stringify(metadata).substring(0, 2000),
        },
        update: {
          status: 'failed',
          processedAt: new Date(),
          errorMessage: error.message || 'Erro desconhecido',
          tipoIntegracao: 'Worker',
          processingTimeMs,
          integrationStatus: 'failed',
          metadata: JSON.stringify(metadata).substring(0, 2000),
        },
      });
    } catch (webhookError: any) {
      logger.error(
        { error: webhookError.message, cteId, eventId },
        'Erro ao atualizar WebhookEvent',
      );
    }
  }
};

/**
 * Processa todos os CT-e pendentes
 * Segue o padrão do código C# original: usa stored procedure para listar e processa cada CT-e
 */
export async function processPendingCte(prisma: PrismaClient): Promise<void> {
  try {
    // Usar stored procedure para listar CT-e não integrados (seguindo padrão C#)
    const ctesNaoIntegrados = await listarCtesNaoIntegrados(prisma);

    if (ctesNaoIntegrados.length === 0) {
      return;
    }

    logger.info(
      { count: ctesNaoIntegrados.length, ids: ctesNaoIntegrados.map((c) => c.id) },
      'Processando CT-e pendentes via stored procedure',
    );

    // Processar cada CT-e retornado pela procedure
    for (const cteData of ctesNaoIntegrados) {
      // Converter dados da procedure para o formato esperado
      const payload: CteData = {
        id: cteData.id,
        external_id: cteData.external_id,
        authorization_number: 0, // Não retornado pela procedure, buscará depois se necessário
        status: cteData.Status,
        xml: cteData.XML,
        event_xml: null,
        processed: cteData.Processado,
        created_at: new Date(),
        updated_at: new Date(),
      };

      await processarCteComDados(prisma, payload);
    }
  } catch (error: any) {
    logger.error({ error: error.message, stack: error.stack }, 'Erro ao processar CT-e pendentes');
  }
}

/**
 * Processa um CT-e usando dados já obtidos (seguindo padrão C#)
 */
const processarCteComDados = async (prisma: PrismaClient, payload: CteData) => {
  const cteId = payload.id;
  const eventIdBase = `cte-${cteId}`;
  const processingStartTime = Date.now();
  let webhookEvent = null;

  const eventId = eventIdBase;

  try {
    logger.info({ cteId, external_id: payload.external_id }, 'Iniciando processamento do CT-e');

    // Criar ou atualizar evento WebhookEvent
    await prisma.webhookEvent.upsert({
      where: { id: eventId },
      create: {
        id: eventId,
        source: 'worker/cte',
        status: 'processing',
        tipoIntegracao: 'Worker',
        metadata: JSON.stringify({
          cteId,
          external_id: payload.external_id,
          authorization_number: payload.authorization_number,
          status: payload.status,
          etapa: 'processamento',
        }),
      },
      update: {
        status: 'processing',
        tipoIntegracao: 'Worker',
        metadata: JSON.stringify({
          cteId,
          external_id: payload.external_id,
          authorization_number: payload.authorization_number,
          status: payload.status,
          etapa: 'processamento',
        }),
      },
    });

    // Processar integração com Senior
    logger.info(
      { cteId, external_id: payload.external_id },
      'Processando CT-e nas tabelas da Senior via stored procedures',
    );

    const resultado = await inserirContasReceberCTe(prisma, payload);

    const processingTimeMs = Date.now() - processingStartTime;

    if (resultado.success) {
      if (shouldBypassCteLegacyFlow()) {
        await prisma.webhookEvent.update({
          where: { id: eventId },
          data: {
            status: 'processed',
            processedAt: new Date(),
            processingTimeMs,
            integrationTimeMs: processingTimeMs,
            integrationStatus: 'integrated',
            seniorId: null,
            errorMessage: null,
            metadata: JSON.stringify({
              cteId,
              external_id: payload.external_id,
              authorization_number: payload.authorization_number,
              status: payload.status,
              etapa: 'concluido_postgres_local',
              modo: 'postgres_local_sem_legacy',
              tabelasInseridas: resultado.tabelasInseridas || ['LOCAL_POSTGRES'],
            }),
          },
        });

        logger.info(
          {
            cteId,
            external_id: payload.external_id,
            processingTimeMs,
            mode: 'postgres_local_sem_legacy',
          },
          'CT-e processado em modo PostgreSQL local (sem validação de tabelas Senior)',
        );
        return;
      }

      // Buscar informações detalhadas sobre existência
      const emitCnpj = extrairCnpjDoXml(payload.xml, 'emit');
      const cdEmpresa = emitCnpj ? await buildCdEmpresaFromCnpj(prisma, emitCnpj) : null;
      const verificacao = cdEmpresa
        ? await verificarCteExistente(prisma, payload.xml, cdEmpresa)
        : null;

      const nCT = verificacao?.nCT || extrairTextoDoXml(payload.xml, 'nCT') || '';

      // Construir mensagem detalhada sobre quais tabelas possuem o CT-e
      const tabelasComCte: string[] = [];
      const tabelasSemCte: string[] = [];

      if (verificacao) {
        if (verificacao.existeGTCCONCE) {
          tabelasComCte.push('GTCCONCE');
        } else {
          tabelasSemCte.push('GTCCONCE');
        }

        if (verificacao.existeGTCCONHE) {
          tabelasComCte.push('GTCCONHE');
        } else {
          tabelasSemCte.push('GTCCONHE');
        }

        if (verificacao.existeGTCCONSF) {
          tabelasComCte.push('GTCCONSF');
        } else {
          tabelasSemCte.push('GTCCONSF');
        }
      } else {
        tabelasComCte.push('GTCCONCE', 'GTCCONHE', 'GTCCONSF');
      }

      const tabelasInseridas = resultado.tabelasInseridas || [];
      const tabelasFalhadas = resultado.tabelasFalhadas || [];

      // Se já existia em TODAS as tabelas e nenhuma foi inserida, marcar como skipped
      if (resultado.jaExistia && tabelasInseridas.length === 0) {
        const mensagemTabelas =
          tabelasComCte.length > 0
            ? `As Tabelas (${tabelasComCte.join(', ')}) já possuem o CT-e${nCT ? ` (número ${nCT})` : ''}.`
            : '';

        const mensagemCompleta =
          mensagemTabelas +
          (tabelasSemCte.length > 0 ? ` Tabelas sem CT-e: ${tabelasSemCte.join(', ')}.` : '');

        // Atualizar WebhookEvent com informação de que já existia
        await prisma.webhookEvent.update({
          where: { id: eventId },
          data: {
            status: 'processed',
            processedAt: new Date(),
            processingTimeMs,
            integrationTimeMs: 0,
            integrationStatus: 'skipped',
            errorMessage: mensagemCompleta,
            seniorId: resultado.nrSeqControle ? String(resultado.nrSeqControle) : null,
            metadata: JSON.stringify({
              cteId,
              external_id: payload.external_id,
              authorization_number: payload.authorization_number,
              status: payload.status,
              etapa: 'verificação_existência',
              jaExistia: true,
              existeGTCCONCE: verificacao?.existeGTCCONCE || false,
              existeGTCCONHE: verificacao?.existeGTCCONHE || false,
              existeGTCCONSF: verificacao?.existeGTCCONSF || false,
              nCT: nCT,
              tabelasComCte,
              tabelasSemCte,
            }),
          },
        });

        logger.info(
          {
            cteId,
            external_id: payload.external_id,
            nrSeqControle: resultado.nrSeqControle,
            processingTimeMs,
            tabelasComCte,
            tabelasSemCte,
          },
          'CT-e já existia em todas as tabelas Senior, marcado como processado',
        );
      } else {
        // CT-e foi inserido (totalmente ou parcialmente)
        const tabelasInseridas = resultado.tabelasInseridas || [];
        const tabelasFalhadas = resultado.tabelasFalhadas || [];
        const temFalhas = tabelasFalhadas.length > 0;

        const mensagemTabelas =
          tabelasInseridas.length > 0
            ? `Tabelas inseridas com sucesso: ${tabelasInseridas.join(', ')}${nCT ? ` (CT-e número ${nCT})` : ''}.`
            : '';
        const mensagemTabelasExistentes =
          tabelasComCte.length > 0 ? ` Tabelas que já existiam: ${tabelasComCte.join(', ')}.` : '';
        const mensagemFalhas = temFalhas
          ? ` Tabelas com erro: ${tabelasFalhadas.map((t: any) => `${t.tabela} (${t.erro})`).join(', ')}.`
          : '';
        const mensagemCompleta = mensagemTabelas + mensagemTabelasExistentes + mensagemFalhas;

        const metadata: any = {
          cteId,
          external_id: payload.external_id,
          authorization_number: payload.authorization_number,
          status: payload.status,
          nrSeqControle: resultado.nrSeqControle,
          etapa: 'concluido',
          tabelasInseridas,
          resumo: {
            totalTabelas: tabelasInseridas.length + tabelasFalhadas.length + tabelasComCte.length,
            inseridas: tabelasInseridas.length,
            jaExistentes: tabelasComCte.length,
            falhas: tabelasFalhadas.length,
          },
        };

        // Adicionar informações sobre tabelas já existentes se houver
        if (tabelasComCte.length > 0) {
          metadata.tabelasJaExistentes = tabelasComCte;
          metadata.jaExistiaParcialmente = true;
        }

        if (temFalhas) {
          metadata.tabelasFalhadas = tabelasFalhadas;
          // Verificar se algum erro é relacionado a peso
          const erroPeso = tabelasFalhadas.some(
            (t: any) =>
              t.erro?.includes('peso') ||
              t.erro?.includes('Peso') ||
              t.erro?.includes('peso cubado') ||
              t.erro?.includes('Peso cubado') ||
              t.erro?.includes('QtPeso') ||
              t.erro?.includes('QtPesoCubado') ||
              t.erro?.includes('QtVolume') ||
              t.erro?.includes('8114'),
          );
          if (erroPeso) {
            metadata.erroPeso = true;
            metadata.detalhesErroPeso =
              'Erro relacionado a peso/volume detectado. Verifique os valores de peso real, peso cubado (NULL) e volume.';
          }
        }

        if (nCT) {
          metadata.nCT = nCT;
        }

        await prisma.webhookEvent.update({
          where: { id: eventId },
          data: {
            status: 'processed',
            processedAt: new Date(),
            processingTimeMs,
            integrationTimeMs: processingTimeMs,
            integrationStatus: temFalhas ? 'partial' : 'integrated',
            seniorId: resultado.nrSeqControle ? String(resultado.nrSeqControle) : null,
            errorMessage: temFalhas ? mensagemCompleta : null,
            metadata: JSON.stringify(metadata).substring(0, 2000),
          },
        });

        logger.info(
          {
            cteId,
            external_id: payload.external_id,
            nrSeqControle: resultado.nrSeqControle,
            processingTimeMs,
            tabelasInseridas,
            tabelasJaExistentes: tabelasComCte.length > 0 ? tabelasComCte : undefined,
            tabelasFalhadas: temFalhas ? tabelasFalhadas : undefined,
          },
          tabelasInseridas.length > 0 && tabelasComCte.length > 0
            ? 'CT-e processado com sucesso (algumas tabelas inseridas, outras já existiam)'
            : 'CT-e processado com sucesso',
        );
      }
    } else {
      // Atualizar WebhookEvent com erro
      await prisma.webhookEvent.update({
        where: { id: eventId },
        data: {
          status: 'failed',
          processedAt: new Date(),
          errorMessage: resultado.error || 'Erro desconhecido',
          processingTimeMs,
          integrationTimeMs: processingTimeMs,
          integrationStatus: 'failed',
          metadata: JSON.stringify({
            cteId,
            external_id: payload.external_id,
            authorization_number: payload.authorization_number,
            status: payload.status,
            erro: resultado.error,
            etapa: 'falha',
          }),
        },
      });

      logger.error(
        {
          cteId,
          external_id: payload.external_id,
          error: resultado.error,
          processingTimeMs,
        },
        'Erro ao processar CT-e',
      );
    }
  } catch (error: any) {
    const processingTimeMs = Date.now() - processingStartTime;

    logger.error(
      {
        error: error.message,
        stack: error.stack,
        cteId,
        processingTimeMs,
      },
      'Erro ao processar CT-e',
    );

    // Atualizar WebhookEvent com erro
    try {
      await prisma.webhookEvent.upsert({
        where: { id: eventId },
        create: {
          id: eventId,
          source: 'worker/cte',
          status: 'failed',
          processedAt: new Date(),
          errorMessage: error.message || 'Erro desconhecido',
          processingTimeMs,
          integrationStatus: 'failed',
          tipoIntegracao: 'Worker',
          metadata: JSON.stringify({
            cteId,
            external_id: payload?.external_id || null,
            authorization_number: payload?.authorization_number || null,
            status: payload?.status || null,
            erro: error.message,
            etapa: 'erro',
          }),
        },
        update: {
          status: 'failed',
          processedAt: new Date(),
          errorMessage: error.message || 'Erro desconhecido',
          tipoIntegracao: 'Worker',
          processingTimeMs,
          integrationStatus: 'failed',
          metadata: JSON.stringify({
            cteId,
            external_id: payload?.external_id || null,
            authorization_number: payload?.authorization_number || null,
            status: payload?.status || null,
            erro: error.message,
            etapa: 'erro',
          }),
        },
      });
    } catch (webhookError: any) {
      logger.error(
        { error: webhookError.message, cteId, eventId },
        'Erro ao atualizar WebhookEvent',
      );
    }
  }
};

/**
 * Marca CT-e como processado usando stored procedure (padrão C#)
 * Procedure: P_ALTERAR_XML_PROCESSADO_CR_SENIOR
 */
const alterarXMLProcessado = async (prisma: PrismaClient, external_id: number): Promise<void> => {
  try {
    if (IS_POSTGRES) {
      const result = await prisma.cte.updateMany({
        where: { external_id },
        data: { processed: true },
      });
      if (result.count === 0) {
        logger.warn({ external_id }, 'Nenhum CT-e encontrado para marcar como processado');
      } else {
        logger.debug(
          { external_id, totalAtualizados: result.count },
          'CT-e marcado como processado',
        );
      }
      return;
    }

    const sql = `
      EXEC dbo.P_ALTERAR_XML_PROCESSADO_CR_SENIOR
        @External_id = ${external_id};
    `;
    await prisma.$executeRawUnsafe(sql);
    logger.debug({ external_id }, 'CT-e marcado como processado via procedure');
  } catch (error: any) {
    logger.error(
      { error: error.message, external_id },
      'Erro ao marcar CT-e como processado via procedure',
    );
    throw error;
  }
};

/**
 * Lista CT-e cancelados não processados
 * Busca da tabela ctes onde status = 'canceled' e processed = 0
 */
const listarCtesCanceladosNaoProcessados = async (
  prisma: PrismaClient,
): Promise<
  Array<{
    id: number;
    xml: string;
    event_xml: string | null;
    chCTe: string | null;
    nProt: string | null;
    dhRegEvento: string | null;
    tpEvento: string | null;
    xEvento: string | null;
    emitCnpj: string | null;
    authorization_number: number;
    status: string;
    serie: string | null;
  }>
> => {
  try {
    if (IS_POSTGRES) {
      const ctesCancelados = await prisma.cte.findMany({
        where: {
          status: 'canceled',
          processed: false,
        },
        orderBy: { id: 'asc' },
        take: CTE_BATCH_SIZE,
        select: {
          id: true,
          xml: true,
          event_xml: true,
          authorization_number: true,
          status: true,
        },
      });

      return ctesCancelados.map((cte) => {
        const xmlParaProcessar = cte.event_xml || cte.xml;
        const chCTe = extrairTextoDoXml(xmlParaProcessar, 'chCTe');
        const nProt = extrairTextoDoXml(xmlParaProcessar, 'nProt') || null;
        const dhRegEvento = extrairTextoDoXml(xmlParaProcessar, 'dhRegEvento') || null;
        const tpEvento = extrairTextoDoXml(xmlParaProcessar, 'tpEvento') || null;
        const xEvento = extrairTextoDoXml(xmlParaProcessar, 'xEvento') || null;
        const emitCnpj = extrairCnpjDoXml(xmlParaProcessar, 'CNPJ') || null;
        const serie =
          extrairTextoDoXml(xmlParaProcessar, 'serie') ||
          extrairTextoDoXml(cte.xml, 'serie') ||
          null;

        return {
          id: cte.id,
          xml: cte.xml,
          event_xml: cte.event_xml,
          chCTe,
          nProt,
          dhRegEvento,
          tpEvento,
          xEvento,
          emitCnpj,
          authorization_number: cte.authorization_number,
          status: cte.status,
          serie,
        };
      });
    }

    const ctesTable = CTE_TABLE;
    const sql = `
      SELECT TOP ${CTE_BATCH_SIZE}
        id,
        xml,
        event_xml,
        authorization_number,
        status
      FROM ${ctesTable} WITH (NOLOCK)
      WHERE status = 'canceled'
        AND processed = 0
      ORDER BY id ASC
    `;

    const ctesCancelados = await prisma.$queryRawUnsafe<
      Array<{
        id: number;
        xml: string;
        event_xml: string | null;
        authorization_number: number;
        status: string;
      }>
    >(sql);

    // Processar cada registro e extrair dados do XML
    return (ctesCancelados || []).map((cte) => {
      // Usar event_xml se disponível, caso contrário usar xml
      const xmlParaProcessar = cte.event_xml || cte.xml;

      // Extrair chave de acesso do XML
      const chCTe = extrairTextoDoXml(xmlParaProcessar, 'chCTe');

      // Extrair dados do evento de cancelamento
      const nProt = extrairTextoDoXml(xmlParaProcessar, 'nProt') || null;
      const dhRegEvento = extrairTextoDoXml(xmlParaProcessar, 'dhRegEvento') || null;
      const tpEvento = extrairTextoDoXml(xmlParaProcessar, 'tpEvento') || null;
      const xEvento = extrairTextoDoXml(xmlParaProcessar, 'xEvento') || null;

      // Extrair CNPJ do emitente
      const emitCnpj = extrairCnpjDoXml(xmlParaProcessar, 'CNPJ') || null;

      // Extrair série (pode estar no XML original ou no evento)
      const serie =
        extrairTextoDoXml(xmlParaProcessar, 'serie') || extrairTextoDoXml(cte.xml, 'serie') || null;

      return {
        id: cte.id,
        xml: cte.xml,
        event_xml: cte.event_xml,
        chCTe,
        nProt,
        dhRegEvento,
        tpEvento,
        xEvento,
        emitCnpj,
        authorization_number: cte.authorization_number,
        status: cte.status,
        serie,
      };
    });
  } catch (error: any) {
    logger.error({ error: error.message }, 'Erro ao listar CT-e cancelados não processados');
    return [];
  }
};

/**
 * Processa CT-e cancelados
 * Atualiza GTCCONHE com InConhecimento = 1 e DtCancelamento
 */
export async function processPendingCteCancelados(prisma: PrismaClient): Promise<void> {
  try {
    const ctesCancelados = await listarCtesCanceladosNaoProcessados(prisma);

    if (ctesCancelados.length === 0) {
      return;
    }

    logger.info({ count: ctesCancelados.length }, 'Processando CT-e cancelados');

    for (const cteCancelado of ctesCancelados) {
      const eventId = `cte-cancelado-${cteCancelado.id}`;
      const processingStartTime = Date.now();

      try {
        logger.info(
          { cteCanceladoId: cteCancelado.id },
          'Iniciando processamento de CT-e cancelado',
        );

        // Criar ou atualizar evento WebhookEvent
        await prisma.webhookEvent.upsert({
          where: { id: eventId },
          create: {
            id: eventId,
            source: 'worker/cte-cancelado',
            status: 'processing',
            tipoIntegracao: 'Worker',
            metadata: JSON.stringify({
              cteCanceladoId: cteCancelado.id,
              etapa: 'processamento_cancelamento',
            }),
          },
          update: {
            status: 'processing',
            tipoIntegracao: 'Worker',
            metadata: JSON.stringify({
              cteCanceladoId: cteCancelado.id,
              etapa: 'processamento_cancelamento',
            }),
          },
        });

        // Extrair CNPJ do emitente do XML do evento de cancelamento
        // O XML do cancelamento pode estar em event_xml ou xml
        const xmlParaProcessar = cteCancelado.event_xml || cteCancelado.xml;

        // Tentar extrair CNPJ do XML do evento usando múltiplas estratégias
        let emitCnpj = cteCancelado.emitCnpj;

        if (!emitCnpj) {
          // Estratégia 1: Tentar extrair da tag CNPJ direta no XML do evento
          emitCnpj = extrairCnpjDoXml(xmlParaProcessar, 'CNPJ');
        }

        if (!emitCnpj) {
          // Estratégia 2: Tentar extrair da tag infEvento > CNPJ
          emitCnpj = extrairCnpjDoXml(xmlParaProcessar, 'infEvento');
        }

        if (!emitCnpj) {
          // Estratégia 3: Tentar extrair da tag emit no XML original do CT-e
          emitCnpj = extrairCnpjDoXml(cteCancelado.xml, 'emit');
        }

        if (!emitCnpj) {
          // Estratégia 4: Tentar extrair da tag emit no XML do evento
          emitCnpj = extrairCnpjDoXml(xmlParaProcessar, 'emit');
        }

        if (!emitCnpj) {
          // Estratégia 5: Tentar buscar diretamente no XML usando regex mais amplo
          const cnpjMatch = xmlParaProcessar.match(/<CNPJ>(\d{14})<\/CNPJ>/);
          if (cnpjMatch && cnpjMatch[1]) {
            emitCnpj = cnpjMatch[1].trim();
          }
        }

        if (!emitCnpj) {
          // Estratégia 6: Tentar buscar no XML original usando regex mais amplo
          const cnpjMatchOriginal = cteCancelado.xml.match(/<CNPJ>(\d{14})<\/CNPJ>/);
          if (cnpjMatchOriginal && cnpjMatchOriginal[1]) {
            emitCnpj = cnpjMatchOriginal[1].trim();
          }
        }

        if (!emitCnpj) {
          logger.error(
            {
              cteCanceladoId: cteCancelado.id,
              hasEventXml: !!cteCancelado.event_xml,
              hasXml: !!cteCancelado.xml,
              xmlPreview: xmlParaProcessar.substring(0, 500),
            },
            'CNPJ do emitente não encontrado no XML de cancelamento após todas as tentativas',
          );
          throw new Error('CNPJ do emitente não encontrado no XML de cancelamento');
        }

        logger.debug(
          { cteCanceladoId: cteCancelado.id, emitCnpj },
          'CNPJ do emitente extraído com sucesso do XML de cancelamento',
        );

        // Obter código de empresa
        const cdEmpresa = await buildCdEmpresaFromCnpj(prisma, emitCnpj);
        logger.info(
          { cdEmpresa, emitCnpj, cteId: cteCancelado.id },
          'Código de empresa obtido para cancelamento',
        );

        // Processar cancelamento
        const resultado = await cancelarCte(prisma, xmlParaProcessar, cdEmpresa);

        const processingTimeMs = Date.now() - processingStartTime;

        if (resultado.success) {
          // Marcar CT-e como processado na tabela ctes
          try {
            await marcarCteProcessadoPorId(prisma, cteCancelado.id);
            logger.debug(
              { cteId: cteCancelado.id },
              'CT-e cancelado marcado como processado na tabela ctes',
            );
          } catch (updateError: any) {
            logger.warn(
              { error: updateError.message, cteId: cteCancelado.id },
              'Erro ao marcar CT-e cancelado como processado',
            );
          }

          // Atualizar WebhookEvent com sucesso
          await prisma.webhookEvent.update({
            where: { id: eventId },
            data: {
              status: 'processed',
              processedAt: new Date(),
              processingTimeMs,
              integrationTimeMs: processingTimeMs,
              integrationStatus: 'integrated',
              seniorId: resultado.nrSeqControle ? String(resultado.nrSeqControle) : null,
              metadata: JSON.stringify({
                cteCanceladoId: cteCancelado.id,
                nrSeqControle: resultado.nrSeqControle,
                etapa: 'cancelamento_concluido',
              }),
            },
          });

          logger.info(
            {
              cteCanceladoId: cteCancelado.id,
              nrSeqControle: resultado.nrSeqControle,
              processingTimeMs,
            },
            'CT-e cancelado processado com sucesso',
          );
        } else {
          // REGRA: Quando o cancelamento falhar, marcar como processed = 1 para evitar loop infinito
          // Isso evita que o sistema continue tentando processar o mesmo CT-e cancelado que falhou
          try {
            await marcarCteProcessadoPorId(prisma, cteCancelado.id);
            logger.info(
              { cteId: cteCancelado.id },
              'CT-e cancelado marcado como processado após falha (evitando loop)',
            );
          } catch (updateError: any) {
            logger.warn(
              { error: updateError.message, cteId: cteCancelado.id },
              'Erro ao marcar CT-e cancelado como processado após falha',
            );
          }

          // Atualizar WebhookEvent com erro
          await prisma.webhookEvent.update({
            where: { id: eventId },
            data: {
              status: 'failed',
              processedAt: new Date(),
              errorMessage: resultado.error || 'Erro desconhecido',
              processingTimeMs,
              integrationStatus: 'failed',
              metadata: JSON.stringify({
                cteCanceladoId: cteCancelado.id,
                etapa: 'cancelamento_erro',
              }),
            },
          });

          logger.error(
            {
              cteCanceladoId: cteCancelado.id,
              error: resultado.error,
              processingTimeMs,
            },
            'Erro ao processar CT-e cancelado - marcado como processado para evitar loop',
          );
        }
      } catch (error: any) {
        // REGRA: Quando ocorrer erro inesperado, também marcar como processed = 1 para evitar loop infinito
        try {
          await marcarCteProcessadoPorId(prisma, cteCancelado.id);
          logger.info(
            { cteId: cteCancelado.id },
            'CT-e cancelado marcado como processado após erro inesperado (evitando loop)',
          );
        } catch (updateError: any) {
          logger.warn(
            { error: updateError.message, cteId: cteCancelado.id },
            'Erro ao marcar CT-e cancelado como processado após erro inesperado',
          );
        }

        // Atualizar WebhookEvent com erro
        const processingTimeMs = Date.now() - processingStartTime;
        try {
          await prisma.webhookEvent.update({
            where: { id: eventId },
            data: {
              status: 'failed',
              processedAt: new Date(),
              errorMessage: error.message || 'Erro desconhecido',
              processingTimeMs,
              integrationStatus: 'failed',
              metadata: JSON.stringify({
                cteCanceladoId: cteCancelado.id,
                etapa: 'erro_inesperado',
                erro: error.message,
              }),
            },
          });
        } catch (webhookError: any) {
          logger.warn(
            { error: webhookError.message, cteId: cteCancelado.id },
            'Erro ao atualizar WebhookEvent após erro inesperado',
          );
        }

        logger.error(
          {
            error: error.message,
            stack: error.stack,
            cteCanceladoId: cteCancelado.id,
          },
          'Erro ao processar CT-e cancelado - marcado como processado para evitar loop',
        );
      }
    }
  } catch (error: any) {
    logger.error({ error: error.message, stack: error.stack }, 'Erro ao processar CT-e cancelados');
  }
}
