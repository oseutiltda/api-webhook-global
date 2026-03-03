import { Router } from 'express';
import { PrismaClient } from '@prisma/client';
import { logger } from '../utils/logger';

const router = Router();
const prisma = new PrismaClient();

const isMissingWebhookEventTable = (error: any): boolean => {
  if (error?.code !== 'P2021') return false;
  const table = String(error?.meta?.table || '');
  return table.includes('WebhookEvent');
};

const getEventType = (source: string): string => {
  const lowerSource = source.toLowerCase();
  if (lowerSource.includes('nfse') || lowerSource.includes('/api/nfse')) {
    return 'NFSe';
  }
  if (
    lowerSource.includes('ciot') ||
    lowerSource.includes('/api/ciot') ||
    lowerSource.includes('/webhooks/ctrb/ciot')
  ) {
    return 'CIOT';
  }
  if (lowerSource.includes('cte') || lowerSource.includes('/webhooks/cte')) {
    return 'CT-e';
  }
  if (
    lowerSource.includes('pessoa') ||
    lowerSource.includes('/api/pessoa') ||
    lowerSource.includes('/webhooks/pessoa')
  ) {
    return 'Pessoa';
  }
  if (lowerSource.includes('faturas/pagar') || lowerSource.includes('/api/contaspagar')) {
    return 'Contas a Pagar';
  }
  if (lowerSource.includes('faturas/receber') || lowerSource.includes('/api/contasreceber')) {
    return 'Contas a Receber';
  }
  return 'Outros';
};

const parseMetadata = (metadata: string | null): Record<string, unknown> | null => {
  if (!metadata) return null;
  try {
    const parsed = JSON.parse(metadata);
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
      return parsed as Record<string, unknown>;
    }
    return null;
  } catch {
    return null;
  }
};

const normalizeValue = (value: unknown): string | null => {
  if (value === null || value === undefined) return null;
  const normalized = String(value).trim();
  return normalized.length > 0 ? normalized : null;
};

const isCiotSource = (source: string): boolean => {
  const lower = source.toLowerCase();
  return lower.includes('ciot');
};

const isNfseSource = (source: string): boolean => {
  const lower = source.toLowerCase();
  return lower.includes('nfse');
};

const isCteSource = (source: string): boolean => {
  const lower = source.toLowerCase();
  return lower.includes('cte') || lower.includes('ct-e');
};

const isPessoaSource = (source: string): boolean => {
  const lower = source.toLowerCase();
  return lower.includes('pessoa');
};

const extractUniqueRecordKeys = (source: string, metadata: string | null) => {
  const parsedMetadata = parseMetadata(metadata);
  if (!parsedMetadata) {
    return { ciot: null, nfse: null, cte: null, pessoa: null };
  }

  const ciot = isCiotSource(source) ? normalizeValue(parsedMetadata.nrciot) : null;
  const nfse = isNfseSource(source)
    ? normalizeValue(parsedMetadata.numeroNfse) ||
      normalizeValue(parsedMetadata.numeroDocumento) ||
      normalizeValue(parsedMetadata.nfseId)
    : null;
  const cte = isCteSource(source) ? normalizeValue(parsedMetadata.chCTe) : null;
  const pessoa = isPessoaSource(source)
    ? normalizeValue(parsedMetadata.codPessoa) || normalizeValue(parsedMetadata.codPessoaEsl)
    : null;

  return { ciot, nfse, cte, pessoa };
};

const getPeriodKey = (receivedAt: Date, period: string): string => {
  if (period === 'diario') {
    return String(receivedAt.getHours()).padStart(2, '0');
  }
  const year = receivedAt.getFullYear();
  const month = String(receivedAt.getMonth() + 1).padStart(2, '0');
  const day = String(receivedAt.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
};

const formatPeriodLabel = (periodKey: string, period: string): string => {
  if (period === 'diario') {
    return `${periodKey}:00`;
  }
  const [year, month, day] = periodKey.split('-');
  if (!year || !month || !day) return periodKey;
  return `${day}/${month}`;
};

// Health check dos serviços
router.get('/health', async (_req, res) => {
  const healthStatus = {
    timestamp: new Date().toISOString(),
    services: {
      backend: {
        status: 'online' as 'online' | 'offline',
        lastCheck: new Date().toISOString(),
        uptime: process.uptime(),
      },
      database: {
        status: 'offline' as 'online' | 'offline',
        lastCheck: new Date().toISOString(),
        responseTimeMs: 0,
        error: null as string | null,
      },
      worker: {
        status: 'unknown' as 'online' | 'offline' | 'unknown',
        lastCheck: new Date().toISOString(),
        lastActivity: null as string | null,
        responseTimeMs: 0,
        error: null as string | null,
      },
    },
  };

  // Verificar banco de dados
  const dbStartTime = Date.now();
  try {
    await prisma.$queryRaw`SELECT 1 as health_check`;
    healthStatus.services.database.status = 'online';
    healthStatus.services.database.responseTimeMs = Date.now() - dbStartTime;
  } catch (error: any) {
    healthStatus.services.database.status = 'offline';
    healthStatus.services.database.responseTimeMs = Date.now() - dbStartTime;
    healthStatus.services.database.error = error?.message || 'Erro ao conectar ao banco de dados';
    logger.error({ error: error.message }, 'Erro ao verificar saúde do banco de dados');
  }

  // Verificar worker (verificar se processou eventos recentemente - últimos 5 minutos)
  const workerStartTime = Date.now();
  try {
    const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000);
    const recentEvents = await prisma.webhookEvent.findFirst({
      where: {
        OR: [{ processedAt: { gte: fiveMinutesAgo } }, { receivedAt: { gte: fiveMinutesAgo } }],
      },
      orderBy: { receivedAt: 'desc' },
      select: {
        processedAt: true,
        receivedAt: true,
        source: true,
      },
    });

    if (recentEvents) {
      healthStatus.services.worker.status = 'online';
      healthStatus.services.worker.lastActivity = recentEvents.processedAt
        ? recentEvents.processedAt.toISOString()
        : recentEvents.receivedAt.toISOString();
    } else {
      // Verificar se há eventos pendentes (worker pode estar processando)
      const pendingCount = await prisma.webhookEvent.count({
        where: {
          status: { in: ['pending', 'processing'] },
        },
      });

      if (pendingCount > 0) {
        healthStatus.services.worker.status = 'online'; // Há trabalho pendente, worker pode estar processando
      } else {
        healthStatus.services.worker.status = 'unknown'; // Sem atividade recente, mas pode estar ocioso
      }
    }
    healthStatus.services.worker.responseTimeMs = Date.now() - workerStartTime;
  } catch (error: any) {
    if (isMissingWebhookEventTable(error)) {
      healthStatus.services.worker.status = 'unknown';
      healthStatus.services.worker.error = null;
      healthStatus.services.worker.responseTimeMs = Date.now() - workerStartTime;
      logger.info('Tabela WebhookEvent não existe ainda; worker health em modo desconhecido');
    } else {
      healthStatus.services.worker.status = 'offline';
      healthStatus.services.worker.responseTimeMs = Date.now() - workerStartTime;
      healthStatus.services.worker.error = error?.message || 'Erro ao verificar status do worker';
      logger.error({ error: error.message }, 'Erro ao verificar saúde do worker');
    }
  }

  // Backend sempre online se chegou aqui
  healthStatus.services.backend.status = 'online';

  const allOnline =
    healthStatus.services.backend.status === 'online' &&
    healthStatus.services.database.status === 'online' &&
    (healthStatus.services.worker.status === 'online' ||
      healthStatus.services.worker.status === 'unknown');

  res.status(allOnline ? 200 : 503).json(healthStatus);
});

// Estatísticas gerais do worker
router.get('/worker/stats', async (_req, res) => {
  try {
    // Verificar se a tabela WebhookEvent existe, se não existir retornar dados vazios
    let total = 0;
    try {
      total = await prisma.webhookEvent.count();
    } catch (error: any) {
      // Se a tabela não existir (P2021), retornar dados vazios
      if (isMissingWebhookEventTable(error)) {
        logger.debug('Tabela WebhookEvent não existe ainda');
        return res.json({
          total: 0,
          pending: 0,
          processing: 0,
          processed: 0,
          failed: 0,
          processedLast24h: 0,
          successRate: 0,
          eventsByType: [],
        });
      }
      throw error;
    }

    // Tentar contar por status, mas tratar caso os campos não existam
    let pending = 0;
    let processing = 0;
    let processed = 0;
    let failed = 0;

    try {
      [pending, processing, processed, failed] = await Promise.all([
        prisma.webhookEvent.count({ where: { status: 'pending' } }).catch(() => 0),
        prisma.webhookEvent.count({ where: { status: 'processing' } }).catch(() => 0),
        prisma.webhookEvent.count({ where: { status: 'processed' } }).catch(() => 0),
        prisma.webhookEvent.count({ where: { status: 'failed' } }).catch(() => 0),
      ]);
    } catch {
      // Se status não existir, todos são pendentes
      pending = total;
    }

    // Eventos processados nas últimas 24h
    const last24h = new Date();
    last24h.setHours(last24h.getHours() - 24);
    let processedLast24h = 0;
    let totalLast24h = 0;
    let failedLast24h = 0;
    try {
      processedLast24h = await prisma.webhookEvent.count({
        where: {
          status: 'processed',
          processedAt: { gte: last24h },
        },
      });

      // Total de eventos nas últimas 24h
      totalLast24h = await prisma.webhookEvent.count({
        where: { receivedAt: { gte: last24h } },
      });

      // Falhas nas últimas 24h
      failedLast24h = await prisma.webhookEvent.count({
        where: {
          status: 'failed',
          receivedAt: { gte: last24h },
        },
      });
    } catch {
      // Campo não existe ainda
      processedLast24h = 0;
      totalLast24h = 0;
      failedLast24h = 0;
    }

    // Taxa de sucesso (últimas 24h)
    const successRate = totalLast24h > 0 ? (processedLast24h / totalLast24h) * 100 : 0;

    // Eventos por tipo (últimas 24h) - Agrupar por tipo principal (NFSe, CIOT, CT-e, etc.)
    let eventsByType: Array<{ source: string; count: number }> = [];
    try {
      const grouped = await prisma.webhookEvent.groupBy({
        by: ['source'],
        where: { receivedAt: { gte: last24h } },
        _count: true,
      });

      // Agrupar por tipo principal
      const typeMap = new Map<string, number>();
      grouped.forEach((e) => {
        const eventType = getEventType(e.source);
        const currentCount = typeMap.get(eventType) || 0;
        typeMap.set(eventType, currentCount + e._count);
      });

      // Converter para array e ordenar por count (decrescente)
      eventsByType = Array.from(typeMap.entries())
        .map(([source, count]) => ({ source, count }))
        .sort((a, b) => b.count - a.count);
    } catch (e: any) {
      logger.warn({ error: e.message }, 'Erro ao agrupar eventos por tipo');
      eventsByType = [];
    }

    // Estatísticas de integração (últimas 24h)
    const integrationStats = {
      integrated: 0,
      pending: 0,
      failed: 0,
      skipped: 0,
    };
    try {
      const integrationGrouped = await prisma.webhookEvent.groupBy({
        by: ['integrationStatus'],
        where: {
          receivedAt: { gte: last24h },
          integrationStatus: { not: null },
        },
        _count: true,
      });
      integrationGrouped.forEach((g) => {
        const status = g.integrationStatus || 'pending';
        if (status === 'integrated') integrationStats.integrated = g._count;
        else if (status === 'pending') integrationStats.pending = g._count;
        else if (status === 'failed') integrationStats.failed = g._count;
        else if (status === 'skipped') integrationStats.skipped = g._count;
      });
    } catch (e: any) {
      logger.warn({ error: e.message }, 'Erro ao buscar estatísticas de integração');
    }

    // Contagem de registros únicos por tipo (últimas 24h)
    const uniqueRecords = {
      ciot: { total: 0, unique: 0 },
      nfse: { total: 0, unique: 0 },
      cte: { total: 0, unique: 0 },
      pessoa: { total: 0, unique: 0 },
    };
    try {
      const eventsForUnique = await prisma.webhookEvent.findMany({
        where: { receivedAt: { gte: last24h } },
        select: { source: true, metadata: true },
      });
      const ciotSet = new Set<string>();
      const nfseSet = new Set<string>();
      const cteSet = new Set<string>();
      const pessoaSet = new Set<string>();

      for (const event of eventsForUnique) {
        if (isCiotSource(event.source)) uniqueRecords.ciot.total += 1;
        if (isNfseSource(event.source)) uniqueRecords.nfse.total += 1;
        if (isCteSource(event.source)) uniqueRecords.cte.total += 1;
        if (isPessoaSource(event.source)) uniqueRecords.pessoa.total += 1;

        const uniqueKeys = extractUniqueRecordKeys(event.source, event.metadata);
        if (uniqueKeys.ciot) ciotSet.add(uniqueKeys.ciot);
        if (uniqueKeys.nfse) nfseSet.add(uniqueKeys.nfse);
        if (uniqueKeys.cte) cteSet.add(uniqueKeys.cte);
        if (uniqueKeys.pessoa) pessoaSet.add(uniqueKeys.pessoa);
      }

      uniqueRecords.ciot.unique = ciotSet.size;
      uniqueRecords.nfse.unique = nfseSet.size;
      uniqueRecords.cte.unique = cteSet.size;
      uniqueRecords.pessoa.unique = pessoaSet.size;
    } catch (e: any) {
      logger.warn({ error: e.message, stack: e.stack }, 'Erro ao buscar registros únicos');
    }

    // Estatísticas por tipoIntegracao (últimas 24h)
    let eventsByTipoIntegracao: Array<{ tipoIntegracao: string; count: number }> = [];
    try {
      const tipoIntegracaoGrouped = await prisma.webhookEvent.groupBy({
        by: ['tipoIntegracao'],
        where: {
          receivedAt: { gte: last24h },
          tipoIntegracao: { not: null },
        },
        _count: true,
      });
      eventsByTipoIntegracao = tipoIntegracaoGrouped.map((g) => ({
        tipoIntegracao: g.tipoIntegracao || 'Não especificado',
        count: g._count,
      }));
    } catch (e: any) {
      logger.warn({ error: e.message }, 'Erro ao agrupar eventos por tipoIntegracao');
      eventsByTipoIntegracao = [];
    }

    res.json({
      total,
      totalLast24h, // Total de eventos nas últimas 24h
      pending,
      processing,
      processed,
      failed,
      failedLast24h, // Falhas nas últimas 24h
      processedLast24h,
      successRate: Math.round(successRate * 100) / 100,
      eventsByType,
      integrationStats,
      uniqueRecords,
      eventsByTipoIntegracao,
    });
  } catch (error: any) {
    logger.error(
      { error: error.message, stack: error.stack },
      'Erro ao buscar estatísticas do worker',
    );
    res.status(500).json({
      error: error.message || 'Erro ao buscar estatísticas',
      details: process.env.NODE_ENV === 'development' ? error.stack : undefined,
    });
  }
});

// Lista de eventos recentes
router.get('/worker/events', async (req, res) => {
  try {
    const page = Number(req.query.page) || 1;
    const limit = Number(req.query.limit) || 50;
    const status = req.query.status as string | undefined;
    const source = req.query.source as string | undefined;
    const tipoIntegracao = req.query.tipoIntegracao as string | undefined;

    const where: any = {};
    // Só adicionar status se for fornecido e não for 'all'
    // Se o campo não existir no banco, a query falhará, então vamos tentar sem ele primeiro
    if (status && status !== 'all') {
      where.status = status;
    }
    if (source) where.source = { contains: source };
    if (tipoIntegracao && tipoIntegracao !== 'all') {
      where.tipoIntegracao = tipoIntegracao;
    }

    let events: any[] = [];
    let total = 0;

    try {
      [events, total] = await Promise.all([
        prisma.webhookEvent.findMany({
          where,
          orderBy: { receivedAt: 'desc' },
          skip: (page - 1) * limit,
          take: limit,
          select: {
            id: true,
            source: true,
            receivedAt: true,
            status: true,
            processedAt: true,
            errorMessage: true,
            retryCount: true,
            integrationStatus: true,
            processingTimeMs: true,
            integrationTimeMs: true,
            seniorId: true,
            metadata: true,
            tipoIntegracao: true,
          },
        }),
        prisma.webhookEvent.count({ where }),
      ]);
    } catch (e: any) {
      // Se a tabela não existir (P2021), retornar dados vazios
      if (isMissingWebhookEventTable(e)) {
        logger.debug('Tabela WebhookEvent não existe ainda');
        return res.json({
          events: [],
          pagination: {
            page,
            limit,
            total: 0,
            totalPages: 0,
          },
        });
      }
      // Se falhar (provavelmente campo status não existe), tentar sem filtro de status
      if (where.status) {
        logger.warn({ error: e.message }, 'Campo status não disponível, removendo filtro');
        delete where.status;
        try {
          [events, total] = await Promise.all([
            prisma.webhookEvent.findMany({
              where,
              orderBy: { receivedAt: 'desc' },
              skip: (page - 1) * limit,
              take: limit,
              select: {
                id: true,
                source: true,
                receivedAt: true,
                status: true,
                processedAt: true,
                errorMessage: true,
                retryCount: true,
                integrationStatus: true,
                processingTimeMs: true,
                integrationTimeMs: true,
                seniorId: true,
                metadata: true,
                tipoIntegracao: true,
              },
            }),
            prisma.webhookEvent.count({ where }),
          ]);
        } catch (e2: any) {
          // Se a tabela não existir, retornar vazio
          if (isMissingWebhookEventTable(e2)) {
            logger.debug('Tabela WebhookEvent não existe ainda');
            return res.json({
              events: [],
              pagination: {
                page,
                limit,
                total: 0,
                totalPages: 0,
              },
            });
          }
          throw e2;
        }
      } else {
        throw e;
      }
    }

    // Garantir que todos os campos estejam presentes, incluindo metadata
    const eventsWithMetadata = events.map((event: any) => ({
      ...event,
      metadata: event.metadata ?? null, // Garantir que metadata seja sempre incluído
    }));

    res.json({
      events: eventsWithMetadata,
      pagination: {
        page,
        limit,
        total,
        totalPages: Math.ceil(total / limit),
      },
    });
  } catch (error: any) {
    logger.error(
      { error: error.message, stack: error.stack },
      'Erro ao buscar estatísticas do worker',
    );
    res.status(500).json({
      error: error.message || 'Erro ao buscar estatísticas',
      details: process.env.NODE_ENV === 'development' ? error.stack : undefined,
    });
  }
});

// Detalhes de um evento específico
router.get('/worker/events/:id', async (req, res) => {
  try {
    const event = await prisma.webhookEvent.findUnique({
      where: { id: req.params.id },
    });

    if (!event) {
      return res.status(404).json({ error: 'Evento não encontrado' });
    }

    res.json(event);
  } catch (error: any) {
    logger.error({ error: error.message, stack: error.stack }, 'Erro ao buscar evento');
    res.status(500).json({
      error: error.message || 'Erro ao buscar evento',
      details: process.env.NODE_ENV === 'development' ? error.stack : undefined,
    });
  }
});

// Eventos com falha que precisam atenção
router.get('/worker/failures', async (_req, res) => {
  try {
    let failures: any[] = [];
    try {
      failures = await prisma.webhookEvent.findMany({
        where: {
          status: 'failed',
          retryCount: { gte: 3 },
        },
        orderBy: { receivedAt: 'desc' },
        take: 20,
        select: {
          id: true,
          source: true,
          receivedAt: true,
          status: true,
          processedAt: true,
          errorMessage: true,
          retryCount: true,
          integrationStatus: true,
          processingTimeMs: true,
          integrationTimeMs: true,
          seniorId: true,
          metadata: true,
          tipoIntegracao: true,
        },
      });
    } catch (e: any) {
      // Se campos não existirem, retornar array vazio
      logger.warn({ error: e.message }, 'Campos de status não disponíveis');
      failures = [];
    }

    // Garantir que todos os campos estejam presentes, incluindo metadata
    const failuresWithMetadata = failures.map((failure: any) => ({
      ...failure,
      metadata: failure.metadata ?? null, // Garantir que metadata seja sempre incluído
    }));

    res.json(failuresWithMetadata);
  } catch (error: any) {
    logger.error({ error: error.message, stack: error.stack }, 'Erro ao buscar falhas do worker');
    res.status(500).json({
      error: error.message || 'Erro ao buscar falhas',
      details: process.env.NODE_ENV === 'development' ? error.stack : undefined,
    });
  }
});

// Métricas de performance (últimas 24h)
router.get('/worker/performance', async (_req, res) => {
  try {
    const last24h = new Date();
    last24h.setHours(last24h.getHours() - 24);

    let processedEvents: any[] = [];
    try {
      processedEvents = await prisma.webhookEvent.findMany({
        where: {
          status: 'processed',
          processedAt: { gte: last24h },
        },
        select: {
          receivedAt: true,
          processedAt: true,
        },
      });
    } catch (e: any) {
      // Se a tabela não existir (P2021), retornar dados vazios
      if (isMissingWebhookEventTable(e)) {
        logger.debug('Tabela WebhookEvent não existe ainda');
        return res.json({
          avgProcessingTimeSeconds: 0,
          avgProcessingTimeMs: 0,
          totalProcessed: 0,
          hourlyStats: [],
        });
      }
      // Campos não existem ainda
      logger.warn({ error: e.message }, 'Campos de processamento não disponíveis');
      processedEvents = [];
    }

    const processingTimes = processedEvents
      .filter((e) => e.processedAt)
      .map((e) => {
        const received = new Date(e.receivedAt).getTime();
        const processed = new Date(e.processedAt!).getTime();
        return processed - received;
      });

    const avgProcessingTime =
      processingTimes.length > 0
        ? processingTimes.reduce((a, b) => a + b, 0) / processingTimes.length
        : 0;

    // Eventos por hora (últimas 24h) - usando Prisma
    let allEvents: any[] = [];
    try {
      allEvents = await prisma.webhookEvent.findMany({
        where: {
          receivedAt: { gte: last24h },
        },
        select: {
          receivedAt: true,
        },
      });
    } catch (e: any) {
      // Se a tabela não existir (P2021), retornar dados vazios
      if (isMissingWebhookEventTable(e)) {
        logger.debug('Tabela WebhookEvent não existe ainda');
        return res.json({
          avgProcessingTimeSeconds: 0,
          avgProcessingTimeMs: 0,
          totalProcessed: 0,
          hourlyStats: [],
        });
      }
      // Se for outro erro, propagar
      throw e;
    }

    // Agrupar por hora manualmente
    const hourlyMap = new Map<number, number>();
    allEvents.forEach((event) => {
      const hour = new Date(event.receivedAt).getHours();
      hourlyMap.set(hour, (hourlyMap.get(hour) || 0) + 1);
    });

    const hourlyStats = Array.from(hourlyMap.entries())
      .map(([hour, count]) => ({ hour, count }))
      .sort((a, b) => a.hour - b.hour);

    res.json({
      avgProcessingTimeMs: Math.round(avgProcessingTime),
      avgProcessingTimeSeconds: Math.round((avgProcessingTime / 1000) * 100) / 100,
      totalProcessed: processedEvents.length,
      hourlyStats: hourlyStats.map((h) => ({
        hour: h.hour,
        count: Number(h.count),
      })),
    });
  } catch (error: any) {
    logger.error(
      { error: error.message, stack: error.stack },
      'Erro ao buscar performance do worker',
    );
    res.status(500).json({
      error: error.message || 'Erro ao buscar performance',
      details: process.env.NODE_ENV === 'development' ? error.stack : undefined,
    });
  }
});

// Dashboard de Produtividade - Dados agregados por período
router.get('/worker/productivity', async (req, res) => {
  try {
    const period = (req.query.period as string) || 'mensal'; // diario, semanal, mensal
    const now = new Date();

    let startDate: Date;

    switch (period) {
      case 'diario':
        // Últimas 24 horas, agrupado por hora
        startDate = new Date(now.getTime() - 24 * 60 * 60 * 1000);
        break;
      case 'semanal':
        // Últimos 7 dias, agrupado por dia
        startDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
        break;
      case 'mensal':
        // Últimos 30 dias, agrupado por dia
        startDate = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
        break;
      default:
        startDate = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
    }

    try {
      logger.debug(
        { period, startDate: startDate.toISOString() },
        'Buscando dados de produtividade',
      );

      // Verificar se há dados antes de executar queries
      let totalEventsCheck = 0;
      try {
        totalEventsCheck = await prisma.webhookEvent.count({
          where: {
            receivedAt: { gte: startDate },
          },
        });
      } catch (e: any) {
        logger.warn({ error: e.message }, 'Erro ao contar eventos');
      }

      if (totalEventsCheck === 0) {
        logger.debug(
          { period, startDate: startDate.toISOString() },
          'Nenhum evento encontrado no período',
        );
        return res.json({
          period,
          summary: {
            totalEvents: 0,
            totalProcessed: 0,
            totalFailed: 0,
            successRate: 0,
            avgProcessingTimeMs: 0,
            avgProcessingTimeSeconds: 0,
          },
          byPeriod: [],
          byType: [],
          byTipoIntegracao: [],
        });
      }

      logger.debug(
        { period, totalEventsCheck },
        `Encontrados ${totalEventsCheck} eventos no período`,
      );

      const events = await prisma.webhookEvent.findMany({
        where: { receivedAt: { gte: startDate } },
        select: {
          source: true,
          status: true,
          processingTimeMs: true,
          integrationStatus: true,
          tipoIntegracao: true,
          metadata: true,
          receivedAt: true,
        },
      });

      const byPeriodMap = new Map<
        string,
        {
          total: number;
          processados: number;
          falhas: number;
          pendentes: number;
          processando: number;
          integrados: number;
          integracaoFalhas: number;
          webApi: number;
          worker: number;
          tempoTotalMs: number;
          tempoCount: number;
          ciotTotal: number;
          nfseTotal: number;
          cteTotal: number;
          pessoaTotal: number;
          ciotSet: Set<string>;
          nfseSet: Set<string>;
          cteSet: Set<string>;
          pessoaSet: Set<string>;
        }
      >();

      const byTypeMap = new Map<
        string,
        {
          total: number;
          processados: number;
          falhas: number;
          tempoTotalMs: number;
          tempoCount: number;
        }
      >();
      const byTipoIntegracaoMap = new Map<
        string,
        {
          total: number;
          processados: number;
          falhas: number;
          tempoTotalMs: number;
          tempoCount: number;
        }
      >();

      for (const event of events) {
        const periodKey = getPeriodKey(event.receivedAt, period);
        if (!byPeriodMap.has(periodKey)) {
          byPeriodMap.set(periodKey, {
            total: 0,
            processados: 0,
            falhas: 0,
            pendentes: 0,
            processando: 0,
            integrados: 0,
            integracaoFalhas: 0,
            webApi: 0,
            worker: 0,
            tempoTotalMs: 0,
            tempoCount: 0,
            ciotTotal: 0,
            nfseTotal: 0,
            cteTotal: 0,
            pessoaTotal: 0,
            ciotSet: new Set<string>(),
            nfseSet: new Set<string>(),
            cteSet: new Set<string>(),
            pessoaSet: new Set<string>(),
          });
        }

        const periodAgg = byPeriodMap.get(periodKey)!;
        periodAgg.total += 1;
        if (event.status === 'processed') periodAgg.processados += 1;
        if (event.status === 'failed') periodAgg.falhas += 1;
        if (event.status === 'pending') periodAgg.pendentes += 1;
        if (event.status === 'processing') periodAgg.processando += 1;
        if (event.integrationStatus === 'integrated') periodAgg.integrados += 1;
        if (event.integrationStatus === 'failed') periodAgg.integracaoFalhas += 1;
        if (event.tipoIntegracao === 'Web API') periodAgg.webApi += 1;
        if (event.tipoIntegracao === 'Worker') periodAgg.worker += 1;

        if (event.processingTimeMs !== null) {
          periodAgg.tempoTotalMs += event.processingTimeMs;
          periodAgg.tempoCount += 1;
        }

        if (isCiotSource(event.source)) periodAgg.ciotTotal += 1;
        if (isNfseSource(event.source)) periodAgg.nfseTotal += 1;
        if (isCteSource(event.source)) periodAgg.cteTotal += 1;
        if (isPessoaSource(event.source)) periodAgg.pessoaTotal += 1;

        const uniqueKeys = extractUniqueRecordKeys(event.source, event.metadata);
        if (uniqueKeys.ciot) periodAgg.ciotSet.add(uniqueKeys.ciot);
        if (uniqueKeys.nfse) periodAgg.nfseSet.add(uniqueKeys.nfse);
        if (uniqueKeys.cte) periodAgg.cteSet.add(uniqueKeys.cte);
        if (uniqueKeys.pessoa) periodAgg.pessoaSet.add(uniqueKeys.pessoa);

        const sourceType = getEventType(event.source);
        if (!byTypeMap.has(sourceType)) {
          byTypeMap.set(sourceType, {
            total: 0,
            processados: 0,
            falhas: 0,
            tempoTotalMs: 0,
            tempoCount: 0,
          });
        }
        const typeAgg = byTypeMap.get(sourceType)!;
        typeAgg.total += 1;
        if (event.status === 'processed') typeAgg.processados += 1;
        if (event.status === 'failed') typeAgg.falhas += 1;
        if (event.processingTimeMs !== null) {
          typeAgg.tempoTotalMs += event.processingTimeMs;
          typeAgg.tempoCount += 1;
        }

        if (event.tipoIntegracao) {
          if (!byTipoIntegracaoMap.has(event.tipoIntegracao)) {
            byTipoIntegracaoMap.set(event.tipoIntegracao, {
              total: 0,
              processados: 0,
              falhas: 0,
              tempoTotalMs: 0,
              tempoCount: 0,
            });
          }
          const tipoAgg = byTipoIntegracaoMap.get(event.tipoIntegracao)!;
          tipoAgg.total += 1;
          if (event.status === 'processed') tipoAgg.processados += 1;
          if (event.status === 'failed') tipoAgg.falhas += 1;
          if (event.processingTimeMs !== null) {
            tipoAgg.tempoTotalMs += event.processingTimeMs;
            tipoAgg.tempoCount += 1;
          }
        }
      }

      // Calcular métricas agregadas
      let totalEvents = 0;
      let totalProcessed = 0;
      let totalFailed = 0;
      let totalProcessingMs = 0;
      let totalProcessingCount = 0;
      for (const periodData of byPeriodMap.values()) {
        totalEvents += periodData.total;
        totalProcessed += periodData.processados;
        totalFailed += periodData.falhas;
        totalProcessingMs += periodData.tempoTotalMs;
        totalProcessingCount += periodData.tempoCount;
      }
      const avgProcessingTime =
        totalProcessingCount > 0 ? totalProcessingMs / totalProcessingCount : 0;

      const successRate = totalEvents > 0 ? (totalProcessed / totalEvents) * 100 : 0;
      const sortedPeriodKeys = Array.from(byPeriodMap.keys()).sort((a, b) => {
        if (period === 'diario') return Number(a) - Number(b);
        return a.localeCompare(b);
      });

      const combinedData = sortedPeriodKeys.map((periodKey) => {
        const event = byPeriodMap.get(periodKey)!;
        return {
          periodo: formatPeriodLabel(periodKey, period),
          total: event.total,
          processados: event.processados,
          falhas: event.falhas,
          pendentes: event.pendentes,
          processando: event.processando,
          tempoMedioMs: event.tempoCount > 0 ? event.tempoTotalMs / event.tempoCount : null,
          taxaSucesso: event.total > 0 ? (event.processados / event.total) * 100 : 0,
          integrados: event.integrados,
          integracaoFalhas: event.integracaoFalhas,
          webApi: event.webApi,
          worker: event.worker,
          ciot: {
            unicos: event.ciotSet.size,
            total: event.ciotTotal,
          },
          nfse: {
            unicos: event.nfseSet.size,
            total: event.nfseTotal,
          },
          cte: {
            unicos: event.cteSet.size,
            total: event.cteTotal,
          },
          pessoa: {
            unicos: event.pessoaSet.size,
            total: event.pessoaTotal,
          },
        };
      });

      res.json({
        period,
        summary: {
          totalEvents,
          totalProcessed,
          totalFailed,
          successRate: Math.round(successRate * 100) / 100,
          avgProcessingTimeMs: Math.round(avgProcessingTime),
          avgProcessingTimeSeconds: Math.round((avgProcessingTime / 1000) * 100) / 100,
        },
        byPeriod: combinedData,
        byType: Array.from(byTypeMap.entries())
          .map(([source, data]) => ({
            source,
            total: data.total,
            processados: data.processados,
            falhas: data.falhas,
            tempoMedioMs: data.tempoCount > 0 ? data.tempoTotalMs / data.tempoCount : null,
            taxaSucesso: data.total > 0 ? (data.processados / data.total) * 100 : 0,
          }))
          .sort((a, b) => b.total - a.total),
        byTipoIntegracao: Array.from(byTipoIntegracaoMap.entries())
          .map(([tipoIntegracao, data]) => ({
            tipoIntegracao,
            total: data.total,
            processados: data.processados,
            falhas: data.falhas,
            tempoMedioMs: data.tempoCount > 0 ? data.tempoTotalMs / data.tempoCount : null,
            taxaSucesso: data.total > 0 ? (data.processados / data.total) * 100 : 0,
          }))
          .sort((a, b) => b.total - a.total),
      });
    } catch (e: any) {
      logger.warn({ error: e.message, stack: e.stack }, 'Erro ao buscar dados de produtividade');
      res.json({
        period,
        summary: {
          totalEvents: 0,
          totalProcessed: 0,
          totalFailed: 0,
          successRate: 0,
          avgProcessingTimeMs: 0,
          avgProcessingTimeSeconds: 0,
        },
        byPeriod: [],
        byType: [],
        byTipoIntegracao: [],
      });
    }
  } catch (error: any) {
    logger.error(
      { error: error.message, stack: error.stack },
      'Erro ao buscar produtividade do worker',
    );
    res.status(500).json({
      error: error.message || 'Erro ao buscar produtividade',
      details: process.env.NODE_ENV === 'development' ? error.stack : undefined,
    });
  }
});

export default router;
