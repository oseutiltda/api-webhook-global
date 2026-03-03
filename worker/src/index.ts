import 'dotenv/config';
import { PrismaClient } from '@prisma/client';
import type { WebhookEvent } from '@prisma/client';
import { env } from './config/env';
import { logger } from './utils/logger';
import { processEvent } from './services/processor';
import { processPendingNfse } from './services/nfseSync';
import { processPendingCiot } from './services/ciotSync';
import { processPendingCte, processPendingCteCancelados } from './services/cteSync';
import { processPendingContasPagar } from './services/contasPagarSync';
import { processPendingContasReceber } from './services/contasReceberSync';
import { processPendingContasReceberBaixa } from './services/contasReceberBaixaSync';

const prisma = new PrismaClient();
const INTERVAL_MS = Number(env.WORKER_INTERVAL_MS);
const BATCH_SIZE = Number(env.WORKER_BATCH_SIZE);
const MAX_RETRIES = Number(env.WORKER_MAX_RETRIES);
const ENABLE_WORKER = env.ENABLE_WORKER;
const IS_POSTGRES = env.DATABASE_URL.startsWith('postgresql://');
const ENABLE_SQLSERVER_LEGACY = env.ENABLE_SQLSERVER_LEGACY;

function isMissingWebhookEventTable(error: any): boolean {
  if (error?.code !== 'P2021') return false;
  const table = String(error?.meta?.table || '');
  return table.toLowerCase().includes('webhookevent');
}

// Serviços habilitados via variável de ambiente ENABLED_WORKER_SERVICES (ex: "NFSE,CIOT,CTE").
// Se não estiver definida ou vazia, considera todos os serviços habilitados (comportamento atual).
const enabledWorkerServices = (env.ENABLED_WORKER_SERVICES || '')
  .split(',')
  .map((s) => s.trim().toUpperCase())
  .filter((s) => s.length > 0);

function isServiceEnabled(service: string): boolean {
  if (enabledWorkerServices.length === 0) {
    // Sem configuração explícita -> todos habilitados (mantém comportamento atual)
    return true;
  }
  return enabledWorkerServices.includes(service.toUpperCase());
}

async function processBatch() {
  try {
    // Buscar eventos pendentes (limitado pelo batch size)
    // Se a tabela WebhookEvent não existir, apenas logar e retornar (não é erro crítico)
    let events: WebhookEvent[] = [];
    try {
      events = await prisma.webhookEvent.findMany({
        where: {
          status: 'pending',
          retryCount: { lt: MAX_RETRIES },
        },
        take: BATCH_SIZE,
        orderBy: { receivedAt: 'asc' },
      });
    } catch (error: any) {
      // Se a tabela não existir (P2021), apenas registrar e seguir para processamento de NFSe
      if (isMissingWebhookEventTable(error)) {
        logger.debug('Tabela WebhookEvent não existe ainda, aguardando criação');
        events = [];
      } else if (error?.code === 'P2024' || error?.code === 'P1017') {
        // Erro de connection pool timeout - aguardar e retornar vazio para próxima tentativa
        logger.warn(
          {
            error: error.message,
            code: error.code,
            meta: error.meta,
          },
          'Erro de conexão com banco de dados ao buscar eventos - aguardando próxima tentativa',
        );
        events = [];
      } else {
        // Se for outro erro, propagar
        throw error;
      }
    }

    if (events.length === 0) {
      logger.debug('Nenhum evento pendente encontrado');
    } else {
      logger.info({ count: events.length }, 'Processando lote de eventos');

      for (const event of events) {
        const processingStartTime = Date.now();
        try {
          // Marcar como processando
          await prisma.webhookEvent.update({
            where: { id: event.id },
            data: { status: 'processing' },
          });

          // Processar evento
          // Eventos de API que já foram processados pelo backend devem ser ignorados
          // (ex: /api/CTe/InserirCte já foi processado via stored procedure no backend)
          if (event.source.includes('/api/CTe/InserirCte')) {
            // CT-e inserido via API já foi processado pelo backend
            // Apenas marcar como processado se ainda estiver pendente
            if (event.status === 'pending') {
              await prisma.webhookEvent.update({
                where: { id: event.id },
                data: {
                  status: 'processed',
                  processedAt: new Date(),
                  processingTimeMs: Date.now() - processingStartTime,
                  integrationStatus: 'skipped',
                },
              });
              logger.debug(
                { eventId: event.id, source: event.source },
                'Evento de API CT-e já processado pelo backend, marcando como processado',
              );
            }
            continue;
          }

          // Pessoa inserido via API já foi processado pelo backend (stored procedure + integração Senior em background)
          // Não precisa de processamento adicional no worker
          if (
            event.source.includes('/api/Pessoa/InserirPessoa') ||
            event.source.includes('/api/Pessoa')
          ) {
            // Pessoa inserido via API já foi processado pelo backend
            // A integração com Senior é feita em background pelo próprio backend
            // Apenas marcar como processado se ainda estiver pendente
            if (event.status === 'pending') {
              await prisma.webhookEvent.update({
                where: { id: event.id },
                data: {
                  status: 'processed',
                  processedAt: new Date(),
                  processingTimeMs: Date.now() - processingStartTime,
                  integrationStatus: 'skipped',
                },
              });
              logger.debug(
                { eventId: event.id, source: event.source },
                'Evento de API Pessoa já processado pelo backend, marcando como processado',
              );
            }
            continue;
          }

          // Contas a Receber inserido via API já foi processado pelo backend
          // O worker processa via processPendingContasReceber que busca faturas pendentes do banco
          if (event.source.includes('/api/ContasReceber/InserirContasReceber')) {
            // Apenas marcar como processado se ainda estiver pendente
            if (event.status === 'pending') {
              await prisma.webhookEvent.update({
                where: { id: event.id },
                data: {
                  status: 'processed',
                  processedAt: new Date(),
                  processingTimeMs: Date.now() - processingStartTime,
                  integrationStatus: 'skipped',
                },
              });
              logger.debug(
                { eventId: event.id, source: event.source },
                'Evento de API ContasReceber já processado pelo backend, marcando como processado',
              );
            }
            continue;
          }

          // Eventos de Pessoa via API já foram processados pelo backend
          // O worker processará apenas se necessário (quando processEvent retornar sucesso)
          // Por enquanto, deixar o processEvent decidir se precisa processar

          const result = await processEvent(event.id, event.source);
          const processingTimeMs = Date.now() - processingStartTime;

          if (result.success) {
            // Marcar como processado
            await prisma.webhookEvent.update({
              where: { id: event.id },
              data: {
                status: 'processed',
                processedAt: new Date(),
                processingTimeMs,
                integrationStatus: result.integrationStatus || null,
                integrationTimeMs: result.integrationTimeMs || null,
                seniorId: result.seniorId || null,
                metadata: result.metadata
                  ? JSON.stringify(result.metadata).substring(0, 2000)
                  : null,
              },
            });

            logger.info(
              {
                eventId: event.id,
                recordsProcessed: result.recordsProcessed,
                processingTimeMs,
              },
              'Evento processado com sucesso',
            );
          } else {
            // Incrementar retry e marcar como failed se exceder limite
            const newRetryCount = event.retryCount + 1;
            const newStatus = newRetryCount >= MAX_RETRIES ? 'failed' : 'pending';

            await prisma.webhookEvent.update({
              where: { id: event.id },
              data: {
                status: newStatus,
                retryCount: newRetryCount,
                errorMessage: result.error?.substring(0, 1000) || null,
                processingTimeMs,
                integrationStatus: result.integrationStatus === 'failed' ? 'failed' : null,
              },
            });

            logger.warn(
              {
                eventId: event.id,
                error: result.error,
                retryCount: newRetryCount,
                processingTimeMs,
              },
              'Falha ao processar evento',
            );
          }
        } catch (error: any) {
          logger.error({ error, eventId: event.id }, 'Erro inesperado ao processar evento');

          const newRetryCount = event.retryCount + 1;
          const newStatus = newRetryCount >= MAX_RETRIES ? 'failed' : 'pending';

          await prisma.webhookEvent.update({
            where: { id: event.id },
            data: {
              status: newStatus,
              retryCount: newRetryCount,
              errorMessage: error.message?.substring(0, 1000) || 'Erro desconhecido',
            },
          });
        }
      }
    }

    // Em PostgreSQL com legado desligado, manter apenas fluxos explicitamente
    // adaptados para modo seguro, evitando executar domínios ainda dependentes de SQL Server.
    if (IS_POSTGRES && !ENABLE_SQLSERVER_LEGACY) {
      logger.debug(
        {
          isPostgres: IS_POSTGRES,
          enableSqlServerLegacy: ENABLE_SQLSERVER_LEGACY,
        },
        'Worker em modo PostgreSQL sem legado: executando somente fluxos adaptados',
      );

      if (isServiceEnabled('CTE')) {
        await processPendingCte(prisma);
        await processPendingCteCancelados(prisma);
      }

      if (isServiceEnabled('CIOT')) {
        await processPendingCiot(prisma);
      }

      if (isServiceEnabled('NFSE')) {
        await processPendingNfse(prisma);
      }

      if (isServiceEnabled('CONTAS_PAGAR')) {
        await processPendingContasPagar(prisma);
      }

      if (isServiceEnabled('CONTAS_RECEBER')) {
        await processPendingContasReceber(prisma);
      }

      if (isServiceEnabled('CONTAS_RECEBER_BAIXA')) {
        await processPendingContasReceberBaixa(prisma);
      }

      return;
    }

    if (isServiceEnabled('NFSE')) {
      await processPendingNfse(prisma);
    }

    if (isServiceEnabled('CIOT')) {
      await processPendingCiot(prisma);
    }

    if (isServiceEnabled('CTE')) {
      await processPendingCte(prisma);
      await processPendingCteCancelados(prisma);
    }

    if (isServiceEnabled('CONTAS_PAGAR')) {
      await processPendingContasPagar(prisma);
    }

    if (isServiceEnabled('CONTAS_RECEBER')) {
      await processPendingContasReceber(prisma);
    }

    if (isServiceEnabled('CONTAS_RECEBER_BAIXA')) {
      await processPendingContasReceberBaixa(prisma);
    }
  } catch (error: any) {
    // Se a tabela WebhookEvent não existir (P2021), apenas logar como debug (não é erro crítico)
    if (isMissingWebhookEventTable(error)) {
      logger.debug('Tabela WebhookEvent não existe ainda, aguardando criação');
      return;
    }

    // Tratar erros de connection pool timeout (P2024) e conexão fechada (P1017)
    if (error?.code === 'P2024' || error?.code === 'P1017') {
      logger.warn(
        {
          error: error.message,
          code: error.code,
          meta: error.meta,
        },
        'Erro de conexão com banco de dados - aguardando próxima tentativa (connection pool timeout)',
      );
      // Não propagar o erro para evitar loops infinitos - aguardar próxima iteração
      return;
    }

    const errorMessage = error?.message || error?.toString() || 'Erro desconhecido';
    const errorDetails = {
      name: error?.name,
      message: errorMessage,
      code: error?.code,
      meta: error?.meta,
    };
    logger.error(
      { error: errorDetails, fullError: error },
      'Erro ao buscar/processar lote de eventos',
    );

    // Se for erro de inicialização do Prisma, mostrar detalhes adicionais
    if (error?.name === 'PrismaClientInitializationError') {
      logger.error(
        {
          message: errorMessage,
          code: error?.code,
          meta: error?.meta,
        },
        'Erro de conexão com o banco de dados - verifique a DATABASE_URL no arquivo .env',
      );
    }
  }
}

async function startWorker() {
  if (!ENABLE_WORKER) {
    logger.warn(
      { enableWorker: ENABLE_WORKER },
      'Worker desativado por configuração (ENABLE_WORKER=false)',
    );
    return;
  }

  logger.info(
    { interval: INTERVAL_MS, batchSize: BATCH_SIZE, maxRetries: MAX_RETRIES },
    'Worker iniciado',
  );

  // Processar imediatamente na inicialização
  await processBatch();

  // Processar em intervalos
  setInterval(async () => {
    await processBatch();
  }, INTERVAL_MS);
}

// Graceful shutdown
process.on('SIGTERM', async () => {
  logger.info('SIGTERM recebido, encerrando worker...');
  await prisma.$disconnect();
  process.exit(0);
});

process.on('SIGINT', async () => {
  logger.info('SIGINT recebido, encerrando worker...');
  await prisma.$disconnect();
  process.exit(0);
});

process.on('unhandledRejection', (reason) => {
  logger.error({ reason }, 'Unhandled Rejection');
});

process.on('uncaughtException', (error) => {
  logger.error({ error }, 'Uncaught Exception');
  process.exit(1);
});

// Iniciar worker
startWorker().catch((error) => {
  logger.error({ error }, 'Erro fatal ao iniciar worker');
  process.exit(1);
});
