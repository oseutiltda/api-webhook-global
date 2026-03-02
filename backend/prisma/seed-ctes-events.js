const path = require('path');
const dotenv = require('dotenv');
const { PrismaClient } = require('@prisma/client');

dotenv.config();
dotenv.config({ path: path.resolve(__dirname, '../../.env') });

const prisma = new PrismaClient();

const EVENT_IDS = [
  'seed-cte-evt-001',
  'seed-cte-evt-002',
  'seed-cte-evt-003',
  'seed-cte-evt-004',
  'seed-cte-evt-005',
  'seed-cte-evt-006',
  'seed-cte-evt-007',
  'seed-cte-evt-008',
];

const now = Date.now();
const minutesAgo = (minutes) => new Date(now - minutes * 60 * 1000);

const makeMetadata = (cteId, chCTe, extra = {}) =>
  JSON.stringify({
    cteId,
    chCTe,
    ...extra,
  });

async function main() {
  await prisma.webhookEvent.deleteMany({
    where: {
      id: {
        in: EVENT_IDS,
      },
    },
  });

  await prisma.webhookEvent.createMany({
    data: [
      {
        id: 'seed-cte-evt-001',
        source: '/webhooks/cte/autorizado',
        receivedAt: minutesAgo(120),
        status: 'processed',
        processedAt: minutesAgo(118),
        retryCount: 0,
        integrationStatus: 'integrated',
        processingTimeMs: 220,
        integrationTimeMs: 800,
        seniorId: 'cte-700001',
        metadata: makeMetadata(990001, '35260312345678000123570010000000011000000001'),
        tipoIntegracao: 'Web API',
      },
      {
        id: 'seed-cte-evt-002',
        source: '/webhooks/cte/autorizado',
        receivedAt: minutesAgo(95),
        status: 'processed',
        processedAt: minutesAgo(94),
        retryCount: 0,
        integrationStatus: 'integrated',
        processingTimeMs: 180,
        integrationTimeMs: 620,
        seniorId: 'cte-700002',
        metadata: makeMetadata(990002, '35260312345678000123570010000000021000000002'),
        tipoIntegracao: 'Worker',
      },
      {
        id: 'seed-cte-evt-003',
        source: '/webhooks/cte/cancelado',
        receivedAt: minutesAgo(80),
        status: 'processed',
        processedAt: minutesAgo(79),
        retryCount: 0,
        integrationStatus: 'skipped',
        processingTimeMs: 140,
        integrationTimeMs: 0,
        seniorId: null,
        metadata: makeMetadata(990003, '35260312345678000123570010000000031000000003', {
          motivo: 'cancelamento',
        }),
        tipoIntegracao: 'Web API',
      },
      {
        id: 'seed-cte-evt-004',
        source: '/webhooks/cte/cancelado',
        receivedAt: minutesAgo(65),
        status: 'failed',
        processedAt: minutesAgo(64),
        errorMessage: 'Falha simulada na integracao de cancelamento',
        retryCount: 1,
        integrationStatus: 'failed',
        processingTimeMs: 400,
        integrationTimeMs: 900,
        seniorId: null,
        metadata: makeMetadata(990004, '35260312345678000123570010000000041000000004'),
        tipoIntegracao: 'Worker',
      },
      {
        id: 'seed-cte-evt-005',
        source: '/webhooks/cte/autorizado',
        receivedAt: minutesAgo(45),
        status: 'pending',
        processedAt: null,
        retryCount: 0,
        integrationStatus: 'pending',
        processingTimeMs: null,
        integrationTimeMs: null,
        seniorId: null,
        metadata: makeMetadata(990005, '35260312345678000123570010000000051000000005'),
        tipoIntegracao: 'Web API',
      },
      {
        id: 'seed-cte-evt-006',
        source: '/webhooks/cte/autorizado',
        receivedAt: minutesAgo(30),
        status: 'processing',
        processedAt: null,
        retryCount: 0,
        integrationStatus: 'pending',
        processingTimeMs: null,
        integrationTimeMs: null,
        seniorId: null,
        metadata: makeMetadata(990006, '35260312345678000123570010000000061000000006'),
        tipoIntegracao: 'Worker',
      },
      {
        id: 'seed-cte-evt-007',
        source: '/api/CTe/InserirCte',
        receivedAt: minutesAgo(20),
        status: 'processed',
        processedAt: minutesAgo(19),
        retryCount: 0,
        integrationStatus: 'integrated',
        processingTimeMs: 210,
        integrationTimeMs: 700,
        seniorId: 'cte-700007',
        metadata: makeMetadata(990001, '35260312345678000123570010000000011000000001', {
          duplicateSample: true,
        }),
        tipoIntegracao: 'Web API',
      },
      {
        id: 'seed-cte-evt-008',
        source: '/api/CTe/InserirCte',
        receivedAt: minutesAgo(5),
        status: 'processed',
        processedAt: minutesAgo(4),
        retryCount: 0,
        integrationStatus: 'integrated',
        processingTimeMs: 160,
        integrationTimeMs: 540,
        seniorId: 'cte-700008',
        metadata: makeMetadata(990002, '35260312345678000123570010000000021000000002', {
          duplicateSample: true,
        }),
        tipoIntegracao: 'Worker',
      },
    ],
  });

  // eslint-disable-next-line no-console
  console.log('[seed:ctes-events] 8 eventos de CT-e inseridos em WebhookEvent.');
}

main()
  .catch((error) => {
    // eslint-disable-next-line no-console
    console.error('[seed:ctes-events] erro ao executar seed', error);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
