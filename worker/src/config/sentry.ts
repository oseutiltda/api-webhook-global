import { logger } from '../utils/logger';

let sentryInitialized = false;
let sentryClient: any | null = null;

export function initSentry() {
  if (sentryInitialized) return;

  const dsn = process.env.SENTRY_DSN;

  if (!dsn) {
    logger.warn('SENTRY_DSN não configurado; Sentry desativado no worker');
    return;
  }

  try {
    const Sentry = require('@sentry/node');
    Sentry.init({
      dsn,
      environment: process.env.NODE_ENV || 'development',
      release: process.env.SENTRY_RELEASE || 'api-webhook-global@1.0.0',
      tracesSampleRate: Number(process.env.SENTRY_TRACES_SAMPLE_RATE || '0.2'),
    });
    sentryClient = Sentry;
  } catch (error: any) {
    logger.warn(
      { error: error?.message },
      'Pacote @sentry/node não instalado; Sentry desativado no worker',
    );
    return;
  }

  sentryInitialized = true;
  logger.info({ environment: process.env.NODE_ENV }, 'Sentry inicializado no worker');
}

export function captureSentryException(error: unknown, context?: Record<string, unknown>) {
  if (!sentryClient) return;
  sentryClient.captureException(error, context);
}

export function flushSentry(timeoutMs = 2000) {
  if (!sentryClient) return Promise.resolve(false);
  return sentryClient.flush(timeoutMs);
}
