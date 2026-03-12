import { PrismaClient } from '@prisma/client';
import { logger } from './logger';

const prisma = new PrismaClient();

/**
 * Cria ou atualiza um evento WebhookEvent para monitoramento
 * @param eventId ID único do evento (ex: ciot-{manifestId})
 * @param source Fonte do evento (ex: /api/CIOT/InserirContasPagarCIOT)
 * @param status Status do evento (pending, processing, processed, failed)
 * @param errorMessage Mensagem de erro (se houver)
 * @param metadata Metadados adicionais (JSON string)
 * @param tipoIntegracao Tipo de integração: 'Web API' (backend) ou 'Worker' (worker)
 */
export async function createOrUpdateWebhookEvent(
  eventId: string,
  source: string,
  status: 'pending' | 'processing' | 'processed' | 'failed',
  errorMessage?: string | null,
  metadata?: Record<string, any> | null,
  tipoIntegracao?: string | null,
): Promise<void> {
  try {
    // Se tipoIntegracao não for fornecido, usar 'Web API' como padrão (chamado do backend)
    const tipo = tipoIntegracao || 'Web API';

    await prisma.webhookEvent.upsert({
      where: { id: eventId },
      create: {
        id: eventId,
        source,
        receivedAt: new Date(),
        status,
        errorMessage: errorMessage ? errorMessage.substring(0, 1000) : null,
        retryCount: 0,
        metadata: metadata ? JSON.stringify(metadata).substring(0, 2000) : null,
        tipoIntegracao: tipo,
      },
      update: {
        status,
        errorMessage: errorMessage ? errorMessage.substring(0, 1000) : null,
        metadata: metadata ? JSON.stringify(metadata).substring(0, 2000) : null,
        tipoIntegracao: tipo, // Atualizar também no update para garantir consistência
        ...(status === 'processed' || status === 'failed' ? { processedAt: new Date() } : {}),
      },
    });
  } catch (error: any) {
    // Tratar erro de unique constraint (evento já existe - pode acontecer em condições de corrida)
    if (error?.code === 'P2002' || error?.message?.includes('Unique constraint')) {
      // Tentar atualizar o evento existente
      try {
        await prisma.webhookEvent.update({
          where: { id: eventId },
          data: {
            status,
            errorMessage: errorMessage ? errorMessage.substring(0, 1000) : null,
            metadata: metadata ? JSON.stringify(metadata).substring(0, 2000) : null,
            tipoIntegracao: tipoIntegracao || 'Web API',
            ...(status === 'processed' || status === 'failed' ? { processedAt: new Date() } : {}),
          },
        });
        logger.debug({ eventId, source }, 'Evento WebhookEvent atualizado após unique constraint');
      } catch (updateError: any) {
        // Se também falhar na atualização, apenas logar como aviso (não é crítico)
        logger.warn(
          {
            error: updateError?.message,
            eventId,
            source,
          },
          'Erro ao atualizar WebhookEvent após unique constraint',
        );
      }
      return; // Sair silenciosamente após tentar atualizar
    }

    // Ignorar erros de tabela não existente ou outros erros não críticos
    if (error?.code !== 'P2021' && error?.code !== 'P2003') {
      logger.warn(
        { error: error?.message, eventId, source },
        'Erro ao criar/atualizar WebhookEvent',
      );
    }
  }
}

/**
 * Gera um eventId para CIOT baseado no manifestId ou dados do manifesto
 */
export function generateCiotEventId(manifestId?: number | null, nrciot?: string | null): string {
  if (manifestId) {
    return `ciot-${manifestId}`;
  }
  if (nrciot) {
    // Se não tem manifestId ainda, usar timestamp para garantir unicidade
    return `ciot-${nrciot}-${Date.now()}`;
  }
  // Fallback: usar timestamp
  return `ciot-${Date.now()}`;
}
