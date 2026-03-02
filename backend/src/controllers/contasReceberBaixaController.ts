import type { Request, Response } from 'express';
import { PrismaClient } from '@prisma/client';
import { contasReceberBaixaSchema } from '../schemas/contasReceberBaixa';
import { inserirContasReceberBaixa } from '../services/contasReceberBaixaService';
import { logger } from '../utils/logger';
import { createOrUpdateWebhookEvent } from '../utils/webhookEvent';
import { z } from 'zod';

const prisma = new PrismaClient();

/**
 * Gera um eventId para ContasReceberBaixa baseado no installment_id
 */
function generateContasReceberBaixaEventId(installmentId?: number | null): string {
  if (installmentId) {
    return `contas-receber-baixa-${installmentId}`;
  }
  return `contas-receber-baixa-${Date.now()}`;
}

/**
 * Controller para inserir Baixa de Contas a Receber
 * POST /api/ContasReceber/InserirContasReceberBaixa?token=...
 */
export async function inserirContasReceberBaixaController(req: Request, res: Response) {
  const startTime = Date.now();
  let eventId: string | undefined;
  let webhookEventId: string | undefined;

  try {
    // Normalizar payload (aceitar tanto camelCase quanto snake_case)
    const normalizedBody = normalizeContasReceberBaixaPayload(req.body);

    // Validar payload com Zod
    const baixa = contasReceberBaixaSchema.parse(normalizedBody);

    // Gerar eventId baseado no installment_id
    eventId = generateContasReceberBaixaEventId(baixa.installment_id);

    // Criar registro inicial no WebhookEvent
    webhookEventId = eventId;
    await createOrUpdateWebhookEvent(
      webhookEventId,
      '/api/ContasReceber/InserirContasReceberBaixa',
      'pending'
    );

    // Atualizar status para processing
    await createOrUpdateWebhookEvent(
      webhookEventId,
      '/api/ContasReceber/InserirContasReceberBaixa',
      'processing'
    );

    // Chamar service para inserir
    // Alterado para assíncrono para evitar timeout do cliente
    inserirContasReceberBaixa(baixa).then(resultado => {
      const processingTime = Date.now() - startTime;
      if (resultado.status) {
        createOrUpdateWebhookEvent(
          webhookEventId!,
          '/api/ContasReceber/InserirContasReceberBaixa',
          'processed',
          null,
          {
            integrationStatus: 'integrated',
            processingTimeMs: processingTime,
            installment_id: baixa.installment_id,
            payment_date: baixa.payment_date,
            payment_value: baixa.payment_value,
          }
        ).catch(e => logger.error({ error: e.message }, 'Erro ao atualizar WebhookEvent sucesso (baixa)'));
      } else {
        createOrUpdateWebhookEvent(
          webhookEventId!,
          '/api/ContasReceber/InserirContasReceberBaixa',
          'failed',
          resultado.mensagem,
          {
            integrationStatus: 'failed',
            processingTimeMs: processingTime,
            installment_id: baixa.installment_id,
            error: resultado.mensagem,
          }
        ).catch(e => logger.error({ error: e.message }, 'Erro ao atualizar WebhookEvent falha (baixa)'));
      }
    }).catch(err => {
      logger.error({ eventId: webhookEventId, error: err.message }, 'Erro no processamento em segundo plano de baixa de conta a receber');
    });

    const processingTime = Date.now() - startTime;

    logger.info(
      {
        eventId: webhookEventId,
        installment_id: baixa.installment_id,
        processingTimeMs: processingTime,
      },
      'Processamento de baixa de conta a receber iniciado (assíncrono)'
    );

    return res.status(202).json({
      Status: true,
      Mensagem: 'Processamento iniciado com sucesso. Acompanhe pelo EventId.',
      EventId: webhookEventId,
    });
  } catch (error: any) {
    const processingTime = Date.now() - startTime;

    // Se for erro de validação Zod
    if (error instanceof z.ZodError) {
      const errorMessages = error.issues
        .map((e) => `${e.path.join('.')}: ${e.message}`)
        .join(', ');

      if (webhookEventId) {
        await createOrUpdateWebhookEvent(
          webhookEventId,
          '/api/ContasReceber/InserirContasReceberBaixa',
          'failed',
          `Validação falhou: ${errorMessages}`,
          {
            integrationStatus: 'failed',
            processingTimeMs: processingTime,
          }
        );
      }

      logger.error(
        {
          eventId: webhookEventId,
          error: errorMessages,
          processingTimeMs: processingTime,
        },
        'Erro de validação ao inserir baixa de conta a receber'
      );

      return res.status(400).json({
        Status: false,
        Mensagem: `Erro de validação: ${errorMessages}`,
        EventId: webhookEventId,
      });
    }

    // Outros erros
    if (webhookEventId) {
      await createOrUpdateWebhookEvent(
        webhookEventId,
        '/api/ContasReceber/InserirContasReceberBaixa',
        'failed',
        error.message || 'Erro desconhecido',
        {
          integrationStatus: 'failed',
          processingTimeMs: processingTime,
        }
      );
    }

    logger.error(
      {
        eventId: webhookEventId,
        error: error.message,
        stack: error.stack,
        processingTimeMs: processingTime,
      },
      'Erro inesperado ao inserir baixa de conta a receber'
    );

    return res.status(500).json({
      Status: false,
      Mensagem: 'Baixa não inserida, favor verificar log!',
      EventId: webhookEventId,
    });
  }
}

/**
 * Normaliza campos do payload de camelCase/snake_case para o formato esperado
 */
function normalizeContasReceberBaixaPayload(body: any): any {
  const normalized: Record<string, any> = {};

  // Campos principais - aceitar tanto camelCase quanto snake_case
  if (body.installment_id !== undefined || body.installmentId !== undefined) {
    normalized.installment_id = body.installment_id ?? body.installmentId;
  }

  if (body.payment_date !== undefined || body.paymentDate !== undefined) {
    normalized.payment_date = body.payment_date ?? body.paymentDate;
  }

  if (body.payment_value !== undefined || body.paymentValue !== undefined) {
    normalized.payment_value = body.payment_value ?? body.paymentValue;
  }

  if (body.discount_value !== undefined || body.discountValue !== undefined) {
    normalized.discount_value = body.discount_value ?? body.discountValue;
  }

  if (body.interest_value !== undefined || body.interestValue !== undefined) {
    normalized.interest_value = body.interest_value ?? body.interestValue;
  }

  if (body.payment_method !== undefined || body.paymentMethod !== undefined) {
    normalized.payment_method = body.payment_method ?? body.paymentMethod;
  }

  if (body.bank_account !== undefined || body.bankAccount !== undefined) {
    normalized.bank_account = body.bank_account ?? body.bankAccount;
  }

  if (body.bankname !== undefined) {
    normalized.bankname = body.bankname;
  }

  if (body.accountnumber !== undefined || body.accountNumber !== undefined) {
    normalized.accountnumber = body.accountnumber ?? body.accountNumber;
  }

  if (body.accountdigit !== undefined || body.accountDigit !== undefined) {
    normalized.accountdigit = body.accountdigit ?? body.accountDigit;
  }

  if (body.comments !== undefined) {
    normalized.comments = body.comments;
  }

  return normalized;
}

