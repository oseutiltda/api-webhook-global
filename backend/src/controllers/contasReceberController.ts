import type { Request, Response } from 'express';
import { PrismaClient } from '@prisma/client';
import { contasReceberSchema } from '../schemas/contasReceber';
import { inserirContasReceber } from '../services/contasReceberService';
import { logger } from '../utils/logger';
import { createOrUpdateWebhookEvent } from '../utils/webhookEvent';

const prisma = new PrismaClient();

/**
 * Gera um eventId para ContasReceber baseado no document ou id
 */
function generateContasReceberEventId(document?: string | null, id?: number | null): string {
  if (document) {
    return `contas-receber-${document}`;
  }
  if (id) {
    return `contas-receber-${id}`;
  }
  return `contas-receber-${Date.now()}`;
}

/**
 * Controller para inserir Contas a Receber
 * POST /api/ContasReceber/InserirContasReceber?token=...
 */
export async function inserirContasReceberController(req: Request, res: Response) {
  const startTime = Date.now();
  let eventId: string | undefined;
  let webhookEventId: string | undefined;

  try {
    // Normalizar payload (aceitar tanto camelCase quanto snake_case)
    const normalizedBody = normalizeContasReceberPayload(req.body);

    logger.debug(
      {
        hasInstallmentCount: !!normalizedBody.installment_count,
        installmentCountValue: normalizedBody.installment_count,
        hasData: !!normalizedBody.data,
        hasInstallments: !!normalizedBody.data?.installments,
        hasInvoiceItems: !!normalizedBody.data?.invoice_items,
      },
      'Payload normalizado para ContasReceber',
    );

    // Validar payload com Zod
    const contasReceber = contasReceberSchema.parse(normalizedBody);

    // Gerar eventId baseado no document ou id
    eventId = generateContasReceberEventId(contasReceber.data.document, contasReceber.data.id);

    // Criar registro inicial no WebhookEvent
    webhookEventId = eventId;
    await createOrUpdateWebhookEvent(
      webhookEventId,
      '/api/ContasReceber/InserirContasReceber',
      'pending',
    );

    // Atualizar status para processing
    await createOrUpdateWebhookEvent(
      webhookEventId,
      '/api/ContasReceber/InserirContasReceber',
      'processing',
    );

    // Chamar service para inserir (o service já atualiza o WebhookEvent com detalhes das tabelas)
    // Sincrono: Cabeçalho da fatura (rápido)
    // Assíncrono: Itens e Parcelas (lento)
    const resultado = await inserirContasReceber(contasReceber, webhookEventId, startTime);

    const processingTime = Date.now() - startTime;

    if (resultado.status) {
      logger.info(
        {
          eventId: webhookEventId,
          idFatura: resultado.idFatura,
          document: contasReceber.data.document,
          processingTimeMs: processingTime,
        },
        resultado.mensagem,
      );

      return res.status(202).json({
        Status: resultado.status,
        Mensagem: resultado.mensagem,
        EventId: webhookEventId,
      });
    } else {
      logger.error(
        {
          eventId: webhookEventId,
          document: contasReceber.data.document,
          error: resultado.mensagem,
          processingTimeMs: processingTime,
        },
        'Falha ao inserir conta a receber',
      );

      return res.status(400).json({
        Status: resultado.status,
        Mensagem: resultado.mensagem,
        EventId: webhookEventId,
      });
    }
  } catch (error: any) {
    const processingTime = Date.now() - startTime;

    // Se for erro de validação Zod
    if (error.name === 'ZodError' || error.constructor?.name === 'ZodError') {
      const errors = error.errors || error.issues || [];
      const errorMessages = errors
        .map((e: any) => {
          const path = Array.isArray(e.path) ? e.path.join('.') : e.path || 'unknown';
          return `${path}: ${e.message || 'Erro de validação'}`;
        })
        .join(', ');

      if (webhookEventId) {
        await createOrUpdateWebhookEvent(
          webhookEventId,
          '/api/ContasReceber/InserirContasReceber',
          'failed',
          `Validação falhou: ${errorMessages}`,
          {
            integrationStatus: 'failed',
            processingTimeMs: processingTime,
          },
        );
      }

      logger.error(
        {
          eventId: webhookEventId,
          error: errorMessages,
          processingTimeMs: processingTime,
        },
        'Erro de validação ao inserir conta a receber',
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
        '/api/ContasReceber/InserirContasReceber',
        'failed',
        error.message || 'Erro desconhecido',
        {
          integrationStatus: 'failed',
          processingTimeMs: processingTime,
        },
      );
    }

    logger.error(
      {
        eventId: webhookEventId,
        error: error.message,
        stack: error.stack,
        processingTimeMs: processingTime,
      },
      'Erro inesperado ao inserir conta a receber',
    );

    return res.status(500).json({
      Status: false,
      Mensagem: 'Registro não inserido, favor verificar log!',
      EventId: webhookEventId,
    });
  }
}

/**
 * Normaliza campos do payload de camelCase/snake_case para o formato esperado
 */
function normalizeContasReceberPayload(body: any): any {
  const normalized: Record<string, any> = {};

  // Normalizar installment_count - pode estar no nível raiz ou dentro de data
  if (body.installment_count !== undefined) {
    normalized.installment_count = body.installment_count;
  } else if (body.data?.installment_count !== undefined) {
    normalized.installment_count = body.data.installment_count;
  }

  // Normalizar data
  if (body.data) {
    const dataCopy = { ...body.data };
    // Remover installment_count de dentro de data se existir (já foi movido para nível raiz)
    if ('installment_count' in dataCopy) {
      delete dataCopy.installment_count;
    }
    normalized.data = normalizeContasReceberData(dataCopy);
  }

  return normalized;
}

/**
 * Normaliza o objeto data
 */
function normalizeContasReceberData(data: any): any {
  const normalized: Record<string, any> = {};

  // Campos principais (NÃO incluir installment_count aqui - ele vai para o nível raiz)
  // Converter campos numéricos de string para number
  if (data.id !== undefined) normalized.id = Number(data.id);
  if (data.type !== undefined) normalized.type = data.type;
  if (data.document !== undefined) normalized.document = data.document;
  if (data.issue_date !== undefined) normalized.issue_date = data.issue_date;
  if (data.due_date !== undefined) normalized.due_date = data.due_date;
  if (data.value !== undefined && data.value !== null && data.value !== '') {
    normalized.value = typeof data.value === 'string' ? Number(data.value) : data.value;
  }
  if (data.installment_period !== undefined)
    normalized.installment_period = data.installment_period;
  if (data.comments !== undefined) normalized.comments = data.comments;
  if (data.cancelado !== undefined)
    normalized.cancelado =
      typeof data.cancelado === 'string' ? Number(data.cancelado) : data.cancelado;
  if (data.Obscancelado !== undefined) normalized.Obscancelado = data.Obscancelado;
  if (data.DsUsuarioCan !== undefined) normalized.DsUsuarioCan = data.DsUsuarioCan;

  // Normalizar corporation
  if (data.corporation) {
    normalized.corporation = {
      id: data.corporation.id,
      person_id: data.corporation.person_id ?? data.corporation.personId,
      nickname: data.corporation.nickname,
      cnpj: data.corporation.cnpj,
    };
  }

  // Normalizar customer
  if (data.customer) {
    normalized.customer = {
      id: data.customer.id,
      name: data.customer.name,
      type: data.customer.type,
      cnpj: data.customer.cnpj,
      cpf: data.customer.cpf,
      email: data.customer.email,
      phone: data.customer.phone,
    };
  }

  // Normalizar accounting_planning_management
  if (data.accounting_planning_management || data.accountingPlanningManagement) {
    const apm = data.accounting_planning_management || data.accountingPlanningManagement;
    normalized.accounting_planning_management = {
      id: apm.id,
      code_cache: apm.code_cache ?? apm.codeCache ?? '0',
      name: apm.name,
      value: apm.value,
      total: apm.total,
    };
  }

  // Normalizar installments com conversão de tipos
  if (data.installments && Array.isArray(data.installments)) {
    normalized.installments = data.installments.map((inst: any) => {
      const normalizedInst: any = {
        id: typeof inst.id === 'string' ? Number(inst.id) : inst.id,
        due_date: inst.due_date ?? inst.dueDate,
        payment_method: inst.payment_method ?? inst.paymentMethod,
        comments: inst.comments,
        payment_date: inst.payment_date ?? inst.paymentDate,
      };

      if (inst.position !== undefined && inst.position !== null) {
        normalizedInst.position =
          typeof inst.position === 'string' ? Number(inst.position) : inst.position;
      }

      if (inst.value !== undefined && inst.value !== null && inst.value !== '') {
        normalizedInst.value = typeof inst.value === 'string' ? Number(inst.value) : inst.value;
      }

      const interestValue = inst.interest_value ?? inst.interestValue;
      if (interestValue !== undefined && interestValue !== null && interestValue !== '') {
        normalizedInst.interest_value =
          typeof interestValue === 'string' ? Number(interestValue) : interestValue;
      }

      const discountValue = inst.discount_value ?? inst.discountValue;
      if (discountValue !== undefined && discountValue !== null && discountValue !== '') {
        normalizedInst.discount_value =
          typeof discountValue === 'string' ? Number(discountValue) : discountValue;
      }

      return normalizedInst;
    });
  }

  // Normalizar invoice_items com conversão de tipos
  if (data.invoice_items || data.invoiceItems) {
    const items = data.invoice_items || data.invoiceItems;
    if (Array.isArray(items)) {
      normalized.invoice_items = items.map((item: any) => {
        const normalizedItem: any = {
          id: typeof item.id === 'string' ? Number(item.id) : item.id,
          cte_key: item.cte_key ?? item.cteKey ?? '',
          payer_name: item.payer_name ?? item.payerName,
          draft_number: item.draft_number ?? item.draftNumber,
          nfse_number: item.nfse_number ?? item.nfseNumber,
          nfse_series: item.nfse_series ?? item.nfseSeries,
          type: item.type,
        };

        // Converter cte_number apenas se não for string vazia
        const cteNumber = item.cte_number ?? item.cteNumber;
        if (cteNumber !== undefined && cteNumber !== null && cteNumber !== '') {
          normalizedItem.cte_number = typeof cteNumber === 'string' ? Number(cteNumber) : cteNumber;
        }

        // Converter cte_series apenas se não for string vazia
        const cteSeries = item.cte_series ?? item.cteSeries;
        if (cteSeries !== undefined && cteSeries !== null && cteSeries !== '') {
          normalizedItem.cte_series = typeof cteSeries === 'string' ? Number(cteSeries) : cteSeries;
        }

        // Converter total
        if (item.total !== undefined && item.total !== null && item.total !== '') {
          normalizedItem.total = typeof item.total === 'string' ? Number(item.total) : item.total;
        }

        return normalizedItem;
      });
    }
  }

  return normalized;
}
