import type { Request, Response } from 'express';
import { PrismaClient } from '@prisma/client';
import { contasPagarSchema, type ContasPagar } from '../schemas/contasPagar';
import { inserirContasPagar } from '../services/contasPagarService';
import { logger } from '../utils/logger';
import { createOrUpdateWebhookEvent } from '../utils/webhookEvent';

const prisma = new PrismaClient();

/**
 * Gera um eventId para ContasPagar baseado no document ou id
 */
function generateContasPagarEventId(document?: string | null, id?: number | null): string {
  if (document) {
    return `contas-pagar-${document}`;
  }
  if (id) {
    return `contas-pagar-${id}`;
  }
  return `contas-pagar-${Date.now()}`;
}

/**
 * Controller para inserir Contas a Pagar
 * POST /api/ContasPagar/InserirContasPagar?token=...
 * Segue o mesmo padrão síncrono do Contas a Receber para retornar status correto
 */
export async function inserirContasPagarController(req: Request, res: Response) {
  const startTime = Date.now();
  let eventId: string | undefined;
  let webhookEventId: string | undefined;

  try {
    // Normalizar payload (aceitar tanto camelCase quanto snake_case)
    const normalizedBody = normalizeContasPagarPayload(req.body);

    // Validar payload com Zod
    const contasPagar = contasPagarSchema.parse(normalizedBody);

    // Gerar eventId baseado no document ou id
    eventId = generateContasPagarEventId(
      contasPagar.data.document,
      contasPagar.data.id
    );

    // Criar registro inicial no WebhookEvent
    webhookEventId = eventId;
    await createOrUpdateWebhookEvent(
      webhookEventId,
      '/api/ContasPagar/InserirContasPagar',
      'pending'
    );

    // Atualizar status para processing
    await createOrUpdateWebhookEvent(
      webhookEventId,
      '/api/ContasPagar/InserirContasPagar',
      'processing'
    );

    // Chamar service para inserir (o service já atualiza o WebhookEvent com detalhes das tabelas)
    // Sincrono: Cabeçalho da fatura (rápido)
    // Assíncrono: Itens e Parcelas (lento)
    const resultado = await inserirContasPagar(contasPagar, webhookEventId);

    const processingTime = Date.now() - startTime;

    if (resultado.status) {
      logger.info(
        {
          eventId: webhookEventId,
          idFatura: resultado.idFatura,
          document: contasPagar.data.document,
          processingTimeMs: processingTime,
        },
        resultado.mensagem
      );

      return res.status(202).json({
        Status: resultado.status,
        Mensagem: resultado.mensagem,
        IdFatura: resultado.idFatura,
        EventId: webhookEventId,
      });
    } else {
      logger.error(
        {
          eventId: webhookEventId,
          document: contasPagar.data.document,
          error: resultado.mensagem,
          processingTimeMs: processingTime,
        },
        'Falha ao processar conta a pagar'
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
          const path = Array.isArray(e.path) ? e.path.join('.') : (e.path || 'unknown');
          return `${path}: ${e.message || 'Erro de validação'}`;
        })
        .join(', ');

      if (webhookEventId) {
        await createOrUpdateWebhookEvent(
          webhookEventId,
          '/api/ContasPagar/InserirContasPagar',
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
        'Erro de validação ao inserir conta a pagar'
      );

      return res.status(400).json({
        Status: false,
        Mensagem: `Erro de validação: ${errorMessages}`,
      });
    }

    // Outros erros de validação/inicialização - retornar erro imediatamente
    if (webhookEventId) {
      await createOrUpdateWebhookEvent(
        webhookEventId,
        '/api/ContasPagar/InserirContasPagar',
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
      'Erro inesperado ao inserir conta a pagar'
    );

    return res.status(500).json({
      Status: false,
      Mensagem: 'Registro não inserido, favor verificar log!',
    });
  }
}

/**
 * Normaliza campos do payload de camelCase/snake_case para o formato esperado
 */
function normalizeContasPagarPayload(body: any): any {
  const normalized: Record<string, any> = {};

  // Normalizar campos principais
  if (body.installment_count !== undefined) {
    normalized.installment_count = typeof body.installment_count === 'string' 
      ? parseFloat(body.installment_count) || 0 
      : body.installment_count;
  }

  // Normalizar data
  if (body.data) {
    normalized.data = normalizeContasPagarData(body.data);
  }

  return normalized;
}

/**
 * Normaliza o objeto data
 */
function normalizeContasPagarData(data: any): any {
  const normalized: Record<string, any> = {};

  // Campos principais
  if (data.id !== undefined) {
    normalized.id = typeof data.id === 'string' ? parseFloat(data.id) || 0 : data.id;
  }
  if (data.type !== undefined) {
    normalized.type = data.type;
  }
  if (data.document !== undefined) {
    normalized.document = data.document;
  }
  if (data.issue_date !== undefined) {
    normalized.issue_date = data.issue_date;
  }
  if (data.due_date !== undefined) {
    normalized.due_date = data.due_date;
  }
  if (data.value !== undefined) {
    normalized.value = typeof data.value === 'string' ? parseFloat(data.value) || 0 : data.value;
  }
  if (data.installment_period !== undefined) {
    normalized.installment_period = data.installment_period;
  }
  if (data.comments !== undefined) {
    normalized.comments = data.comments;
  }
  if (data.cancelado !== undefined) {
    normalized.cancelado = typeof data.cancelado === 'string' ? parseFloat(data.cancelado) || 0 : data.cancelado;
  }
  if (data.Obscancelado !== undefined) {
    normalized.Obscancelado = data.Obscancelado;
  }
  if (data.DsUsuarioCancel !== undefined) {
    normalized.DsUsuarioCancel = data.DsUsuarioCancel;
  }

  // Normalizar corporation
  if (data.corporation) {
    normalized.corporation = {
      id: typeof data.corporation.id === 'string' ? parseFloat(data.corporation.id) || 0 : data.corporation.id,
      person_id: typeof (data.corporation.person_id ?? data.corporation.personId) === 'string'
        ? parseFloat(data.corporation.person_id ?? data.corporation.personId) || 0
        : (data.corporation.person_id ?? data.corporation.personId),
      nickname: data.corporation.nickname,
      cnpj: data.corporation.cnpj,
    };
  }

  // Normalizar receiver
  if (data.receiver) {
    normalized.receiver = {
      id: typeof data.receiver.id === 'string' ? parseFloat(data.receiver.id) || 0 : data.receiver.id,
      name: data.receiver.name,
      type: data.receiver.type,
      cnpj: data.receiver.cnpj,
      cpf: data.receiver.cpf,
      email: data.receiver.email,
      phone: data.receiver.phone,
    };
  }

  // Normalizar accounting_planning_management
  if (data.accounting_planning_management || data.accountingPlanningManagement) {
    const apm = data.accounting_planning_management || data.accountingPlanningManagement;
    normalized.accounting_planning_management = {
      id: typeof apm.id === 'string' ? parseFloat(apm.id) || 0 : apm.id,
      code_cache: apm.code_cache ?? apm.codeCache ?? '0',
      name: apm.name,
    };
  }

  // Normalizar cost_centers
  if (data.cost_centers || data.costCenters) {
    const cc = data.cost_centers || data.costCenters;
    normalized.cost_centers = {
      id: typeof cc.id === 'string' ? parseFloat(cc.id) || 0 : cc.id,
      name: cc.name,
    };
  }

  // Normalizar installments
  if (data.installments && Array.isArray(data.installments)) {
    normalized.installments = data.installments.map((inst: any) => ({
      id: typeof inst.id === 'string' ? parseFloat(inst.id) || 0 : (inst.id || 0),
      position: typeof inst.position === 'string' ? parseFloat(inst.position) || 0 : (inst.position || 0),
      due_date: inst.due_date ?? inst.dueDate,
      value: typeof inst.value === 'string' ? parseFloat(inst.value) || 0 : (inst.value || 0),
      interest_value: typeof inst.interest_value === 'string' || typeof inst.interestValue === 'string' 
        ? parseFloat(inst.interest_value ?? inst.interestValue) || 0 
        : (inst.interest_value ?? inst.interestValue ?? 0),
      discount_value: typeof inst.discount_value === 'string' || typeof inst.discountValue === 'string'
        ? parseFloat(inst.discount_value ?? inst.discountValue) || 0
        : (inst.discount_value ?? inst.discountValue ?? 0),
      payment_method: inst.payment_method ?? inst.paymentMethod,
      comments: inst.comments,
      payment_date: inst.payment_date ?? inst.paymentDate,
    }));
  }

  // Normalizar invoice_items
  if (data.invoice_items || data.invoiceItems) {
    const items = data.invoice_items || data.invoiceItems;
    if (Array.isArray(items)) {
      normalized.invoice_items = items.map((item: any) => ({
        id: typeof item.id === 'string' ? parseFloat(item.id) || 0 : (item.id || 0),
        freight_id: typeof item.freight_id === 'string' || typeof item.freightId === 'string'
          ? parseFloat(item.freight_id ?? item.freightId) || 0
          : (item.freight_id ?? item.freightId ?? 0),
        cte_key: item.cte_key ?? item.cteKey,
        cte_number: typeof item.cte_number === 'string' || typeof item.cteNumber === 'string'
          ? parseFloat(item.cte_number ?? item.cteNumber) || 0
          : (item.cte_number ?? item.cteNumber ?? 0),
        cte_series: typeof item.cte_series === 'string' || typeof item.cteSeries === 'string'
          ? parseFloat(item.cte_series ?? item.cteSeries) || 0
          : (item.cte_series ?? item.cteSeries ?? 0),
        payer_name: item.payer_name ?? item.payerName,
        // draft_number, nfse_number e nfse_series devem ser strings (converter número para string se necessário)
        draft_number: item.draft_number ?? item.draftNumber 
          ? String(item.draft_number ?? item.draftNumber) 
          : undefined,
        nfse_number: item.nfse_number ?? item.nfseNumber 
          ? String(item.nfse_number ?? item.nfseNumber) 
          : undefined,
        nfse_series: item.nfse_series ?? item.nfseSeries 
          ? String(item.nfse_series ?? item.nfseSeries) 
          : undefined,
        type: item.type,
        total: typeof item.total === 'string' ? parseFloat(item.total) || 0 : (item.total || 0),
      }));
    }
  }

  return normalized;
}
