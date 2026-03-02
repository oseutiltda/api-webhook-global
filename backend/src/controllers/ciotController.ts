import type { Request, Response } from 'express';
import { PrismaClient } from '@prisma/client';
import { contasPagarCIOTSchema, cancelarContasPagarCIOTSchema } from '../schemas/ciot';
import { inserirContasPagarCIOT, cancelarContasPagarCIOT } from '../services/ciotService';
import { logger } from '../utils/logger';
import { createOrUpdateWebhookEvent, generateCiotEventId } from '../utils/webhookEvent';

const prisma = new PrismaClient();

/**
 * Controller para inserir ContasPagarCIOT
 * POST /api/CIOT/InserirContasPagarCIOT?token=...
 */
export async function inserirContasPagarCIOTController(req: Request, res: Response) {
  let eventId: string | null = null;
  const source = '/api/CIOT/InserirContasPagarCIOT';
  
  try {
    // Aceitar payloads com raiz "manifest" (minúscula) e normalizar para "Manifest"
    if (req.body.manifest && !req.body.Manifest) {
      req.body.Manifest = req.body.manifest;
    }
    // Normalizar campos top-level vindos em minúsculas
    if (req.body.obscancelado !== undefined && req.body.Obscancelado === undefined) {
      req.body.Obscancelado = req.body.obscancelado;
    }
    if (req.body.dsusuariocanc !== undefined && req.body.DsUsuarioCan === undefined) {
      req.body.DsUsuarioCan = req.body.dsusuariocanc;
    }
    if (req.body.dsusuariocan !== undefined && req.body.DsUsuarioCan === undefined) {
      req.body.DsUsuarioCan = req.body.dsusuariocan;
    }

    // Gerar eventId baseado nos dados disponíveis
    const nrciot = req.body.Manifest?.nrciot || req.body.manifest?.nrciot;
    eventId = generateCiotEventId(null, nrciot);

    // Criar evento inicial como pending em background (não bloqueia)
    createOrUpdateWebhookEvent(eventId, source, 'pending', null, {
      nrciot: nrciot || null,
      etapa: 'validacao',
    }).catch((err: any) => logger.warn({ error: err?.message }, 'Erro ao criar evento pending (não crítico)'));

    // Debug: verificar se dadosFaturamento está presente antes da validação
    logger.debug({ 
      hasDadosFaturamento: !!req.body.Manifest?.dadosFaturamento,
      dadosFaturamento: req.body.Manifest?.dadosFaturamento 
    }, 'DadosFaturamento antes da validação do schema');

    // Validar schema
    const data = contasPagarCIOTSchema.parse(req.body);

    // Debug: verificar se dadosFaturamento está presente após a validação
    logger.debug({ 
      hasDadosFaturamento: !!data.Manifest?.dadosFaturamento,
      dadosFaturamento: data.Manifest?.dadosFaturamento 
    }, 'DadosFaturamento após validação do schema');

    logger.info({ cancelado: data.cancelado, nrciot: data.Manifest?.nrciot, hasDadosFaturamento: !!data.Manifest?.dadosFaturamento }, 'Recebida requisição para inserir ContasPagarCIOT');

    // Atualizar evento para processing em background (não bloqueia)
    createOrUpdateWebhookEvent(eventId, source, 'processing', null, {
      nrciot: data.Manifest?.nrciot || null,
      etapa: 'processamento',
    }).catch((err: any) => logger.warn({ error: err?.message }, 'Erro ao criar evento processing (não crítico)'));

    // Processar inserção
    const resultado = await inserirContasPagarCIOT(data, eventId);

    if (resultado.status) {
      // Se temos manifestId, migrar para o eventId correto (ciot-{manifestId})
      // Isso garante que o worker encontre o mesmo evento
      const finalEventId = resultado.manifestId 
        ? `ciot-${resultado.manifestId}` 
        : eventId;
      
      // Determinar código HTTP: 201 Created para novos registros, 200 OK para atualizações
      const httpStatus = resultado.created === true ? 201 : 200;
      
      // Retornar resposta IMEDIATAMENTE
      res.status(httpStatus).json({
        Status: resultado.status,
        Mensagem: resultado.mensagem,
      });

      // Atualizar eventos em background (não bloqueia a resposta)
      (async () => {
        try {
          // Se o eventId mudou, migrar o evento antigo para o novo
          if (finalEventId !== eventId && resultado.manifestId) {
            try {
              const oldEvent = await prisma.webhookEvent.findUnique({ where: { id: eventId } });
              if (oldEvent) {
                await prisma.webhookEvent.upsert({
                  where: { id: finalEventId },
                  create: {
                    id: finalEventId,
                    source: oldEvent.source,
                    receivedAt: oldEvent.receivedAt,
                    status: 'processed',
                    errorMessage: null,
                    retryCount: oldEvent.retryCount || 0,
                    metadata: JSON.stringify({
                      manifestId: resultado.manifestId,
                      nrciot: data.Manifest?.nrciot || null,
                      etapa: 'backend_concluido',
                    }).substring(0, 2000),
                    processedAt: new Date(),
                  },
                  update: {
                    status: 'processed',
                    metadata: JSON.stringify({
                      manifestId: resultado.manifestId,
                      nrciot: data.Manifest?.nrciot || null,
                      etapa: 'backend_concluido',
                    }).substring(0, 2000),
                    processedAt: new Date(),
                  },
                });
                await prisma.webhookEvent.delete({ where: { id: eventId } }).catch(() => {});
                logger.debug({ oldEventId: eventId, newEventId: finalEventId, manifestId: resultado.manifestId }, 'Evento migrado para eventId com manifestId');
              }
            } catch (migrateError: any) {
              logger.warn({ error: migrateError?.message, oldEventId: eventId, newEventId: finalEventId }, 'Erro ao migrar evento, usando eventId original');
            }
          }

          // Atualizar evento como processado (o worker vai atualizar com mais detalhes depois)
          // O metadata completo com tabelas já foi atualizado no ciotService.ts
          await createOrUpdateWebhookEvent(finalEventId, source, 'processed', null, {
            manifestId: resultado.manifestId || null,
            nrciot: data.Manifest?.nrciot || null,
            etapa: 'backend_concluido',
          });
        } catch (bgError: any) {
          logger.error({ error: bgError?.message, eventId: finalEventId, nrciot }, 'Erro ao atualizar eventos em background (não crítico)');
        }
      })();

      return;
    } else {
      // Atualizar evento como falha
      await createOrUpdateWebhookEvent(eventId, source, 'failed', resultado.mensagem, {
        nrciot: data.Manifest?.nrciot || null,
        etapa: 'backend_falha',
        erro: resultado.mensagem,
      });

      return res.status(400).json({
        Status: resultado.status,
        Mensagem: resultado.mensagem,
      });
    }
  } catch (error: any) {
    // Erro de validação Zod
    if (error.name === 'ZodError') {
      logger.warn({ errors: error.errors }, 'Erro de validação no schema CIOT');
      
      if (eventId) {
        await createOrUpdateWebhookEvent(
          eventId,
          source,
          'failed',
          'Dados inválidos - Erro de validação',
          {
            nrciot: req.body.Manifest?.nrciot || req.body.manifest?.nrciot || null,
            etapa: 'validacao_falha',
            erros: error.errors,
          }
        );
      }

      return res.status(400).json({
        Status: false,
        Mensagem: 'Dados inválidos',
        Erros: error.errors,
      });
    }

    logger.error({ error: error.message, stack: error.stack }, 'Erro ao processar ContasPagarCIOT');
    
    if (eventId) {
      await createOrUpdateWebhookEvent(
        eventId,
        source,
        'failed',
        `Erro interno: ${error.message}`,
        {
          nrciot: req.body.Manifest?.nrciot || req.body.manifest?.nrciot || null,
          etapa: 'erro_interno',
          erro: error.message,
        }
      );
    }

    return res.status(500).json({
      Status: false,
      Mensagem: 'Erro interno ao processar requisição',
      ...(process.env.NODE_ENV === 'development' && { detalhes: error.message }),
    });
  }
}

/**
 * Controller para cancelar ContasPagarCIOT
 * POST /api/CIOT/CancelarContasPagarCIOT?token=...
 */
export async function cancelarContasPagarCIOTController(req: Request, res: Response) {
  let eventId: string | null = null;
  const source = '/api/CIOT/CancelarContasPagarCIOT';

  try {
    if (req.body.manifest && !req.body.Manifest) {
      req.body.Manifest = req.body.manifest;
    }
    if (req.body.obscancelado !== undefined && req.body.Obscancelado === undefined) {
      req.body.Obscancelado = req.body.obscancelado;
    }
    if (req.body.dsusuariocanc !== undefined && req.body.DsUsuarioCan === undefined) {
      req.body.DsUsuarioCan = req.body.dsusuariocanc;
    }
    if (req.body.dsusuariocan !== undefined && req.body.DsUsuarioCan === undefined) {
      req.body.DsUsuarioCan = req.body.dsusuariocan;
    }

    // Gerar eventId baseado nos dados recebidos (pode ser undefined se não vier nrciot)
    const nrciotRaw = req.body.Manifest?.nrciot || req.body.manifest?.nrciot;
    eventId = generateCiotEventId(null, nrciotRaw);

    // Evento inicial: recebimento/pending
    await createOrUpdateWebhookEvent(eventId, source, 'pending', null, {
      nrciot: nrciotRaw || null,
      etapa: 'validacao',
      acao: 'cancelamento',
    });

    // Validar schema
    const data = cancelarContasPagarCIOTSchema.parse(req.body);

    logger.info({ nrciot: data.Manifest.nrciot }, 'Recebida requisição para cancelar ContasPagarCIOT');

    // Atualizar evento para processamento
    await createOrUpdateWebhookEvent(eventId, source, 'processing', null, {
      nrciot: data.Manifest.nrciot,
      etapa: 'processamento_cancelamento',
      acao: 'cancelamento',
    });

    // Processar cancelamento
    await cancelarContasPagarCIOT(
      data.Manifest.nrciot,
      data.Obscancelado,
      data.DsUsuarioCan
    );

    // Finalizar evento com sucesso
    await createOrUpdateWebhookEvent(eventId, source, 'processed', null, {
      nrciot: data.Manifest.nrciot,
      etapa: 'cancelamento_concluido',
      acao: 'cancelamento',
    });

    return res.status(200).json({
      Status: true,
      Mensagem: 'Registro cancelado com sucesso!',
    });
  } catch (error: any) {
    // Erro de validação Zod
    if (error.name === 'ZodError') {
      logger.warn({ errors: error.errors }, 'Erro de validação no schema de cancelamento CIOT');
      if (eventId) {
        await createOrUpdateWebhookEvent(eventId, source, 'failed', 'Dados inválidos - Erro de validação', {
          nrciot: req.body.Manifest?.nrciot || req.body.manifest?.nrciot || null,
          etapa: 'validacao_falha',
          acao: 'cancelamento',
          erros: error.errors,
        });
      }

      return res.status(400).json({
        Status: false,
        Mensagem: 'Dados inválidos',
        Erros: error.errors,
      });
    }

    logger.error({ error: error.message, stack: error.stack }, 'Erro ao cancelar ContasPagarCIOT');

    if (eventId) {
      await createOrUpdateWebhookEvent(
        eventId,
        source,
        'failed',
        `Erro interno: ${error.message}`,
        {
          nrciot: req.body.Manifest?.nrciot || req.body.manifest?.nrciot || null,
          etapa: 'erro_interno',
          acao: 'cancelamento',
          erro: error.message,
        }
      );
    }

    return res.status(500).json({
      Status: false,
      Mensagem: 'Erro interno ao processar cancelamento',
      ...(process.env.NODE_ENV === 'development' && { detalhes: error.message }),
    });
  }
}

