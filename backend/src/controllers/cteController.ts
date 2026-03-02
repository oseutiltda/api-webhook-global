import type { Request, Response } from 'express';
import { inserirCteSchema } from '../schemas/cte';
import { inserirCte } from '../services/cteService';
import { logger } from '../utils/logger';
import { createOrUpdateWebhookEvent } from '../utils/webhookEvent';

/**
 * Controller para inserir CT-e
 * POST /api/CTe/InserirCte?token=...
 */
export async function inserirCteController(req: Request, res: Response) {
  let eventId: string | null = null;
  const source = '/api/CTe/InserirCte';
  
  try {
    // Validar schema (aceita formato aninhado ou direto)
    const data = inserirCteSchema.parse(req.body);

    // Normalizar: extrair cteData do formato aninhado ou usar diretamente
    let cteData: any;
    if ('cte' in data || 'Cte' in data) {
      // Formato aninhado: { cte: {...} } ou { Cte: {...} }
      cteData = (data as any).Cte || (data as any).cte;
      if (!cteData) {
        await createOrUpdateWebhookEvent(
          `cte-${Date.now()}`,
          source,
          'failed',
          'CT-e não fornecido',
          {
            etapa: 'validacao_falha',
          }
        );
        return res.status(400).json({
          Status: false,
          Mensagem: 'CT-e não fornecido',
        });
      }
    } else {
      // Formato direto: { id, authorization_number, status, xml, event_xml }
      cteData = data;
    }

    // Gerar eventId baseado nos dados
    const externalId = cteData.id;
    const authorizationNumber = cteData.authorization_number;
    eventId = externalId 
      ? `cte-${externalId}` 
      : authorizationNumber 
        ? `cte-auth-${authorizationNumber}-${Date.now()}`
        : `cte-${Date.now()}`;

    logger.info(
      {
        external_id: cteData.id,
        authorization_number: cteData.authorization_number,
        status: cteData.status,
      },
      'Recebida requisição para inserir CT-e'
    );

    // Criar/atualizar eventos em background (não bloqueia)
    createOrUpdateWebhookEvent(eventId, source, 'pending', null, {
      external_id: externalId || null,
      authorization_number: authorizationNumber || null,
      etapa: 'validacao',
    }).catch((err: any) => logger.warn({ error: err?.message, eventId }, 'Erro ao criar evento pending (não crítico)'));

    createOrUpdateWebhookEvent(eventId, source, 'processing', null, {
      external_id: cteData.id,
      authorization_number: cteData.authorization_number,
      status: cteData.status,
      etapa: 'processamento',
    }).catch((err: any) => logger.warn({ error: err?.message, eventId }, 'Erro ao criar evento processing (não crítico)'));

    // Processar inserção
    const resultado = await inserirCte(cteData);

    if (resultado.status) {
      // Retornar resposta IMEDIATAMENTE após processar CT-e
      // Operações de atualização de eventos serão feitas em background
      const responseData = {
        Status: resultado.status,
        Mensagem: resultado.mensagem,
      };

      // Determinar código HTTP: 201 Created para novos registros, 200 OK para atualizações
      const httpStatus = resultado.created === true ? 201 : 200;

      // Enviar resposta imediatamente
      res.status(httpStatus).json(responseData);

      // Atualizar evento como processado em background (não bloqueia a resposta)
      (async () => {
        try {
          await createOrUpdateWebhookEvent(eventId, source, 'processed', null, {
            external_id: cteData.id,
            authorization_number: cteData.authorization_number,
            status: cteData.status,
            cteId: resultado.cteId || null,
            etapa: 'backend_concluido',
            mensagem: resultado.mensagem,
          });
        } catch (bgError: any) {
          logger.error(
            { error: bgError?.message, eventId, external_id: cteData.id },
            'Erro ao atualizar eventos em background (não crítico)'
          );
        }
      })();

      return;
    } else {
      // Retornar resposta imediatamente mesmo em caso de falha
      const responseData = {
        Status: resultado.status,
        Mensagem: resultado.mensagem,
      };

      res.status(400).json(responseData);

      // Atualizar evento como falha em background (não bloqueia a resposta)
      createOrUpdateWebhookEvent(eventId, source, 'failed', resultado.mensagem, {
        external_id: cteData.id,
        authorization_number: cteData.authorization_number,
        status: cteData.status,
        etapa: 'backend_falha',
        mensagem: resultado.mensagem,
      }).catch((err: any) => logger.warn({ error: err?.message, eventId }, 'Erro ao atualizar evento failed (não crítico)'));

      return;
    }
  } catch (error: any) {
    // Erro de validação Zod
    if (error.name === 'ZodError') {
      const errorDetails = error.errors.map((err: any) => ({
        campo: err.path.join('.'),
        mensagem: err.message,
        valorRecebido: err.input,
      }));
      
      logger.warn({ 
        errors: error.errors,
        errorDetails,
        bodyKeys: Object.keys(req.body || {}),
      }, 'Erro de validação no schema CT-e');
      
      if (eventId) {
        await createOrUpdateWebhookEvent(
          eventId,
          source,
          'failed',
          `Dados inválidos - Erro de validação: ${errorDetails.map((e: any) => `${e.campo}: ${e.mensagem}`).join(', ')}`,
          {
            etapa: 'validacao_falha',
            erros: errorDetails,
            bodyKeys: Object.keys(req.body || {}),
          }
        );
      }

      return res.status(400).json({
        Status: false,
        Mensagem: 'Dados inválidos',
        Erros: errorDetails,
      });
    }

    logger.error({ error: error.message, stack: error.stack }, 'Erro ao processar CT-e');
    
    if (eventId) {
      await createOrUpdateWebhookEvent(
        eventId,
        source,
        'failed',
        `Erro interno: ${error.message}`,
        {
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

