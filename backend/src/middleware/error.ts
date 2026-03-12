import { Request, Response, NextFunction } from 'express';
import { ZodError } from 'zod';
import { captureSentryException } from '../config/sentry';
import { logger } from '../utils/logger';

export function errorHandler(err: any, _req: Request, res: Response, _next: NextFunction) {
  if (err instanceof ZodError) {
    return res.status(400).json({ error: 'ValidationError', details: err.flatten() });
  }

  // Tratar erros de parsing JSON (SyntaxError do Express)
  // O Express adiciona propriedade 'body' e 'status' ao erro de parsing JSON
  if (err instanceof SyntaxError && 'body' in err) {
    const jsonError = err as any;
    const bodyStr =
      typeof jsonError.body === 'string' ? jsonError.body : JSON.stringify(jsonError.body);

    // Tentar corrigir automaticamente valores sem aspas
    if (err.message.includes('Unexpected token') && bodyStr) {
      try {
        const { fixMalformedJson } = require('./jsonFixer');
        const correctedBody = fixMalformedJson(bodyStr);

        // Se a correção resultou em um JSON diferente, tentar parsear
        if (correctedBody !== bodyStr) {
          try {
            const parsedBody = JSON.parse(correctedBody);
            logger.info(
              {
                url: _req.url,
                originalPreview: bodyStr.substring(0, 200),
                correctedPreview: correctedBody.substring(0, 200),
              },
              'JSON corrigido automaticamente e parseado com sucesso',
            );

            // Armazenar o body corrigido para uso posterior
            _req.body = parsedBody;

            // Retornar sucesso para que o próximo middleware possa processar
            return _next();
          } catch (parseError: any) {
            logger.warn(
              {
                error: parseError.message,
                originalError: err.message,
              },
              'JSON corrigido mas ainda inválido após correção',
            );
          }
        }
      } catch (fixError: any) {
        logger.warn({ error: fixError.message }, 'Erro ao tentar corrigir JSON');
      }
    }

    const bodyPreview = bodyStr.substring(0, 500);
    logger.error(
      {
        err: {
          message: err.message,
          stack: err.stack,
          bodyPreview: bodyPreview,
          bodyLength: bodyStr.length,
        },
      },
      'Erro ao fazer parse do JSON - JSON mal formatado',
    );

    // Tentar extrair informações mais específicas do erro
    let detalhesEspecificos = err.message;
    if (err.message.includes('Unexpected token')) {
      const match = err.message.match(/Unexpected token ['"]([^'"]+)['"]/);
      if (match) {
        detalhesEspecificos = `Token inesperado encontrado: "${match[1]}". O sistema tentou corrigir automaticamente, mas ainda há problemas no JSON. Verifique se todos os valores de string estão entre aspas.`;
      } else {
        detalhesEspecificos =
          'Valores de string devem estar entre aspas (ex: "S/N" ao invés de S/N). O sistema tentou corrigir automaticamente, mas ainda há problemas no JSON.';
      }
    }

    return res.status(jsonError.status || 400).json({
      error: 'JSON inválido',
      mensagem:
        'O JSON enviado está mal formatado. O sistema tentou corrigir automaticamente, mas ainda há problemas. Verifique se todos os valores estão entre aspas.',
      detalhes: detalhesEspecificos,
      erroOriginal: err.message,
    });
  }

  const status = err.status || 500;
  const message = err.message || 'Internal Server Error';

  if (!(err instanceof ZodError) && !(err instanceof SyntaxError && 'body' in err)) {
    captureSentryException(err, {
      url: _req.url,
      method: _req.method,
      requestId: _req.id,
      statusCode: status,
    });
  }

  logger.error({ err }, 'Request error');
  return res.status(status).json({ error: message });
}
