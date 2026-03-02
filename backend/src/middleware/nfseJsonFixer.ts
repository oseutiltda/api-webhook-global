import { Request, Response, NextFunction } from 'express';
import { logger } from '../utils/logger';
import { fixMalformedJson } from './jsonFixer';

/**
 * Middleware específico para a rota de NFSe que corrige JSON mal formatado
 * Captura o body raw, corrige valores sem aspas, e faz o parsing manual
 */
export function nfseJsonFixer() {
  return (req: Request, res: Response, next: NextFunction) => {
    // Apenas aplicar na rota de NFSe
    if (!req.url.includes('/api/NFSe/InserirNFSe')) {
      return next();
    }

    // Apenas para requisições JSON
    if (!req.headers['content-type']?.includes('application/json')) {
      return next();
    }

    // Se o body já foi parseado, pular
    if (req.body && typeof req.body === 'object') {
      return next();
    }

    // Capturar o body raw
    const chunks: Buffer[] = [];
    
    req.on('data', (chunk: Buffer) => {
      chunks.push(chunk);
    });

    req.on('end', () => {
      try {
        const rawBody = Buffer.concat(chunks).toString('utf8');
        const originalBody = rawBody;
        
        // Corrigir valores sem aspas
        const correctedBody = fixMalformedJson(originalBody);
        
        if (correctedBody !== originalBody) {
          logger.info({ 
            url: req.url,
            originalPreview: originalBody.substring(0, 300),
            correctedPreview: correctedBody.substring(0, 300)
          }, 'JSON da NFSe corrigido automaticamente - valores sem aspas foram ajustados');
        }

        // Parsear o JSON corrigido
        try {
          req.body = JSON.parse(correctedBody);
          // Marcar que o body já foi parseado para evitar que express.json() tente novamente
          (req as any)._body = true;
          logger.debug({ url: req.url }, 'JSON da NFSe parseado com sucesso após correção');
          return next();
        } catch (parseError: any) {
          logger.error({ 
            error: parseError.message,
            url: req.url,
            bodyPreview: correctedBody.substring(0, 500)
          }, 'Erro ao parsear JSON da NFSe mesmo após correção');
          
          // Retornar erro detalhado
          return res.status(400).json({
            error: 'JSON inválido',
            mensagem: 'O JSON enviado está mal formatado. O sistema tentou corrigir automaticamente, mas ainda há problemas.',
            detalhes: `Erro de parsing: ${parseError.message}. Verifique se todos os valores de string estão entre aspas.`,
            erroOriginal: parseError.message
          });
        }
      } catch (error: any) {
        logger.error({ error: error.message, url: req.url }, 'Erro ao processar JSON da NFSe');
        return res.status(400).json({
          error: 'Erro ao processar JSON',
          mensagem: 'Ocorreu um erro ao tentar corrigir o JSON da requisição.',
          detalhes: error.message
        });
      }
    });

    req.on('error', (error: Error) => {
      logger.error({ error: error.message, url: req.url }, 'Erro ao ler body da requisição NFSe');
      return res.status(400).json({
        error: 'Erro ao ler requisição',
        mensagem: 'Ocorreu um erro ao ler o corpo da requisição.',
        detalhes: error.message
      });
    });
  };
}
