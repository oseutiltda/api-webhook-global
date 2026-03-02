import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import rateLimit from 'express-rate-limit';
import pinoHttp from 'pino-http';
import { errorHandler } from './middleware/error';
import webhookRouter from './routes/webhooks';
import dashboardRouter from './routes/dashboard';
import {
  verifyNfseToken,
  ensureNfseIdempotency,
  verifyCiotToken,
  verifyCteToken,
  verifyPessoaToken,
  verifyContasPagarToken,
  verifyContasReceberToken,
} from './middleware/auth';
import * as controller from './controllers/webhookController';
import * as ciotController from './controllers/ciotController';
import * as cteController from './controllers/cteController';
import * as pessoaController from './controllers/pessoaController';
import * as contasPagarController from './controllers/contasPagarController';
import * as contasReceberController from './controllers/contasReceberController';
import * as contasReceberBaixaController from './controllers/contasReceberBaixaController';
import { openApiSpec, renderSwaggerUiHtml } from './docs/openapi';
import { logger } from './utils/logger';

const app = express();

app.use(pinoHttp({ logger } as any));
app.use(helmet());
app.use(cors());

// Middleware específico para corrigir JSON mal formatado na rota de NFSe
import { nfseJsonFixer } from './middleware/nfseJsonFixer';
app.use(nfseJsonFixer());

// Express.json() com verificação para pular se body já foi parseado pelo nfseJsonFixer
const jsonParser = express.json({ limit: '2mb', strict: true });
app.use((req: express.Request, res: express.Response, next: express.NextFunction) => {
  // Se o body já foi parseado (por exemplo, pelo nfseJsonFixer), pular o parser padrão
  if (req.body && typeof req.body === 'object' && (req as any)._body) {
    return next();
  }
  // Caso contrário, usar o parser padrão
  jsonParser(req, res, next);
});

const limiter = rateLimit({ windowMs: 60 * 1000, max: 120 });
app.use('/webhooks', limiter);
app.use('/api/NFSe', limiter);
app.use('/api/CIOT', limiter);
app.use('/api/CTe', limiter);
app.use('/api/Pessoa', limiter);
app.use('/api/ContasPagar', limiter);
app.use('/api/ContasReceber', limiter);

// Health check simples (mantido para compatibilidade)
// A rota completa está em /api/health via dashboardRouter
app.get('/health', (_req, res) => res.json({ status: 'ok' }));

// Swagger/OpenAPI
app.get('/docs.json', (_req, res) => {
  res.setHeader('Content-Type', 'application/json');
  return res.status(200).json(openApiSpec);
});

app.get('/docs', (_req, res) => {
  // CSP específica para o Swagger UI em /docs (carregado via CDN + script inline de bootstrap).
  // Mantemos o restante da aplicação com a política padrão do helmet.
  res.setHeader(
    'Content-Security-Policy',
    [
      "default-src 'self'",
      "script-src 'self' 'unsafe-inline' https://unpkg.com",
      "style-src 'self' 'unsafe-inline' https://unpkg.com",
      "img-src 'self' data: https:",
      "font-src 'self' data: https:",
      "connect-src 'self' https://unpkg.com",
    ].join('; '),
  );
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  return res.status(200).send(renderSwaggerUiHtml('/docs.json'));
});

// Rotas específicas com token fixo via query parameter (antes do dashboard router para evitar conflitos)
// Rota específica para NFSe com token fixo via query parameter - insere na tabela nfse
app.post('/api/NFSe/InserirNFSe', verifyNfseToken, ensureNfseIdempotency, controller.nfseInserir);

// Rotas para CIOT com token fixo via query parameter
app.post(
  '/api/CIOT/InserirContasPagarCIOT',
  verifyCiotToken,
  ciotController.inserirContasPagarCIOTController,
);
app.post(
  '/api/CIOT/CancelarContasPagarCIOT',
  verifyCiotToken,
  ciotController.cancelarContasPagarCIOTController,
);

// Rotas para CT-e com token fixo via query parameter
app.post('/api/CTe/InserirCte', verifyCteToken, cteController.inserirCteController);

// Rotas para Pessoa com token fixo via query parameter
app.post('/api/Pessoa/InserirPessoa', verifyPessoaToken, pessoaController.inserirPessoaController);

// Rotas para ContasPagar com token fixo via query parameter
app.post(
  '/api/ContasPagar/InserirContasPagar',
  verifyContasPagarToken,
  contasPagarController.inserirContasPagarController,
);

// Rotas para ContasReceber com token fixo via query parameter
app.post(
  '/api/ContasReceber/InserirContasReceber',
  verifyContasReceberToken,
  contasReceberController.inserirContasReceberController,
);
app.post(
  '/api/ContasReceber/InserirContasReceberBaixa',
  verifyContasReceberToken,
  contasReceberBaixaController.inserirContasReceberBaixaController,
);

app.use('/webhooks', webhookRouter);
app.use('/api', dashboardRouter);

app.use(errorHandler);

export default app;
