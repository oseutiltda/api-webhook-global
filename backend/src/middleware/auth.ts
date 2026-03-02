import type { Request, Response, NextFunction } from 'express';
import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

const DEFAULT_API_TOKEN = process.env.API_FIXED_TOKEN || '';
const NFSE_FIXED_TOKEN = process.env.NFSE_FIXED_TOKEN || DEFAULT_API_TOKEN;
const CIOT_FIXED_TOKEN = process.env.CIOT_FIXED_TOKEN || DEFAULT_API_TOKEN;
const CTE_FIXED_TOKEN = process.env.CTE_FIXED_TOKEN || DEFAULT_API_TOKEN;
const PESSOA_FIXED_TOKEN = process.env.PESSOA_FIXED_TOKEN || DEFAULT_API_TOKEN;
const CONTAS_PAGAR_FIXED_TOKEN = process.env.CONTAS_PAGAR_FIXED_TOKEN || DEFAULT_API_TOKEN;
const CONTAS_RECEBER_FIXED_TOKEN = process.env.CONTAS_RECEBER_FIXED_TOKEN || DEFAULT_API_TOKEN;

const hasValidFixedToken = (
  providedToken: string | undefined,
  expectedToken: string,
  res: Response,
): boolean => {
  if (!expectedToken) {
    res.status(500).json({ error: 'Token da rota não configurado no ambiente' });
    return false;
  }
  if (!providedToken || providedToken !== expectedToken) {
    res.status(401).json({ error: 'Token inválido ou ausente' });
    return false;
  }
  return true;
};

export function verifyWebhookSecret(req: Request, res: Response, next: NextFunction) {
  const provided = req.headers['x-webhook-secret'] as string | undefined;
  const expected = process.env.WEBHOOK_SECRET;
  if (!expected) return res.status(500).json({ error: 'WEBHOOK_SECRET não configurado' });
  if (!provided || provided !== expected) return res.status(401).json({ error: 'Não autorizado' });
  return next();
}

export async function ensureIdempotency(req: Request, res: Response, next: NextFunction) {
  const eventId = (req.body && (req.body.id || req.headers['x-event-id'])) as string | undefined;
  if (!eventId) return res.status(400).json({ error: 'Id do evento ausente' });
  const source = (req.path || 'unknown').slice(0, 190);
  const exists = await prisma.webhookEvent.findUnique({ where: { id: eventId } }).catch(() => null);
  if (exists) return res.status(200).json({ status: 'duplicate_ignored' });
  await prisma.webhookEvent.create({ data: { id: eventId, source } });
  return next();
}

export function verifyNfseToken(req: Request, res: Response, next: NextFunction) {
  const providedToken = req.query.token as string | undefined;
  if (!hasValidFixedToken(providedToken, NFSE_FIXED_TOKEN, res)) {
    return;
  }
  return next();
}

export function verifyCiotToken(req: Request, res: Response, next: NextFunction) {
  const providedToken = req.query.token as string | undefined;
  if (!hasValidFixedToken(providedToken, CIOT_FIXED_TOKEN, res)) {
    return;
  }
  return next();
}

export function verifyCteToken(req: Request, res: Response, next: NextFunction) {
  const providedToken = req.query.token as string | undefined;
  if (!hasValidFixedToken(providedToken, CTE_FIXED_TOKEN, res)) {
    return;
  }
  return next();
}

export function verifyPessoaToken(req: Request, res: Response, next: NextFunction) {
  // Aceitar tanto 'token' quanto 'Token' (case-insensitive)
  const providedToken = (req.query.token || req.query.Token) as string | undefined;
  if (!hasValidFixedToken(providedToken, PESSOA_FIXED_TOKEN, res)) {
    return;
  }
  return next();
}

export function verifyContasPagarToken(req: Request, res: Response, next: NextFunction) {
  // Aceitar tanto 'token' quanto 'Token' (case-insensitive)
  const providedToken = (req.query.token || req.query.Token) as string | undefined;
  if (!hasValidFixedToken(providedToken, CONTAS_PAGAR_FIXED_TOKEN, res)) {
    return;
  }
  return next();
}

export function verifyContasReceberToken(req: Request, res: Response, next: NextFunction) {
  // Aceitar tanto 'token' quanto 'Token' (case-insensitive)
  const providedToken = (req.query.token || req.query.Token) as string | undefined;
  if (!hasValidFixedToken(providedToken, CONTAS_RECEBER_FIXED_TOKEN, res)) {
    return;
  }
  return next();
}

// Middleware específico para idempotência de NFSe que gera o ID baseado no body
export async function ensureNfseIdempotency(req: Request, res: Response, next: NextFunction) {
  try {
    const body = req.body;
    if (!body || !body.infNFeCte) {
      return res.status(400).json({ error: 'Body inválido' });
    }

    const n = body.infNFeCte;
    const prestadorCnpj = n.prestadorServico?.identificacaoPrestador?.cnpj;
    const numeroNfse = n.numero;

    if (!prestadorCnpj || !numeroNfse) {
      return res
        .status(400)
        .json({ error: 'CNPJ do prestador ou número da NFSe ausente no payload' });
    }

    // Verificar idempotência diretamente na tabela nfse
    // Se já existe uma NFSe com o mesmo número e CNPJ do prestador, é duplicata
    const exists = await prisma.nfse
      .findFirst({
        where: {
          NumeroNfse: numeroNfse,
          CnpjIdentPrestador: prestadorCnpj,
        },
      })
      .catch(() => null);

    if (exists) {
      return res.status(200).json({ status: 'duplicate_ignored', message: 'NFSe já existe' });
    }

    return next();
  } catch (error: any) {
    // Se houver erro, continua para permitir o processamento
    // (pode ser que a tabela ainda não esteja totalmente sincronizada)
    console.error('Erro ao verificar idempotência NFSe:', error.message);
    return next();
  }
}
