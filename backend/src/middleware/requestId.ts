import { Request, Response, NextFunction } from 'express';
import { v4 as uuid } from 'uuid';

function normalizeRequestId(headerValue: unknown): string | null {
  if (typeof headerValue === 'string' && headerValue.trim().length > 0) {
    return headerValue.trim();
  }

  if (Array.isArray(headerValue)) {
    const first = headerValue.find((value) => typeof value === 'string' && value.trim().length > 0);
    if (first) return first.trim();
  }

  return null;
}

export function requestIdMiddleware(req: Request, _res: Response, next: NextFunction) {
  const headerRequestId = normalizeRequestId(req.headers['x-request-id']);
  req.id = headerRequestId || uuid();
  _res.setHeader('x-request-id', req.id);
  next();
}
