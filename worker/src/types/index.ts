export type WebhookEventStatus = 'pending' | 'processing' | 'processed' | 'failed';

export type EventType =
  | 'cte/autorizado'
  | 'cte/cancelado'
  | 'ctrb/ciot/base'
  | 'ctrb/ciot/parcelas'
  | 'faturas/pagar/criar'
  | 'faturas/pagar/baixar'
  | 'faturas/pagar/cancelar'
  | 'faturas/receber/criar'
  | 'faturas/receber/baixar'
  | 'nfse/autorizado'
  | 'pessoa/upsert'
  | 'contasPagar/upsert'
  | 'contasReceber/upsert';

export interface ProcessResult {
  success: boolean;
  error?: string;
  recordsProcessed?: number;
  integrationStatus?: 'pending' | 'integrated' | 'failed' | 'skipped';
  integrationTimeMs?: number;
  seniorId?: string;
  metadata?: Record<string, any>;
}
