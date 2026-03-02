import { z } from 'zod';

// Schema para Corporation
export const corporationSchema = z.object({
  id: z.number(),
  person_id: z.number().optional(),
  nickname: z.string().optional(),
  cnpj: z.string().optional(),
});

// Schema para Receiver
export const receiverSchema = z.object({
  id: z.number(),
  name: z.string().optional(),
  type: z.string().optional(),
  cnpj: z.string().optional(),
  cpf: z.string().optional(),
  email: z.string().optional(),
  phone: z.string().optional(),
});

// Schema para AccountingPlanningManagement
export const accountingPlanningManagementSchema = z.object({
  id: z.number(),
  code_cache: z.string().optional(),
  name: z.string().optional(),
});

// Schema para CostCenters
export const costCentersSchema = z.object({
  id: z.number(),
  name: z.string().optional(),
});

// Schema para Installments
export const installmentsSchema = z.object({
  id: z.number(),
  position: z.number().optional(),
  due_date: z.string().optional(),
  value: z.number().optional(),
  interest_value: z.number().optional(),
  discount_value: z.number().optional(),
  payment_method: z.string().optional(),
  comments: z.string().optional(),
  payment_date: z.string().optional(),
});

// Schema para InvoiceItems
export const invoiceItemsSchema = z.object({
  id: z.number(),
  freight_id: z.number().optional(),
  cte_key: z.string().optional(),
  cte_number: z.number().optional(),
  cte_series: z.number().optional(),
  payer_name: z.string().optional(),
  draft_number: z.string().optional(),
  nfse_number: z.string().optional(),
  nfse_series: z.string().optional(),
  type: z.string().optional(),
  total: z.number().optional(),
});

// Schema para Data (dados principais da conta a pagar)
export const contasPagarDataSchema = z.object({
  id: z.number(),
  type: z.string().optional(),
  document: z.string().optional(),
  issue_date: z.string().optional(),
  due_date: z.string().optional(),
  value: z.number().optional(),
  installment_period: z.string().optional(),
  comments: z.string().optional(),
  cancelado: z.number().optional(),
  Obscancelado: z.string().optional(),
  DsUsuarioCancel: z.string().optional(),
  corporation: corporationSchema.optional(),
  receiver: receiverSchema.optional(),
  accounting_planning_management: accountingPlanningManagementSchema.optional(),
  cost_centers: costCentersSchema.optional(),
  installments: z.array(installmentsSchema).optional(),
  invoice_items: z.array(invoiceItemsSchema).optional(),
});

// Schema principal para ContasPagar
export const contasPagarSchema = z.object({
  installment_count: z.number().optional(),
  data: contasPagarDataSchema,
});

export type ContasPagar = z.infer<typeof contasPagarSchema>;
export type ContasPagarData = z.infer<typeof contasPagarDataSchema>;
export type Corporation = z.infer<typeof corporationSchema>;
export type Receiver = z.infer<typeof receiverSchema>;
export type AccountingPlanningManagement = z.infer<typeof accountingPlanningManagementSchema>;
export type CostCenters = z.infer<typeof costCentersSchema>;
export type Installments = z.infer<typeof installmentsSchema>;
export type InvoiceItems = z.infer<typeof invoiceItemsSchema>;

