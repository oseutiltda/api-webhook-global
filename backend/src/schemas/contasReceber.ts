import { z } from 'zod';

// Schema para Corporation (mesmo de ContasPagar)
export const corporationSchema = z.object({
  id: z.number(),
  person_id: z.number().optional(),
  nickname: z.string().optional(),
  cnpj: z.string().optional(),
});

// Schema para Customer (similar ao Receiver, mas sempre person_type = "Customer")
export const customerSchema = z.object({
  id: z.number(),
  name: z.string().optional(),
  type: z.string().optional(),
  cnpj: z.string().optional(),
  cpf: z.string().optional(),
  email: z.string().optional(),
  phone: z.string().optional(),
});

// Schema para AccountingPlanningManagement (mesmo de ContasPagar)
export const accountingPlanningManagementSchema = z.object({
  id: z.number(),
  code_cache: z.string().optional(),
  name: z.string().optional(),
  value: z.number().optional(),
  total: z.number().optional(),
});

// Schema para Installments (mesmo de ContasPagar)
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

// Schema para InvoiceItems (similar ao de ContasPagar, mas sem freight_id)
export const invoiceItemsSchema = z.object({
  id: z.number(),
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

// Schema para Data (dados principais da conta a receber)
export const contasReceberDataSchema = z.object({
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
  DsUsuarioCan: z.string().optional(),
  corporation: corporationSchema.optional(),
  customer: customerSchema.optional(),
  accounting_planning_management: accountingPlanningManagementSchema.optional(),
  installments: z.array(installmentsSchema).optional(),
  invoice_items: z.array(invoiceItemsSchema).optional(),
});

// Schema principal para ContasReceber
export const contasReceberSchema = z.object({
  installment_count: z.number().optional(),
  data: contasReceberDataSchema,
});

export type ContasReceber = z.infer<typeof contasReceberSchema>;
export type ContasReceberData = z.infer<typeof contasReceberDataSchema>;
export type Corporation = z.infer<typeof corporationSchema>;
export type Customer = z.infer<typeof customerSchema>;
export type AccountingPlanningManagement = z.infer<typeof accountingPlanningManagementSchema>;
export type Installments = z.infer<typeof installmentsSchema>;
export type InvoiceItems = z.infer<typeof invoiceItemsSchema>;
