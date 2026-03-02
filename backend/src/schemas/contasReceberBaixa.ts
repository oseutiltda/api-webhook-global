import { z } from 'zod';

// Schema para ContasReceberBaixa
export const contasReceberBaixaSchema = z.object({
  installment_id: z.number(),
  payment_date: z.string().optional(),
  payment_value: z.number().optional(),
  discount_value: z.number().optional(),
  interest_value: z.number().optional(),
  payment_method: z.string().optional(),
  bank_account: z.string().optional(),
  bankname: z.string().optional(),
  accountnumber: z.string().optional(),
  accountdigit: z.string().optional(),
  comments: z.string().optional(),
});

export type ContasReceberBaixa = z.infer<typeof contasReceberBaixaSchema>;

