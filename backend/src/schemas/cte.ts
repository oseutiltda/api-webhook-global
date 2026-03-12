import { z } from 'zod';

// Schema para CT-e baseado no CteRepository.cs
export const cteSchema = z.object({
  id: z.union([z.number().int().positive(), z.string()]).transform((val) => {
    // Aceitar número ou string e converter para número
    if (typeof val === 'string') {
      const parsed = parseInt(val, 10);
      if (isNaN(parsed) || parsed <= 0) {
        throw new Error('id deve ser um número positivo');
      }
      return parsed;
    }
    return val;
  }), // external_id
  authorization_number: z.union([z.number().int().positive(), z.string()]).transform((val) => {
    // Aceitar número ou string e converter para número
    if (typeof val === 'string') {
      const parsed = parseInt(val, 10);
      if (isNaN(parsed) || parsed <= 0) {
        throw new Error('authorization_number deve ser um número positivo');
      }
      return parsed;
    }
    return val;
  }),
  status: z.string().max(50),
  xml: z.string().min(1, 'XML não pode estar vazio'),
  event_xml: z.string().nullable().optional().or(z.literal('')),
});

// Schema para a requisição completa
// Aceita tanto formato aninhado { cte: {...} } quanto formato direto { id, authorization_number, ... }
export const inserirCteSchema = z.union([
  // Formato aninhado: { cte: {...} } ou { Cte: {...} }
  z
    .object({
      cte: cteSchema.optional(),
      Cte: cteSchema.optional(),
    })
    .refine((data) => data.cte || data.Cte, {
      message: 'CT-e deve ser fornecido (campo "cte" ou "Cte")',
      path: ['cte'],
    }),
  // Formato direto: { id, authorization_number, status, xml, event_xml }
  cteSchema,
]);

export type Cte = z.infer<typeof cteSchema>;
export type InserirCte = z.infer<typeof inserirCteSchema>;
