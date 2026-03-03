import { z } from 'zod';

const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  PORT: z.string().optional(),
  DATABASE_URL: z
    .string()
    .min(1, 'DATABASE_URL é obrigatória')
    .refine(
      (val) => {
        // Aceitar formato de URL padrão ou formato de connection string do SQL Server (com ponto e vírgula)
        return (
          val.startsWith('sqlserver://') ||
          val.startsWith('postgresql://') ||
          val.startsWith('mysql://')
        );
      },
      { message: 'DATABASE_URL deve começar com sqlserver://, postgresql:// ou mysql://' },
    ),
  WEBHOOK_SECRET: z.string(),
  // Banco de dados Senior (produção: SOFTRAN_BRASILMAXI, homologação: SOFTRAN_BRASILMAXI_HML)
  SENIOR_DATABASE: z.string().default('SOFTRAN_BRASILMAXI'),
  // Flags de operação segura durante migração
  ENABLE_EXTERNAL_EXPORT: z
    .string()
    .default('false')
    .transform((v) => v.trim().toLowerCase() === 'true'),
  ENABLE_EXTERNAL_IMPORT: z
    .string()
    .default('false')
    .transform((v) => v.trim().toLowerCase() === 'true'),
  ENABLE_SENIOR_INTEGRATION: z
    .string()
    .default('false')
    .transform((v) => v.trim().toLowerCase() === 'true'),
  ENABLE_SQLSERVER_LEGACY: z
    .string()
    .default('false')
    .transform((v) => v.trim().toLowerCase() === 'true'),
});

export const env = envSchema.parse(process.env);
