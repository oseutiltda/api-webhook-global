import { z } from 'zod';

const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  DATABASE_URL: z
    .string()
    .min(1, 'DATABASE_URL é obrigatória. Configure no arquivo .env ou docker-compose.yml')
    .refine(
      (val) => {
        // Aceitar formato de URL padrão ou formato de connection string do SQL Server (com ponto e vírgula)
        return (
          val.startsWith('sqlserver://') ||
          val.startsWith('postgresql://') ||
          val.startsWith('mysql://')
        );
      },
      {
        message: 'DATABASE_URL deve começar com sqlserver://, postgresql:// ou mysql://',
      },
    ),
  WORKER_INTERVAL_MS: z.string().default('5000'),
  WORKER_BATCH_SIZE: z.string().default('10'),
  WORKER_MAX_RETRIES: z.string().default('3'),
  ENABLE_WORKER: z
    .string()
    .default('true')
    .transform((v) => v.trim().toLowerCase() === 'true'),
  CIOT_WORKER_BATCH_SIZE: z.string().default('3'),
  CIOT_SOURCE_DATABASE: z.string().default('AFS_INTEGRADOR'),
  // Banco de dados Senior (produção: SOFTRAN_BRASILMAXI, homologação: SOFTRAN_BRASILMAXI_HML)
  SENIOR_DATABASE: z.string().default('SOFTRAN_BRASILMAXI'),
  // Lista de serviços do worker habilitados, separados por vírgula (ex: "NFSE,CIOT").
  // Se vazio ou não definido, todos os serviços são considerados habilitados.
  ENABLED_WORKER_SERVICES: z.string().optional(),
  // Quando false, desativa rotinas legadas que executam SQL Server raw/procedures.
  // Em migração para PostgreSQL, manter false até converter as integrações.
  ENABLE_SQLSERVER_LEGACY: z
    .string()
    .default('false')
    .transform((v) => v.trim().toLowerCase() === 'true'),
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
});

let env: z.infer<typeof envSchema>;

try {
  env = envSchema.parse(process.env);
} catch (error: any) {
  if (error instanceof z.ZodError) {
    console.error('❌ Erro de configuração de variáveis de ambiente:');
    error.issues.forEach((err: z.ZodIssue) => {
      console.error(`  - ${err.path.join('.')}: ${err.message}`);
      // Mostrar o valor atual (mascarado) para ajudar no diagnóstico
      if (err.path[0] === 'DATABASE_URL') {
        const currentValue = process.env.DATABASE_URL;
        if (!currentValue) {
          console.error('    ⚠️  DATABASE_URL está vazia ou não foi definida');
        } else if (currentValue.length > 0) {
          // Mostrar apenas os primeiros caracteres para não expor credenciais
          const masked = currentValue.substring(0, 30) + '...';
          console.error(`    Valor atual: ${masked}`);
        }
      }
    });
    console.error('\n💡 Soluções:');
    console.error('   1. Verifique se o arquivo .env existe na raiz do projeto');
    console.error('   2. Verifique se DATABASE_URL está configurada no .env');
    console.error(
      '   3. Se a senha contém caracteres especiais (@, :, /, etc), codifique-os usando URL encoding',
    );
    console.error('   4. Exemplo de .env:');
    console.error(
      '      DATABASE_URL="sqlserver://usuario:senha@servidor:1433?database=AFS_INTEGRADOR&encrypt=true&trustServerCertificate=true"',
    );
    process.exit(1);
  }
  throw error;
}

export { env };
