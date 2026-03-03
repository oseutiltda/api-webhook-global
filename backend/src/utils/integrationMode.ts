import { env } from '../config/env';

export const isPostgresSafeMode = (): boolean => {
  return (
    env.DATABASE_URL.startsWith('postgresql://') &&
    !env.ENABLE_SENIOR_INTEGRATION &&
    !env.ENABLE_SQLSERVER_LEGACY &&
    !env.ENABLE_EXTERNAL_EXPORT &&
    !env.ENABLE_EXTERNAL_IMPORT
  );
};

export const buildBypassMetadata = (
  service: string,
  extra: Record<string, unknown> = {},
): Record<string, unknown> => {
  return {
    integrationMode: 'postgres_local_safe',
    service,
    flags: {
      enableSeniorIntegration: env.ENABLE_SENIOR_INTEGRATION,
      enableSqlServerLegacy: env.ENABLE_SQLSERVER_LEGACY,
      enableExternalExport: env.ENABLE_EXTERNAL_EXPORT,
      enableExternalImport: env.ENABLE_EXTERNAL_IMPORT,
    },
    ...extra,
  };
};
