import { env } from '../config/env';

export const isPostgresSafeMode = (): boolean => {
  return (
    env.DATABASE_URL.startsWith('postgresql://') &&
    !env.ENABLE_SQLSERVER_LEGACY &&
    !env.ENABLE_SENIOR_INTEGRATION &&
    !env.ENABLE_EXTERNAL_EXPORT &&
    !env.ENABLE_EXTERNAL_IMPORT
  );
};

export const buildWorkerBypassMetadata = (
  service: string,
  extra: Record<string, unknown> = {},
): Record<string, unknown> => {
  return {
    workerMode: 'postgres_local_safe',
    workerService: service,
    flags: {
      enableSqlServerLegacy: env.ENABLE_SQLSERVER_LEGACY,
      enableSeniorIntegration: env.ENABLE_SENIOR_INTEGRATION,
      enableExternalExport: env.ENABLE_EXTERNAL_EXPORT,
      enableExternalImport: env.ENABLE_EXTERNAL_IMPORT,
    },
    ...extra,
  };
};
