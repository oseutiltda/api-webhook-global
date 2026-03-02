import type { Prisma, PrismaClient } from '@prisma/client';
import { logger } from './logger';
import { env } from '../config/env';

type PrismaExecutor = PrismaClient | Prisma.TransactionClient;

/**
 * Retorna o próximo número de sequência de controle global
 * Busca o MAX entre GTCCONCE (CT-e) e GTCConhe (NFSe) para garantir sequência única
 *
 * @param prisma - Instância do Prisma (PrismaClient ou TransactionClient)
 * @param seniorDatabase - Nome do banco Senior (padrão: valor de SENIOR_DATABASE)
 * @returns Próximo número de sequência de controle
 */
export async function obterProximoNrSeqControle(
  prisma: PrismaExecutor,
  seniorDatabase: string = env.SENIOR_DATABASE,
): Promise<number> {
  try {
    // Usar uma única query que busca o MAX entre ambas as tabelas
    // Isso é mais seguro e eficiente, evitando deadlocks e garantindo atomicidade
    const sql = `
      SELECT 
        ISNULL(MAX(MaxNrSeqControle), 0) AS MaxNrSeqControle
      FROM (
        SELECT MAX(NrSeqControle) AS MaxNrSeqControle
        FROM [${seniorDatabase}]..GTCCONCE WITH (UPDLOCK, HOLDLOCK)
        UNION ALL
        SELECT MAX(NrSeqControle) AS MaxNrSeqControle
        FROM [${seniorDatabase}]..GTCConhe WITH (UPDLOCK, HOLDLOCK)
      ) AS TabelasCombinadas
    `;

    const result = await prisma.$queryRawUnsafe<Array<{ MaxNrSeqControle: number }>>(sql);

    const maxGlobal = result?.[0]?.MaxNrSeqControle ?? 0;

    // Retornar o próximo número (máximo + 1, mínimo 1)
    const proximoNrSeqControle = maxGlobal > 0 ? maxGlobal + 1 : 1;

    logger.debug(
      { maxGlobal, proximoNrSeqControle, seniorDatabase },
      'NrSeqControle calculado (MAX entre GTCCONCE e GTCConhe)',
    );

    return proximoNrSeqControle;
  } catch (error: any) {
    logger.error(
      { error: error.message, seniorDatabase },
      'Erro ao buscar número de sequência de controle',
    );
    // Em caso de erro, retornar 1 como fallback seguro
    return 1;
  }
}
