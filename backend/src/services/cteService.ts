import { PrismaClient } from '@prisma/client';
import { logger } from '../utils/logger';
import type { Cte } from '../schemas/cte';

const prisma = new PrismaClient();

/**
 * Processa Inserção de CT-e
 * Insere diretamente na tabela cte (similar ao padrão NFSe)
 */
export async function inserirCte(
  data: Cte,
): Promise<{ status: boolean; mensagem: string; cteId?: number | null; created?: boolean }> {
  try {
    if (!data) {
      return {
        status: false,
        mensagem: 'CT-e não incluído, favor verificar lançamento!',
        cteId: null,
      };
    }

    logger.info(
      {
        external_id: data.id,
        authorization_number: data.authorization_number,
        status: data.status,
      },
      'Iniciando inserção de CT-e na tabela cte',
    );

    // Verificar se o model Cte existe no Prisma Client
    if (!prisma.cte) {
      logger.error(
        {},
        'Model Cte não encontrado no Prisma Client. Execute: npm run prisma:generate',
      );
      return {
        status: false,
        mensagem:
          'Erro de configuração: Model Cte não encontrado. Execute: npm run prisma:generate',
        cteId: null,
      };
    }

    // Verificar se já existe um CT-e com o mesmo external_id e authorization_number
    const existing = await prisma.cte.findFirst({
      where: {
        external_id: data.id,
        authorization_number: data.authorization_number,
      },
    });

    if (existing) {
      logger.info(
        {
          external_id: data.id,
          authorization_number: data.authorization_number,
          existingId: existing.id,
          statusAnterior: existing.status,
          statusNovo: data.status,
        },
        'CT-e já existe na base de dados. Atualizando registro...',
      );

      // Atualizar o registro existente
      // processed = false (0) para que o worker possa processar novamente
      const cte = await prisma.cte.update({
        where: {
          id: existing.id,
        },
        data: {
          status: data.status,
          xml: data.xml,
          event_xml: data.event_xml || null,
          processed: false, // false = 0 no banco, para o worker processar futuramente
          // updated_at será atualizado automaticamente pelo Prisma (@updatedAt)
        },
      });

      logger.info(
        {
          external_id: data.id,
          authorization_number: data.authorization_number,
          status: data.status,
          cteId: cte.id,
        },
        'CT-e atualizado com sucesso na tabela cte',
      );

      return {
        status: true,
        mensagem: 'Registro atualizado com sucesso!',
        cteId: cte.id,
        created: false, // Indica que foi atualizado (não criado)
      };
    }

    // Inserir diretamente na tabela cte
    // processed = false (0) para que o worker possa processar futuramente
    const cte = await prisma.cte.create({
      data: {
        external_id: data.id,
        authorization_number: data.authorization_number,
        status: data.status,
        xml: data.xml,
        event_xml: data.event_xml || null,
        processed: false, // false = 0 no banco, para o worker processar futuramente
      },
    });

    logger.info(
      {
        external_id: data.id,
        authorization_number: data.authorization_number,
        status: data.status,
        cteId: cte.id,
      },
      'CT-e criado com sucesso na tabela cte',
    );

    return {
      status: true,
      mensagem: 'Registro criado com sucesso!',
      cteId: cte.id,
      created: true, // Indica que foi criado (não atualizado)
    };
  } catch (error: any) {
    // Se for erro de model não encontrado ou incompatibilidade de schema (Prisma Client não gerado ou desatualizado)
    if (
      error?.message?.includes('cte') ||
      error?.message?.includes('Cte') ||
      error?.code === 'P2001' ||
      error?.code === 'P2032'
    ) {
      logger.error(
        {
          error: error.message,
          stack: error.stack,
          errorCode: error?.code,
          hint:
            error?.code === 'P2032'
              ? 'Schema Prisma desatualizado ou dados incompatíveis no banco. Execute: npm run prisma:generate e reconstrua o container'
              : 'Prisma Client precisa ser regenerado. Execute: npm run prisma:generate',
        },
        error?.code === 'P2032'
          ? 'Erro de incompatibilidade de schema no banco de dados (campo updated_at com valor NULL)'
          : 'Model Cte não encontrado no Prisma Client',
      );

      return {
        status: false,
        mensagem:
          error?.code === 'P2032'
            ? 'Erro de configuração: Schema Prisma desatualizado. Execute: npm run prisma:generate e reconstrua o container'
            : 'Erro de configuração: Model Cte não encontrado. Execute: npm run prisma:generate e reconstrua o container',
        cteId: null,
      };
    }

    // Se for erro de duplicação de chave única, tentar fazer UPDATE (race condition)
    if (error?.code === 'P2002') {
      logger.warn(
        {
          external_id: data?.id,
          authorization_number: data?.authorization_number,
          status: data?.status,
          error: error.message,
        },
        'CT-e já existe (erro de constraint única - race condition). Tentando atualizar...',
      );

      try {
        // Tentar encontrar e atualizar o registro existente
        const existing = await prisma.cte.findFirst({
          where: {
            external_id: data.id,
            authorization_number: data.authorization_number,
          },
        });

        if (existing) {
          const cte = await prisma.cte.update({
            where: {
              id: existing.id,
            },
            data: {
              status: data.status,
              xml: data.xml,
              event_xml: data.event_xml || null,
              processed: false,
            },
          });

          logger.info(
            {
              external_id: data.id,
              authorization_number: data.authorization_number,
              cteId: cte.id,
            },
            'CT-e atualizado com sucesso após race condition',
          );

          return {
            status: true,
            mensagem: 'Registro atualizado com sucesso!',
            cteId: cte.id,
            created: false, // Indica que foi atualizado (não criado)
          };
        }
      } catch (updateError: any) {
        logger.error(
          {
            error: updateError?.message,
            external_id: data?.id,
            authorization_number: data?.authorization_number,
          },
          'Erro ao tentar atualizar CT-e após race condition',
        );
      }

      return {
        status: false,
        mensagem:
          'Registro já existe na base de dados, mas não foi possível atualizar. Tente novamente.',
        cteId: null,
      };
    }

    logger.error(
      {
        error: error.message,
        stack: error.stack,
        errorCode: error?.code,
        errorName: error?.name,
        external_id: data?.id,
        authorization_number: data?.authorization_number,
        status: data?.status,
      },
      'Erro ao inserir CT-e',
    );

    return {
      status: false,
      mensagem: 'Registro não inserido, favor verificar log!',
      cteId: null,
    };
  }
}
