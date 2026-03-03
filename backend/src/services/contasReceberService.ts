import { PrismaClient } from '@prisma/client';
import { logger } from '../utils/logger';
import type { ContasReceber, Installments, InvoiceItems } from '../schemas/contasReceber';
import { createOrUpdateWebhookEvent } from '../utils/webhookEvent';
import { env } from '../config/env';

const prisma = new PrismaClient();
const IS_POSTGRES = env.DATABASE_URL.startsWith('postgresql://');
const LEGACY_DISABLED = !env.ENABLE_SENIOR_INTEGRATION;

// Helper para converter valores para SQL
const toSqlValue = (value: any): string => {
  if (value === null || value === undefined) return 'NULL';
  if (typeof value === 'string') {
    const escaped = value.replace(/'/g, "''");
    return `'${escaped}'`;
  }
  if (typeof value === 'number') return String(value);
  if (typeof value === 'boolean') return value ? '1' : '0';
  if (value instanceof Date) {
    return `'${value.toISOString().split('T')[0]}'`;
  }
  return `'${String(value)}'`;
};

// Helper para converter string de data para formato SQL
const parseDate = (dateStr: string | null | undefined): string => {
  if (!dateStr) return 'NULL';
  try {
    const date = new Date(dateStr);
    if (isNaN(date.getTime())) return 'NULL';
    return `'${date.toISOString().split('T')[0]}'`;
  } catch {
    return 'NULL';
  }
};

// Helper para converter número decimal
const toSqlDecimal = (value: number | null | undefined): string => {
  if (value === null || value === undefined) return 'NULL';
  return String(value);
};

/**
 * Insere Filial (Corporation)
 */
export async function inserirFilial(
  contasReceber: ContasReceber,
  tabelasInseridas: string[],
  tabelasFalhadas: Array<{ tabela: string; erro: string }>,
): Promise<number> {
  try {
    if (!contasReceber.data.corporation) {
      logger.warn('Corporation não informada');
      return 0;
    }

    const sql = `
      EXEC dbo.P_CONTAS_RECEBER_CORPORATION_ESL_INCLUIR
        @external_id = ${contasReceber.data.corporation.id},
        @person_id = ${contasReceber.data.corporation.person_id ?? 'NULL'},
        @nickname = ${toSqlValue(contasReceber.data.corporation.nickname)},
        @cnpj = ${toSqlValue(contasReceber.data.corporation.cnpj)};
    `;

    const result = await prisma.$queryRawUnsafe<Array<any>>(sql);
    if (result && result.length > 0) {
      const firstRow = result[0] as Record<string, any>;
      let id = 0;
      if (firstRow?.Id !== undefined) {
        id = Number(firstRow.Id);
      } else if (firstRow?.id !== undefined) {
        id = Number(firstRow.id);
      } else {
        const keys = Object.keys(firstRow) as Array<keyof typeof firstRow>;
        if (keys.length === 1) {
          const onlyKey = keys[0];
          if (onlyKey !== undefined) {
            const value = firstRow[onlyKey];
            if (typeof value === 'number') {
              id = value;
            } else {
              const numValue = Number(value);
              if (!isNaN(numValue)) {
                id = numValue;
              }
            }
          }
        }
      }
      if (id > 0) {
        tabelasInseridas.push('Corporation (Filial)');
        return id;
      }
    }
    tabelasFalhadas.push({
      tabela: 'Corporation (Filial)',
      erro: 'Stored procedure não retornou ID válido',
    });
    return 0;
  } catch (error: any) {
    logger.error({ error: error.message }, 'Erro ao inserir filial');
    tabelasFalhadas.push({
      tabela: 'Corporation (Filial)',
      erro: error.message || 'Erro desconhecido',
    });
    return 0;
  }
}

/**
 * Insere Cliente (Customer/Person) - sempre person_type = "Customer"
 */
export async function inserirCliente(
  contasReceber: ContasReceber,
  tabelasInseridas: string[],
  tabelasFalhadas: Array<{ tabela: string; erro: string }>,
): Promise<number> {
  try {
    if (!contasReceber.data.customer) {
      logger.warn('Customer não informado');
      return 0;
    }

    const sql = `
      EXEC dbo.P_CONTAS_RECEBER_PERSON_ESL_INCLUIR
        @external_id = ${contasReceber.data.customer.id},
        @name = ${toSqlValue(contasReceber.data.customer.name)},
        @type = ${toSqlValue(contasReceber.data.customer.type)},
        @cnpj = ${toSqlValue(contasReceber.data.customer.cnpj)},
        @cpf = ${toSqlValue(contasReceber.data.customer.cpf)},
        @person_type = ${toSqlValue('Customer')},
        @email = ${toSqlValue(contasReceber.data.customer.email)},
        @phone = ${toSqlValue(contasReceber.data.customer.phone)};
    `;

    const result = await prisma.$queryRawUnsafe<Array<any>>(sql);
    if (result && result.length > 0) {
      const firstRow = result[0] as Record<string, any>;
      let id = 0;
      if (firstRow?.Id !== undefined) {
        id = Number(firstRow.Id);
      } else if (firstRow?.id !== undefined) {
        id = Number(firstRow.id);
      } else {
        const keys = Object.keys(firstRow) as Array<keyof typeof firstRow>;
        if (keys.length === 1) {
          const onlyKey = keys[0];
          if (onlyKey !== undefined) {
            const value = firstRow[onlyKey];
            if (typeof value === 'number') {
              id = value;
            } else {
              const numValue = Number(value);
              if (!isNaN(numValue)) {
                id = numValue;
              }
            }
          }
        }
      }
      if (id > 0) {
        tabelasInseridas.push('Customer (Cliente)');
        return id;
      }
    }
    tabelasFalhadas.push({
      tabela: 'Customer (Cliente)',
      erro: 'Stored procedure não retornou ID válido',
    });
    return 0;
  } catch (error: any) {
    logger.error({ error: error.message }, 'Erro ao inserir cliente');
    tabelasFalhadas.push({
      tabela: 'Customer (Cliente)',
      erro: error.message || 'Erro desconhecido',
    });
    return 0;
  }
}

/**
 * Insere Conta Contábil (Accounting Planning Management)
 */
export async function inserirContaContabil(
  contasReceber: ContasReceber,
  tabelasInseridas: string[],
  tabelasFalhadas: Array<{ tabela: string; erro: string }>,
): Promise<number> {
  try {
    if (!contasReceber.data.accounting_planning_management) {
      logger.warn('Accounting Planning Management não informado');
      return 0;
    }

    const codeCache = contasReceber.data.accounting_planning_management.code_cache || '0';

    const sql = `
      EXEC dbo.P_CONTAS_RECEBER_ACOUNT_PLANNING_ESL_INCLUIR
        @external_id = ${contasReceber.data.accounting_planning_management.id},
        @code_cache = ${toSqlValue(codeCache)},
        @name = ${toSqlValue(contasReceber.data.accounting_planning_management.name)};
    `;

    const result = await prisma.$queryRawUnsafe<Array<any>>(sql);
    if (result && result.length > 0) {
      const firstRow = result[0] as Record<string, any>;
      let id = 0;
      if (firstRow?.Id !== undefined) {
        id = Number(firstRow.Id);
      } else if (firstRow?.id !== undefined) {
        id = Number(firstRow.id);
      } else {
        const keys = Object.keys(firstRow) as Array<keyof typeof firstRow>;
        if (keys.length === 1) {
          const onlyKey = keys[0];
          if (onlyKey !== undefined) {
            const value = firstRow[onlyKey];
            if (typeof value === 'number') {
              id = value;
            } else {
              const numValue = Number(value);
              if (!isNaN(numValue)) {
                id = numValue;
              }
            }
          }
        }
      }
      if (id > 0) {
        tabelasInseridas.push('Accounting Planning Management (Conta Contábil)');
        return id;
      }
    }
    tabelasFalhadas.push({
      tabela: 'Accounting Planning Management (Conta Contábil)',
      erro: 'Stored procedure não retornou ID válido',
    });
    return 0;
  } catch (error: any) {
    logger.error({ error: error.message }, 'Erro ao inserir conta contábil');
    tabelasFalhadas.push({
      tabela: 'Accounting Planning Management (Conta Contábil)',
      erro: error.message || 'Erro desconhecido',
    });
    return 0;
  }
}

/**
 * Verifica se fatura já existe
 */
const verificarFaturaExistente = async (
  external_id: number,
  document: string | null,
): Promise<{ id: number; external_id: number } | null> => {
  try {
    const whereClauses: string[] = [];
    if (external_id) {
      whereClauses.push(`external_id = ${external_id}`);
    }
    if (document) {
      whereClauses.push(`document = ${toSqlValue(document)}`);
    }

    if (whereClauses.length === 0) {
      return null;
    }

    const sql = `
      SELECT TOP 1 Id, external_id
      FROM dbo.credit_invoices WITH (NOLOCK)
      WHERE ${whereClauses.join(' OR ')}
    `;

    const result = await prisma.$queryRawUnsafe<Array<{ Id: number; external_id: number }>>(sql);
    if (result && result.length > 0 && result[0]?.Id) {
      return {
        id: Number(result[0].Id),
        external_id: Number(result[0].external_id),
      };
    }
    return null;
  } catch (error: any) {
    logger.warn(
      { error: error.message, external_id, document },
      'Erro ao verificar fatura existente',
    );
    return null;
  }
};

/**
 * Atualiza Fatura Principal
 */
const atualizarFatura = async (
  contasReceber: ContasReceber,
  idFatura: number,
  idFilial: number,
  idCliente: number,
  idContaContabil: number,
  tabelasInseridas: string[],
  tabelasFalhadas: Array<{ tabela: string; erro: string }>,
): Promise<void> => {
  try {
    const installmentCount = contasReceber.installment_count ?? 0;

    const sql = `
      UPDATE dbo.credit_invoices SET
        external_id = ${contasReceber.data.id},
        type = ${toSqlValue(contasReceber.data.type)},
        document = ${toSqlValue(contasReceber.data.document)},
        issue_date = ${parseDate(contasReceber.data.issue_date)},
        due_date = ${parseDate(contasReceber.data.due_date)},
        value = ${toSqlDecimal(contasReceber.data.value)},
        installment_period = ${toSqlValue(contasReceber.data.installment_period)},
        comments = ${toSqlValue(contasReceber.data.comments)},
        corporation_id = ${idFilial},
        customer_id = ${idCliente},
        accounting_planning_id = ${idContaContabil},
        installment_count = ${installmentCount},
        processed = 0,
        updated_at = GETDATE()
      WHERE Id = ${idFatura};
    `;

    await prisma.$executeRawUnsafe(sql);
    logger.info(
      { idFatura, document: contasReceber.data.document },
      'Fatura atualizada com sucesso',
    );
    if (!tabelasInseridas.includes('Credit Invoice (Fatura Principal)')) {
      tabelasInseridas.push('Credit Invoice (Fatura Principal)');
    }
  } catch (error: any) {
    logger.error({ error: error.message, idFatura }, 'Erro ao atualizar fatura');
    if (
      !tabelasFalhadas.some(
        (t) =>
          t.tabela === 'Credit Invoice (Fatura Principal)' && t.erro.includes(`Fatura ${idFatura}`),
      )
    ) {
      tabelasFalhadas.push({
        tabela: 'Credit Invoice (Fatura Principal)',
        erro: `Fatura ${idFatura}: ${error.message || 'Erro desconhecido'}`,
      });
    }
    throw error;
  }
};

/**
 * Insere Fatura Principal
 */
export async function inserirFatura(
  contasReceber: ContasReceber,
  idFilial: number,
  idCliente: number,
  idContaContabil: number,
  tabelasInseridas: string[],
  tabelasFalhadas: Array<{ tabela: string; erro: string }>,
): Promise<number> {
  try {
    const installmentCount = contasReceber.installment_count ?? 0;

    const sql = `
      EXEC dbo.P_CONTAS_RECEBER_ESL_INCLUIR
        @external_id = ${contasReceber.data.id},
        @type = ${toSqlValue(contasReceber.data.type)},
        @document = ${toSqlValue(contasReceber.data.document)},
        @issue_date = ${parseDate(contasReceber.data.issue_date)},
        @due_date = ${parseDate(contasReceber.data.due_date)},
        @value = ${toSqlDecimal(contasReceber.data.value)},
        @installment_period = ${toSqlValue(contasReceber.data.installment_period)},
        @comments = ${toSqlValue(contasReceber.data.comments)},
        @corporation_id = ${idFilial},
        @customer_id = ${idCliente},
        @accounting_planning_id = ${idContaContabil},
        @installment_count = ${installmentCount};
    `;

    const result = await prisma.$queryRawUnsafe<Array<any>>(sql);
    if (result && result.length > 0) {
      const firstRow = result[0] as Record<string, any>;
      let id = 0;
      if (firstRow?.Id !== undefined) {
        id = Number(firstRow.Id);
      } else if (firstRow?.id !== undefined) {
        id = Number(firstRow.id);
      } else {
        const keys = Object.keys(firstRow) as Array<keyof typeof firstRow>;
        if (keys.length === 1) {
          const onlyKey = keys[0];
          if (onlyKey !== undefined) {
            const value = firstRow[onlyKey];
            if (typeof value === 'number') {
              id = value;
            } else {
              const numValue = Number(value);
              if (!isNaN(numValue)) {
                id = numValue;
              }
            }
          }
        }
      }
      if (id > 0) {
        tabelasInseridas.push('Credit Invoice (Fatura Principal)');
        return id;
      }
    }
    tabelasFalhadas.push({
      tabela: 'Credit Invoice (Fatura Principal)',
      erro: 'Stored procedure não retornou ID válido',
    });
    return 0;
  } catch (error: any) {
    logger.error({ error: error.message }, 'Erro ao inserir fatura');
    tabelasFalhadas.push({
      tabela: 'Credit Invoice (Fatura Principal)',
      erro: error.message || 'Erro desconhecido',
    });
    return 0;
  }
}

/**
 * Insere Parcela (Installment)
 */
export async function inserirParcela(
  parcela: Installments,
  idFatura: number,
  tabelasInseridas: string[],
  tabelasFalhadas: Array<{ tabela: string; erro: string }>,
): Promise<number> {
  try {
    const sql = `
      EXEC dbo.P_CONTAS_RECEBER_INSTALLMENTS_ESL_INCLUIR
        @external_id = ${parcela.id},
        @credit_invoice_id = ${idFatura},
        @position = ${parcela.position ?? 'NULL'},
        @due_date = ${parseDate(parcela.due_date)},
        @value = ${toSqlDecimal(parcela.value)},
        @interest_value = ${toSqlDecimal(parcela.interest_value)},
        @discount_value = ${toSqlDecimal(parcela.discount_value)},
        @payment_method = ${toSqlValue(parcela.payment_method)},
        @comments = ${toSqlValue(parcela.comments)},
        @payment_date = ${parseDate(parcela.payment_date)};
    `;

    const result = await prisma.$queryRawUnsafe<Array<any>>(sql);
    if (result && result.length > 0) {
      const firstRow = result[0] as Record<string, any>;
      let id = 0;
      if (firstRow?.Id !== undefined) {
        id = Number(firstRow.Id);
      } else if (firstRow?.id !== undefined) {
        id = Number(firstRow.id);
      } else {
        const keys = Object.keys(firstRow) as Array<keyof typeof firstRow>;
        if (keys.length === 1) {
          const onlyKey = keys[0];
          if (onlyKey !== undefined) {
            const value = firstRow[onlyKey];
            if (typeof value === 'number') {
              id = value;
            } else {
              const numValue = Number(value);
              if (!isNaN(numValue)) {
                id = numValue;
              }
            }
          }
        }
      }
      if (id > 0) {
        if (!tabelasInseridas.includes('Credit Installments (Parcelas)')) {
          tabelasInseridas.push('Credit Installments (Parcelas)');
        }
        return id;
      }
    }
    if (!tabelasFalhadas.some((t) => t.tabela === 'Credit Installments (Parcelas)')) {
      tabelasFalhadas.push({
        tabela: 'Credit Installments (Parcelas)',
        erro: `Parcela ID ${parcela.id}: Stored procedure não retornou ID válido`,
      });
    }
    return 0;
  } catch (error: any) {
    logger.error(
      { error: error.message, parcelaId: parcela.id, idFatura },
      'Erro ao inserir parcela',
    );
    if (
      !tabelasFalhadas.some(
        (t) =>
          t.tabela === 'Credit Installments (Parcelas)' &&
          t.erro.includes(`Parcela ID ${parcela.id}`),
      )
    ) {
      tabelasFalhadas.push({
        tabela: 'Credit Installments (Parcelas)',
        erro: `Parcela ID ${parcela.id}: ${error.message || 'Erro desconhecido'}`,
      });
    }
    return 0;
  }
}

/**
 * Insere Parcela Gerenciamento (Credit Planning Management)
 */
export async function inserirParcelaGerenciamento(
  contasReceber: ContasReceber,
  idParcela: number,
  tabelasInseridas: string[],
  tabelasFalhadas: Array<{ tabela: string; erro: string }>,
): Promise<void> {
  try {
    if (!contasReceber.data.accounting_planning_management) {
      logger.warn('Accounting Planning Management não informado para parcela gerenciamento');
      return;
    }

    const codeCache = contasReceber.data.accounting_planning_management.code_cache || '0';
    const value = contasReceber.data.accounting_planning_management.value ?? 0;
    const total = contasReceber.data.accounting_planning_management.total ?? 0;

    const sql = `
      EXEC dbo.P_CONTAS_RECEBER_CREDIT_PLANNING_MANAGEMENTS_ESL_INCLUIR
        @external_id = ${contasReceber.data.accounting_planning_management.id},
        @credit_installment_id = ${idParcela},
        @code_cache = ${toSqlValue(codeCache)},
        @name = ${toSqlValue(contasReceber.data.accounting_planning_management.name)},
        @value = ${toSqlDecimal(value)},
        @total = ${toSqlDecimal(total)};
    `;

    await prisma.$executeRawUnsafe(sql);
    logger.info({ idParcela }, 'Parcela gerenciamento inserida com sucesso');
    if (!tabelasInseridas.includes('Credit Planning Management (Parcela Gerenciamento)')) {
      tabelasInseridas.push('Credit Planning Management (Parcela Gerenciamento)');
    }
  } catch (error: any) {
    logger.error({ error: error.message, idParcela }, 'Erro ao inserir parcela gerenciamento');
    if (
      !tabelasFalhadas.some(
        (t) =>
          t.tabela === 'Credit Planning Management (Parcela Gerenciamento)' &&
          t.erro.includes(`Parcela ${idParcela}`),
      )
    ) {
      tabelasFalhadas.push({
        tabela: 'Credit Planning Management (Parcela Gerenciamento)',
        erro: `Parcela ${idParcela}: ${error.message || 'Erro desconhecido'}`,
      });
    }
    // Não throw aqui, apenas loga o erro
  }
}

/**
 * Insere Item da Fatura (Invoice Item)
 */
export async function inserirFaturaItem(
  item: InvoiceItems,
  idFatura: number,
  tabelasInseridas: string[],
  tabelasFalhadas: Array<{ tabela: string; erro: string }>,
): Promise<void> {
  try {
    const total = item.total ?? 0;

    const sql = `
      EXEC dbo.P_CONTAS_RECEBER_INVOICE_ITEMS_ESL_INCLUIR
        @external_id = ${item.id},
        @credit_invoice_id = ${idFatura},
        @cte_key = ${toSqlValue(item.cte_key)},
        @cte_number = ${item.cte_number ?? 'NULL'},
        @cte_series = ${item.cte_series ?? 'NULL'},
        @payer_name = ${toSqlValue(item.payer_name)},
        @draft_number = ${toSqlValue(item.draft_number)},
        @nfse_number = ${toSqlValue(item.nfse_number)},
        @nfse_series = ${toSqlValue(item.nfse_series)},
        @type = ${toSqlValue(item.type)},
        @total = ${toSqlDecimal(total)};
    `;

    await prisma.$executeRawUnsafe(sql);
    logger.info({ itemId: item.id, idFatura }, 'Item da fatura inserido com sucesso');
    if (!tabelasInseridas.includes('Invoice Items (Itens da Fatura)')) {
      tabelasInseridas.push('Invoice Items (Itens da Fatura)');
    }
  } catch (error: any) {
    logger.error(
      { error: error.message, itemId: item.id, idFatura },
      'Erro ao inserir item da fatura',
    );
    if (
      !tabelasFalhadas.some(
        (t) =>
          t.tabela === 'Invoice Items (Itens da Fatura)' && t.erro.includes(`Item ID ${item.id}`),
      )
    ) {
      tabelasFalhadas.push({
        tabela: 'Invoice Items (Itens da Fatura)',
        erro: `Item ID ${item.id}: ${error.message || 'Erro desconhecido'}`,
      });
    }
    // Não throw aqui, apenas loga o erro
  }
}

/**
 * Cancela Conta a Receber
 */
export async function cancelarContasReceber(contasReceber: ContasReceber): Promise<void> {
  try {
    if (!contasReceber.data.document) {
      logger.warn('Document não informado para cancelamento');
      return;
    }

    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_CR_CANCELAMENTO_ALTERAR
        @DocumentESL = ${toSqlValue(contasReceber.data.document)},
        @Obscancelado = ${toSqlValue(contasReceber.data.Obscancelado)},
        @DsUsuarioCan = ${toSqlValue(contasReceber.data.DsUsuarioCan)};
    `;

    await prisma.$executeRawUnsafe(sql);
    logger.info({ document: contasReceber.data.document }, 'Conta a receber cancelada com sucesso');
  } catch (error: any) {
    logger.error(
      { error: error.message, document: contasReceber.data.document },
      'Erro ao cancelar conta a receber',
    );
    throw error;
  }
}

/**
 * Função principal para inserir Contas a Receber
 */
export async function inserirContasReceber(
  contasReceber: ContasReceber,
  eventId?: string | null,
  startTime?: number,
): Promise<{ status: boolean; mensagem: string; idFatura?: number; created?: boolean }> {
  // Rastreamento de tabelas inseridas e falhadas
  const tabelasInseridas: string[] = [];
  const tabelasFalhadas: Array<{ tabela: string; erro: string }> = [];
  const startProcessing = startTime || Date.now();

  try {
    if (!contasReceber.data) {
      return {
        status: false,
        mensagem: 'Dados da conta a receber não informados',
      };
    }

    // Migração Global: no modo PostgreSQL sem legado, persistir localmente e não executar SQL Server/Senior.
    if (IS_POSTGRES && LEGACY_DISABLED) {
      const faturaId = String(contasReceber.data.id);
      const numero = contasReceber.data.document || `CR-${faturaId}`;
      const emissao = contasReceber.data.issue_date || new Date().toISOString().slice(0, 10);
      const valor = Number(contasReceber.data.value || 0);
      const clienteCnpj =
        contasReceber.data.customer?.cnpj || contasReceber.data.corporation?.cnpj || '';

      if (contasReceber.data.cancelado === 1) {
        logger.info(
          {
            faturaId,
            document: numero,
            motivo: 'postgres_local_sem_legacy',
          },
          'Conta a receber marcada como cancelada em modo local (sem integração legada)',
        );

        if (eventId) {
          await createOrUpdateWebhookEvent(
            eventId,
            '/api/ContasReceber/InserirContasReceber',
            'processed',
            null,
            {
              integrationStatus: 'canceled',
              integrationMode: 'postgres_local_sem_legacy',
              document: numero,
              id: contasReceber.data.id,
            },
          );
        }

        return {
          status: true,
          mensagem: 'Conta a receber cancelada com sucesso! (modo local)',
          idFatura: contasReceber.data.id,
          created: false,
        };
      }

      if (contasReceber.data.cancelado !== 0 && contasReceber.data.cancelado !== undefined) {
        return {
          status: false,
          mensagem: 'Valor de cancelado inválido. Use 0 para inserir ou 1 para cancelar.',
        };
      }

      const existente = await prisma.faturaReceber.findUnique({ where: { id: faturaId } });
      const created = !existente;

      await prisma.$transaction(async (tx) => {
        await tx.faturaReceber.upsert({
          where: { id: faturaId },
          update: {
            clienteCnpj,
            numero,
            emissao,
            valor,
          },
          create: {
            id: faturaId,
            clienteCnpj,
            numero,
            emissao,
            valor,
          },
        });

        await tx.faturaReceberParcela.deleteMany({ where: { faturaId } });
        if (contasReceber.data.installments && contasReceber.data.installments.length > 0) {
          await tx.faturaReceberParcela.createMany({
            data: contasReceber.data.installments.map((p, index) => ({
              faturaId,
              posicao: p.position ?? index + 1,
              dueDate: p.due_date || emissao,
              valor: Number(p.value || 0),
              interestValue: p.interest_value !== undefined ? Number(p.interest_value) : null,
              discountValue: p.discount_value !== undefined ? Number(p.discount_value) : null,
              paymentMethod: p.payment_method || null,
              comments: p.comments || null,
              installmentId: p.id,
            })),
          });
        }

        await tx.faturaReceberItem.deleteMany({ where: { faturaId } });
        if (contasReceber.data.invoice_items && contasReceber.data.invoice_items.length > 0) {
          await tx.faturaReceberItem.createMany({
            data: contasReceber.data.invoice_items.map((item) => ({
              faturaId,
              cteKey: item.cte_key || null,
              cteNumber: item.cte_number !== undefined ? String(item.cte_number) : null,
              cteSeries: item.cte_series !== undefined ? String(item.cte_series) : null,
              payerName: item.payer_name || null,
              draftNumber: item.draft_number || null,
              nfseNumber: item.nfse_number || null,
              nfseSeries: item.nfse_series || null,
              total: item.total !== undefined ? Number(item.total) : null,
              type: item.type || null,
            })),
          });
        }
      });

      if (eventId) {
        await createOrUpdateWebhookEvent(
          eventId,
          '/api/ContasReceber/InserirContasReceber',
          'processed',
          null,
          {
            integrationStatus: 'integrated',
            integrationMode: 'postgres_local_sem_legacy',
            document: numero,
            id: contasReceber.data.id,
            idFatura: contasReceber.data.id,
            created,
          },
        );
      }

      logger.info(
        {
          faturaId,
          document: numero,
          created,
          motivo: 'postgres_local_sem_legacy',
        },
        'Contas a receber persistida localmente em PostgreSQL',
      );

      return {
        status: true,
        mensagem: created
          ? 'Registro criado com sucesso! (modo local)'
          : 'Registro atualizado com sucesso! (modo local)',
        idFatura: contasReceber.data.id,
        created,
      };
    }

    // Se cancelado == 1, apenas cancela
    if (contasReceber.data.cancelado === 1) {
      try {
        await cancelarContasReceber(contasReceber);
        tabelasInseridas.push('Cancelamento');

        // Atualizar evento WebhookEvent
        if (eventId) {
          await createOrUpdateWebhookEvent(
            eventId,
            '/api/ContasReceber/InserirContasReceber',
            'processed',
            null,
            {
              integrationStatus: 'canceled',
              document: contasReceber.data.document,
              id: contasReceber.data.id,
              tabelasInseridas: tabelasInseridas,
              resumo: {
                totalTabelas: tabelasInseridas.length,
                sucesso: tabelasInseridas.length,
                falhas: 0,
              },
            },
          );
        }

        return {
          status: true,
          mensagem: 'Conta a receber cancelada com sucesso!',
          created: false, // Cancelamento é uma atualização
        };
      } catch (error: any) {
        logger.error({ error: error.message }, 'Erro ao cancelar conta a receber');
        tabelasFalhadas.push({
          tabela: 'Cancelamento',
          erro: error.message || 'Erro desconhecido',
        });

        if (eventId) {
          await createOrUpdateWebhookEvent(
            eventId,
            '/api/ContasReceber/InserirContasReceber',
            'failed',
            error.message || 'Erro ao cancelar conta a receber',
            {
              integrationStatus: 'failed',
              document: contasReceber.data.document,
              id: contasReceber.data.id,
              tabelasFalhadas: tabelasFalhadas,
            },
          );
        }

        return {
          status: false,
          mensagem: 'Erro ao cancelar conta a receber',
        };
      }
    }

    // Se cancelado == 0 ou não informado, insere normalmente
    if (contasReceber.data.cancelado !== 0 && contasReceber.data.cancelado !== undefined) {
      return {
        status: false,
        mensagem: 'Valor de cancelado inválido. Use 0 para inserir ou 1 para cancelar.',
      };
    }

    // Verificar se fatura já existe
    const faturaExistente = await verificarFaturaExistente(
      contasReceber.data.id,
      contasReceber.data.document || null,
    );

    let isUpdate = false;

    // Inserir Filial
    const idFilial = await inserirFilial(contasReceber, tabelasInseridas, tabelasFalhadas);
    if (idFilial <= 0) {
      logger.warn('Falha ao inserir filial');
    }

    // Inserir Cliente
    const idCliente = await inserirCliente(contasReceber, tabelasInseridas, tabelasFalhadas);
    if (idCliente <= 0) {
      logger.warn('Falha ao inserir cliente');
    }

    // Inserir Conta Contábil
    const idContaContabil = await inserirContaContabil(
      contasReceber,
      tabelasInseridas,
      tabelasFalhadas,
    );
    if (idContaContabil <= 0) {
      logger.warn('Falha ao inserir conta contábil');
    }

    let idFatura: number;

    // Se já existe, fazer UPDATE (Síncrono para garantir ID)
    if (faturaExistente) {
      isUpdate = true;
      logger.info(
        {
          idFatura: faturaExistente.id,
          external_id: faturaExistente.external_id,
          document: contasReceber.data.document,
        },
        'Fatura já existe na tabela destino, realizando UPDATE',
      );

      idFatura = faturaExistente.id;
      isUpdate = true;

      // Atualizar fatura
      await atualizarFatura(
        contasReceber,
        idFatura,
        idFilial,
        idCliente,
        idContaContabil,
        tabelasInseridas,
        tabelasFalhadas,
      );

      // Deletar registros relacionados na ordem correta (respeitando foreign keys)
      // IMPORTANTE: Deletar primeiro credit_planning_managements (que referencia credit_installments),
      // depois credit_installments, e por fim os itens

      // 1. Deletar parcelas gerenciamento primeiro (tabela que referencia credit_installments)
      try {
        await prisma.$executeRawUnsafe(`
          DELETE cpm FROM dbo.credit_planning_managements cpm
          INNER JOIN dbo.credit_installments ci ON cpm.credit_installment_id = ci.Id
          WHERE ci.credit_invoice_id = ${idFatura};
        `);
        logger.debug(
          { idFatura },
          'Gerenciamentos de parcelas antigos deletados da tabela credit_planning_managements',
        );
      } catch (deleteError: any) {
        logger.warn(
          { error: deleteError.message, idFatura },
          'Erro ao deletar gerenciamentos de parcelas antigos (não crítico)',
        );
      }

      // 2. Deletar parcelas antigas para reinserir com os novos dados (após deletar gerenciamentos)
      try {
        await prisma.$executeRawUnsafe(
          `DELETE FROM dbo.credit_installments WHERE credit_invoice_id = ${idFatura};`,
        );
        logger.debug({ idFatura }, 'Parcelas antigas deletadas da tabela credit_installments');
      } catch (deleteError: any) {
        logger.warn(
          { error: deleteError.message, idFatura },
          'Erro ao deletar parcelas antigas (não crítico)',
        );
      }

      // 3. Deletar itens existentes para reinserir
      // Como não temos stored procedure para deletar, tentamos DELETE direto na tabela correta
      // Se falhar, não é crítico pois os itens serão reinseridos
      try {
        // Tentar com nome de tabela que pode estar no banco (credit_invoice_items ou invoice_items)
        try {
          await prisma.$executeRawUnsafe(
            `DELETE FROM dbo.credit_invoice_items WHERE credit_invoice_id = ${idFatura};`,
          );
          logger.debug({ idFatura }, 'Itens antigos deletados da tabela credit_invoice_items');
        } catch (e: any) {
          // Se falhar, tentar outro nome possível
          await prisma.$executeRawUnsafe(
            `DELETE FROM dbo.invoice_items WHERE credit_invoice_id = ${idFatura};`,
          );
          logger.debug({ idFatura }, 'Itens antigos deletados da tabela invoice_items');
        }
      } catch (deleteError: any) {
        // Se ambos falharem, apenas logar como warning (não crítico, os itens serão reinseridos)
        logger.warn(
          { error: deleteError.message, idFatura },
          'Erro ao deletar itens antigos (não crítico - itens serão reinseridos)',
        );
      }
    } else {
      // Inserir Fatura (nova)
      idFatura = await inserirFatura(
        contasReceber,
        idFilial,
        idCliente,
        idContaContabil,
        tabelasInseridas,
        tabelasFalhadas,
      );

      // Garantir que processed = 0 para que o worker processe
      if (idFatura > 0) {
        try {
          await prisma.$executeRawUnsafe(
            `UPDATE dbo.credit_invoices SET processed = 0 WHERE Id = ${idFatura}`,
          );
        } catch (e) {
          logger.warn(
            { idFatura, error: (e as Error).message },
            'Erro ao forçar processed = 0 após insert',
          );
        }
      }
    }

    if (idFatura <= 0) {
      const mensagemErro =
        tabelasFalhadas.length > 0
          ? `Fatura não incluída. ${tabelasFalhadas.map((t) => `${t.tabela}: ${t.erro}`).join(', ')}`
          : 'Fatura não incluída, favor verificar lançamento!';

      if (eventId) {
        await createOrUpdateWebhookEvent(
          eventId,
          '/api/ContasReceber/InserirContasReceber',
          'failed',
          mensagemErro,
          {
            integrationStatus: 'failed',
            document: contasReceber.data.document,
            id: contasReceber.data.id,
            tabelasInseridas: tabelasInseridas,
            tabelasFalhadas: tabelasFalhadas,
            resumo: {
              totalTabelas: tabelasInseridas.length + tabelasFalhadas.length,
              sucesso: tabelasInseridas.length,
              falhas: tabelasFalhadas.length,
            },
          },
        );
      }

      return {
        status: false,
        mensagem: mensagemErro,
      };
    }

    // Processar Itens e Parcelas em Background (Assíncrono)
    // Fire-and-forget para não bloquear o retorno do cabeçalho
    processarItensEParcelas(
      contasReceber,
      idFatura,
      eventId,
      isUpdate,
      tabelasInseridas,
      tabelasFalhadas,
      startProcessing,
    ).catch((err) => {
      logger.error(
        { eventId, error: err.message },
        'Erro no processamento assíncrono de itens/parcelas (Contas Receber)',
      );
    });

    // Retorno Síncrono Imediato
    return {
      status: true,
      mensagem: isUpdate ? 'Registro atualizado com sucesso!' : 'Registro criado com sucesso!',
      idFatura,
      created: !isUpdate,
    };
  } catch (error: any) {
    logger.error({ error: error.message, stack: error.stack }, 'Erro ao inserir conta a receber');

    // Atualizar evento WebhookEvent com erro
    if (eventId) {
      const mensagemErro = error.message || 'Erro ao inserir conta a receber';
      const metadata: any = {
        integrationStatus: 'failed',
        document: contasReceber.data?.document,
        id: contasReceber.data?.id,
        erro: mensagemErro,
      };

      if (tabelasInseridas.length > 0) {
        metadata.tabelasInseridas = tabelasInseridas;
      }
      if (tabelasFalhadas.length > 0) {
        metadata.tabelasFalhadas = tabelasFalhadas;
      }

      await createOrUpdateWebhookEvent(
        eventId,
        '/api/ContasReceber/InserirContasReceber',
        'failed',
        mensagemErro,
        metadata,
      );
    }

    return {
      status: false,
      mensagem: 'Registro não inserido, favor verificar log!',
    };
  }
}

/**
 * Processa Itens e Parcelas em Background (Contas Receber)
 */
async function processarItensEParcelas(
  contasReceber: ContasReceber,
  idFatura: number,
  eventId: string | null | undefined,
  isUpdate: boolean,
  tabelasInseridas: string[],
  tabelasFalhadas: Array<{ tabela: string; erro: string }>,
  startProcessing: number,
) {
  // Inserir Parcelas (se informadas)
  if (contasReceber.data.installments && contasReceber.data.installments.length > 0) {
    for (const parcela of contasReceber.data.installments) {
      const idParcela = await inserirParcela(parcela, idFatura, tabelasInseridas, tabelasFalhadas);
      if (idParcela > 0) {
        // Inserir Parcela Gerenciamento para cada parcela
        await inserirParcelaGerenciamento(
          contasReceber,
          idParcela,
          tabelasInseridas,
          tabelasFalhadas,
        );
      }
    }
  }

  // Inserir Itens da Fatura (se informados)
  if (contasReceber.data.invoice_items && contasReceber.data.invoice_items.length > 0) {
    for (const item of contasReceber.data.invoice_items) {
      await inserirFaturaItem(item, idFatura, tabelasInseridas, tabelasFalhadas);
    }
  }

  // Atualizar evento WebhookEvent com informações detalhadas das tabelas
  if (eventId) {
    const temFalhas = tabelasFalhadas.length > 0;

    let mensagemErro: string | null = null;
    if (temFalhas) {
      const tabelasOk =
        tabelasInseridas.length > 0
          ? `Tabelas inseridas/atualizadas com sucesso: ${tabelasInseridas.join(', ')}. `
          : '';
      const tabelasErro = `Tabelas com erro: ${tabelasFalhadas.map((t) => `${t.tabela} (${t.erro})`).join(', ')}.`;
      mensagemErro = tabelasOk + tabelasErro;
    }

    const metadata: any = {
      idFatura: idFatura,
      document: contasReceber.data.document,
      id: contasReceber.data.id,
      installmentCount: contasReceber.data.installments?.length || 0,
      invoiceItemsCount: contasReceber.data.invoice_items?.length || 0,
      etapa: isUpdate ? 'backend_atualizado' : 'backend_inserido',
      operacao: isUpdate ? 'UPDATE' : 'INSERT',
      tabelasInseridas: tabelasInseridas,
      processingTimeMs: Date.now() - startProcessing,
      resumo: {
        totalTabelas: tabelasInseridas.length + tabelasFalhadas.length,
        sucesso: tabelasInseridas.length,
        falhas: tabelasFalhadas.length,
      },
    };

    if (temFalhas) {
      metadata.tabelasFalhadas = tabelasFalhadas;
    }

    await createOrUpdateWebhookEvent(
      eventId,
      '/api/ContasReceber/InserirContasReceber',
      'processed',
      mensagemErro,
      metadata,
    );
  }
}
