import { PrismaClient } from '@prisma/client';
import { logger } from '../utils/logger';
import type { ContasReceberBaixa } from '../schemas/contasReceberBaixa';

const prisma = new PrismaClient();

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

// Helper para converter string de data para formato SQL DateTime
const parseDateTime = (dateStr: string | null | undefined): string => {
  if (!dateStr) return 'NULL';
  try {
    const date = new Date(dateStr);
    if (isNaN(date.getTime())) return 'NULL';
    return `'${date.toISOString().replace('T', ' ').substring(0, 19)}'`;
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
 * Insere Baixa de Contas a Receber
 */
export async function inserirContasReceberBaixa(
  baixa: ContasReceberBaixa
): Promise<{ status: boolean; mensagem: string }> {
  try {
    if (!baixa.installment_id) {
      return {
        status: false,
        mensagem: 'installment_id é obrigatório',
      };
    }

    const sql = `
      EXEC dbo.P_CONTAS_RECEBER_BAIXA_ESL_INCLUIR
        @installment_id = ${baixa.installment_id},
        @payment_date = ${parseDateTime(baixa.payment_date)},
        @payment_value = ${toSqlDecimal(baixa.payment_value)},
        @discount_value = ${toSqlDecimal(baixa.discount_value)},
        @interest_value = ${toSqlDecimal(baixa.interest_value)},
        @payment_method = ${toSqlValue(baixa.payment_method)},
        @bank_account = ${toSqlValue(baixa.bank_account)},
        @bankname = ${toSqlValue(baixa.bankname)},
        @accountnumber = ${toSqlValue(baixa.accountnumber)},
        @accountdigit = ${toSqlValue(baixa.accountdigit)},
        @comments = ${toSqlValue(baixa.comments)};
    `;

    await prisma.$executeRawUnsafe(sql);

    logger.info({ installment_id: baixa.installment_id }, 'Baixa de conta a receber inserida com sucesso');

    return {
      status: true,
      mensagem: 'Baixa cadastrada com sucesso!',
    };
  } catch (error: any) {
    logger.error(
      { error: error.message, installment_id: baixa.installment_id },
      'Erro ao inserir baixa de conta a receber'
    );
    return {
      status: false,
      mensagem: 'Baixa não inserida, favor verificar log!',
    };
  }
}

