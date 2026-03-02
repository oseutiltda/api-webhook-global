import { PrismaClient } from '@prisma/client';
import { logger } from '../utils/logger';
import { createOrUpdateWebhookEvent } from '../utils/webhookEvent';
import type { ContasPagar, Installments, InvoiceItems } from '../schemas/contasPagar';

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
export async function inserirFilial(contasPagar: ContasPagar): Promise<number> {
  try {
    if (!contasPagar.data.corporation) {
      logger.warn('Corporation não informada');
      return 0;
    }

    const sql = `
      EXEC dbo.P_CONTAS_PAGAR_CORPORATION_ESL_INCLUIR
        @external_id = ${contasPagar.data.corporation.id},
        @person_id = ${contasPagar.data.corporation.person_id ?? 'NULL'},
        @nickname = ${toSqlValue(contasPagar.data.corporation.nickname)},
        @cnpj = ${toSqlValue(contasPagar.data.corporation.cnpj)};
    `;

    const result = await prisma.$queryRawUnsafe<Array<any>>(sql);
    if (result && result.length > 0) {
      // Tentar diferentes formatos de retorno
      const firstRow = result[0] as Record<string, any>;
      if (firstRow?.Id !== undefined) {
        return Number(firstRow.Id);
      }
      if (firstRow?.id !== undefined) {
        return Number(firstRow.id);
      }
      // Se retornar um valor escalar direto
      const keys = Object.keys(firstRow) as Array<keyof typeof firstRow>;
      if (keys.length === 1) {
        const onlyKey = keys[0];
        if (onlyKey !== undefined) {
          const value = firstRow[onlyKey];
          if (typeof value === 'number') {
            return value;
          }
          const numValue = Number(value);
          if (!isNaN(numValue)) {
            return numValue;
          }
        }
      }
    }
    return 0;
  } catch (error: any) {
    logger.error({ error: error.message }, 'Erro ao inserir filial');
    return 0;
  }
}

/**
 * Determina o tipo de pessoa baseado no type da conta a pagar
 */
function determinarTipoPessoa(type: string | undefined): string {
  if (!type) return 'Agent';

  const tipoMap: Record<string, string> = {
    'Accounting::Debit::AirlineBilling': 'Airline',
    'Accounting::Debit::AgentBilling': 'Agent',
    'Accounting::Debit::CarrierBilling': 'Carrier',
    'Accounting::Debit::AggregateBilling': 'Aggregate',
    'Accounting::Debit::SellerBilling': 'Seller',
    'Accounting::Debit::DriverBilling': 'Driver',
    'Accounting::Debit::SupplierBilling': 'Supplier',
    'Accounting::Debit::CustomerBilling': 'Customer',
  };

  return tipoMap[type] || 'Agent';
}

/**
 * Insere Fornecedor (Receiver/Person)
 */
export async function inserirFornecedor(contasPagar: ContasPagar): Promise<number> {
  try {
    if (!contasPagar.data.receiver) {
      logger.warn('Receiver não informado');
      return 0;
    }

    const tipoPessoa = determinarTipoPessoa(contasPagar.data.type);

    const sql = `
      EXEC dbo.P_CONTAS_PAGAR_PERSON_ESL_INCLUIR
        @external_id = ${contasPagar.data.receiver.id},
        @name = ${toSqlValue(contasPagar.data.receiver.name)},
        @type = ${toSqlValue(contasPagar.data.receiver.type)},
        @cnpj = ${toSqlValue(contasPagar.data.receiver.cnpj)},
        @cpf = ${toSqlValue(contasPagar.data.receiver.cpf)},
        @person_type = ${toSqlValue(tipoPessoa)},
        @email = ${toSqlValue(contasPagar.data.receiver.email)},
        @phone = ${toSqlValue(contasPagar.data.receiver.phone)};
    `;

    const result = await prisma.$queryRawUnsafe<Array<any>>(sql);
    if (result && result.length > 0) {
      // Tentar diferentes formatos de retorno
      const firstRow = result[0] as Record<string, any>;
      if (firstRow?.Id !== undefined) {
        return Number(firstRow.Id);
      }
      if (firstRow?.id !== undefined) {
        return Number(firstRow.id);
      }
      // Se retornar um valor escalar direto
      const keys = Object.keys(firstRow) as Array<keyof typeof firstRow>;
      if (keys.length === 1) {
        const onlyKey = keys[0];
        if (onlyKey !== undefined) {
          const value = firstRow[onlyKey];
          if (typeof value === 'number') {
            return value;
          }
          const numValue = Number(value);
          if (!isNaN(numValue)) {
            return numValue;
          }
        }
      }
    }
    return 0;
  } catch (error: any) {
    logger.error({ error: error.message }, 'Erro ao inserir fornecedor');
    return 0;
  }
}

/**
 * Insere Conta Contábil (Accounting Planning Management)
 */
export async function inserirContaContabil(contasPagar: ContasPagar): Promise<number> {
  try {
    if (!contasPagar.data.accounting_planning_management) {
      logger.warn('Accounting Planning Management não informado');
      return 0;
    }

    const codeCache = contasPagar.data.accounting_planning_management.code_cache || '0';

    const sql = `
      EXEC dbo.P_CONTAS_PAGAR_ACOUNT_PLANNING_ESL_INCLUIR
        @external_id = ${contasPagar.data.accounting_planning_management.id},
        @code_cache = ${toSqlValue(codeCache)},
        @name = ${toSqlValue(contasPagar.data.accounting_planning_management.name)};
    `;

    const result = await prisma.$queryRawUnsafe<Array<any>>(sql);
    if (result && result.length > 0) {
      // Tentar diferentes formatos de retorno
      const firstRow = result[0] as Record<string, any>;
      if (firstRow?.Id !== undefined) {
        return Number(firstRow.Id);
      }
      if (firstRow?.id !== undefined) {
        return Number(firstRow.id);
      }
      // Se retornar um valor escalar direto
      const keys = Object.keys(firstRow) as Array<keyof typeof firstRow>;
      if (keys.length === 1) {
        const onlyKey = keys[0];
        if (onlyKey !== undefined) {
          const value = firstRow[onlyKey];
          if (typeof value === 'number') {
            return value;
          }
          const numValue = Number(value);
          if (!isNaN(numValue)) {
            return numValue;
          }
        }
      }
    }
    return 0;
  } catch (error: any) {
    logger.error({ error: error.message }, 'Erro ao inserir conta contábil');
    return 0;
  }
}

/**
 * Insere Centro de Custo
 */
export async function inserirCentroCusto(contasPagar: ContasPagar): Promise<number> {
  try {
    if (!contasPagar.data.cost_centers) {
      logger.warn('Cost Centers não informado');
      return 0;
    }

    const sql = `
      EXEC dbo.P_CONTAS_PAGAR_CENTRO_CUSTO_ESL_INCLUIR
        @external_id = ${contasPagar.data.cost_centers.id},
        @name = ${toSqlValue(contasPagar.data.cost_centers.name)};
    `;

    const result = await prisma.$queryRawUnsafe<Array<any>>(sql);
    if (result && result.length > 0) {
      // Tentar diferentes formatos de retorno
      const firstRow = result[0] as Record<string, any>;
      if (firstRow?.Id !== undefined) {
        return Number(firstRow.Id);
      }
      if (firstRow?.id !== undefined) {
        return Number(firstRow.id);
      }
      // Se retornar um valor escalar direto
      const keys = Object.keys(firstRow) as Array<keyof typeof firstRow>;
      if (keys.length === 1) {
        const onlyKey = keys[0];
        if (onlyKey !== undefined) {
          const value = firstRow[onlyKey];
          if (typeof value === 'number') {
            return value;
          }
          const numValue = Number(value);
          if (!isNaN(numValue)) {
            return numValue;
          }
        }
      }
    }
    return 0;
  } catch (error: any) {
    logger.error({ error: error.message }, 'Erro ao inserir centro de custo');
    return 0;
  }
}

/**
 * Insere Associação de Centro de Custo com Fatura
 */
export async function inserirCentroCustoAssociacao(idFatura: number, idCentroCusto: number): Promise<void> {
  try {
    if (idFatura <= 0 || idCentroCusto <= 0) {
      logger.warn({ idFatura, idCentroCusto }, 'IDs inválidos para associação de centro de custo');
      return;
    }

    const sql = `
      EXEC dbo.P_CONTAS_PAGAR_CENTRO_CUSTO_DEBITO_ESL_INCLUIR
        @debit_invoice_id = ${idFatura},
        @cost_center_id = ${idCentroCusto};
    `;

    await prisma.$executeRawUnsafe(sql);
    logger.info({ idFatura, idCentroCusto }, 'Centro de custo associado com sucesso');
  } catch (error: any) {
    logger.error({ error: error.message, idFatura, idCentroCusto }, 'Erro ao associar centro de custo');
    throw error;
  }
}

/**
 * Insere Fatura Principal
 */
export async function inserirFatura(
  contasPagar: ContasPagar,
  idFilial: number,
  idFornecedor: number,
  idContaContabil: number
): Promise<number> {
  try {
    const installmentCount = contasPagar.installment_count ?? 0;

    const sql = `
      EXEC dbo.P_CONTAS_PAGAR_ESL_INCLUIR
        @external_id = ${contasPagar.data.id},
        @type = ${toSqlValue(contasPagar.data.type)},
        @document = ${toSqlValue(contasPagar.data.document)},
        @issue_date = ${parseDate(contasPagar.data.issue_date)},
        @due_date = ${parseDate(contasPagar.data.due_date)},
        @value = ${toSqlDecimal(contasPagar.data.value)},
        @installment_period = ${toSqlValue(contasPagar.data.installment_period)},
        @comments = ${toSqlValue(contasPagar.data.comments)},
        @corporation_id = ${idFilial},
        @receiver_id = ${idFornecedor},
        @accounting_planning_id = ${idContaContabil},
        @installment_count = ${installmentCount};
    `;

    const result = await prisma.$queryRawUnsafe<Array<any>>(sql);
    if (result && result.length > 0) {
      // Tentar diferentes formatos de retorno
      const firstRow = result[0] as Record<string, any>;
      if (firstRow?.Id !== undefined) {
        return Number(firstRow.Id);
      }
      if (firstRow?.id !== undefined) {
        return Number(firstRow.id);
      }
      // Se retornar um valor escalar direto
      const keys = Object.keys(firstRow) as Array<keyof typeof firstRow>;
      if (keys.length === 1) {
        const onlyKey = keys[0];
        if (onlyKey !== undefined) {
          const value = firstRow[onlyKey];
          if (typeof value === 'number') {
            return value;
          }
          const numValue = Number(value);
          if (!isNaN(numValue)) {
            return numValue;
          }
        }
      }
    }
    return 0;
  } catch (error: any) {
    logger.error({ error: error.message }, 'Erro ao inserir fatura');
    return 0;
  }
}

/**
 * Insere Parcela (Installment)
 */
export async function inserirParcela(parcela: Installments, idFatura: number): Promise<number> {
  try {
    const sql = `
      EXEC dbo.P_CONTAS_PAGAR_INSTALLMENTS_ESL_INCLUIR
        @external_id = ${parcela.id},
        @debit_invoice_id = ${idFatura},
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
      // Tentar diferentes formatos de retorno
      const firstRow = result[0] as Record<string, any>;
      if (firstRow?.Id !== undefined) {
        return Number(firstRow.Id);
      }
      if (firstRow?.id !== undefined) {
        return Number(firstRow.id);
      }
      // Se retornar um valor escalar direto
      const keys = Object.keys(firstRow) as Array<keyof typeof firstRow>;
      if (keys.length === 1) {
        const onlyKey = keys[0];
        if (onlyKey !== undefined) {
          const value = firstRow[onlyKey];
          if (typeof value === 'number') {
            return value;
          }
          const numValue = Number(value);
          if (!isNaN(numValue)) {
            return numValue;
          }
        }
      }
    }
    return 0;
  } catch (error: any) {
    logger.error({ error: error.message, parcelaId: parcela.id, idFatura }, 'Erro ao inserir parcela');
    return 0;
  }
}

/**
 * Insere Item da Fatura (Invoice Item)
 */
export async function inserirFaturaItem(item: InvoiceItems, idFatura: number): Promise<void> {
  try {
    const total = item.total ?? 0;

    const sql = `
      EXEC dbo.P_CONTAS_PAGAR_DEBIT_INVOICE_ITEMS_ESL_INCLUIR
        @external_id = ${item.id},
        @debit_invoice_id = ${idFatura},
        @freight_id = ${item.freight_id ?? 'NULL'},
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
  } catch (error: any) {
    logger.error({ error: error.message, itemId: item.id, idFatura }, 'Erro ao inserir item da fatura');
    throw error;
  }
}

/**
 * Cancela Conta a Pagar
 */
export async function cancelarContasPagar(contasPagar: ContasPagar): Promise<void> {
  try {
    if (!contasPagar.data.document) {
      logger.warn('Document não informado para cancelamento');
      return;
    }

    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_CP_CANCELAMENTO_ALTERAR
        @DocumentESL = ${toSqlValue(contasPagar.data.document)},
        @Obscancelado = ${toSqlValue(contasPagar.data.Obscancelado)},
        @DsUsuarioCancel = ${toSqlValue(contasPagar.data.DsUsuarioCancel)};
    `;

    await prisma.$executeRawUnsafe(sql);
    logger.info({ document: contasPagar.data.document }, 'Conta a pagar cancelada com sucesso');
  } catch (error: any) {
    logger.error({ error: error.message, document: contasPagar.data.document }, 'Erro ao cancelar conta a pagar');
    throw error;
  }
}

/**
 * Função principal para inserir Contas a Pagar
 */
export async function inserirContasPagar(
  contasPagar: ContasPagar,
  eventId?: string | null,
  startTime?: number
): Promise<{ status: boolean; mensagem: string; idFatura?: number; created?: boolean }> {
  const tabelasInseridas: string[] = [];
  const tabelasFalhadas: Array<{ tabela: string; erro: string }> = [];
  let isUpdate = false;
  let idFatura = 0;
  const startProcessing = startTime || Date.now();

  try {
    if (!contasPagar.data) {
      return {
        status: false,
        mensagem: 'Dados da conta a pagar não informados',
      };
    }

    // Verificar se já existe
    const verificarFaturaExistente = async (
      external_id: number,
      document: string | null
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
          FROM dbo.debit_invoices WITH (NOLOCK)
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
        logger.warn({ error: error.message, external_id, document }, 'Erro ao verificar fatura existente');
        return null;
      }
    };

    const faturaExistente = await verificarFaturaExistente(
      contasPagar.data.id,
      contasPagar.data.document || null
    );

    if (faturaExistente) {
      isUpdate = true;
      idFatura = faturaExistente.id;
    }

    // Se cancelado == 1, apenas cancela
    if (contasPagar.data.cancelado === 1) {
      await cancelarContasPagar(contasPagar);
      return {
        status: true,
        mensagem: 'Conta a pagar cancelada com sucesso!',
      };
    }

    // Se cancelado == 0 ou não informado, insere normalmente
    if (contasPagar.data.cancelado !== 0 && contasPagar.data.cancelado !== undefined) {
      return {
        status: false,
        mensagem: 'Valor de cancelado inválido. Use 0 para inserir ou 1 para cancelar.',
      };
    }

    // Inserir Filial
    let idFilial = 0;
    try {
      idFilial = await inserirFilial(contasPagar);
      if (idFilial > 0) {
        tabelasInseridas.push('Corporation (Filial)');
      } else {
        tabelasFalhadas.push({ tabela: 'Corporation (Filial)', erro: 'ID não retornado' });
      }
    } catch (error: any) {
      tabelasFalhadas.push({ tabela: 'Corporation (Filial)', erro: error.message || 'Erro desconhecido' });
    }

    // Inserir Fornecedor
    let idFornecedor = 0;
    try {
      idFornecedor = await inserirFornecedor(contasPagar);
      if (idFornecedor > 0) {
        tabelasInseridas.push('Person (Fornecedor)');
      } else {
        tabelasFalhadas.push({ tabela: 'Person (Fornecedor)', erro: 'ID não retornado' });
      }
    } catch (error: any) {
      tabelasFalhadas.push({ tabela: 'Person (Fornecedor)', erro: error.message || 'Erro desconhecido' });
    }

    // Inserir Conta Contábil
    let idContaContabil = 0;
    try {
      idContaContabil = await inserirContaContabil(contasPagar);
      if (idContaContabil > 0) {
        tabelasInseridas.push('Accounting Planning (Conta Contábil)');
      } else {
        tabelasFalhadas.push({ tabela: 'Accounting Planning (Conta Contábil)', erro: 'ID não retornado' });
      }
    } catch (error: any) {
      tabelasFalhadas.push({ tabela: 'Accounting Planning (Conta Contábil)', erro: error.message || 'Erro desconhecido' });
    }

    // Se é UPDATE, deletar dados antigos primeiro
    if (isUpdate && idFatura > 0) {
      try {
        // Deletar parcelas antigas
        await prisma.$executeRawUnsafe(`DELETE FROM dbo.debit_installments WHERE debit_invoice_id = ${idFatura};`);
        // Deletar itens antigos
        await prisma.$executeRawUnsafe(`DELETE FROM dbo.debit_invoice_items WHERE debit_invoice_id = ${idFatura};`);
        // Deletar associação de centro de custo
        await prisma.$executeRawUnsafe(`DELETE FROM dbo.debit_invoice_cost_centers WHERE debit_invoice_id = ${idFatura};`);
      } catch (error: any) {
        logger.warn({ error: error.message, idFatura }, 'Erro ao deletar dados antigos (não crítico)');
      }
    }

    // Inserir/Atualizar Fatura (Síncrono para garantir ID)
    try {
      if (isUpdate) {
        // UPDATE direto na tabela usando SQL
        const installmentCount = contasPagar.installment_count ?? 0;
        
        const sql = `
          UPDATE dbo.debit_invoices SET
            external_id = ${contasPagar.data.id},
            type = ${toSqlValue(contasPagar.data.type)},
            document = ${toSqlValue(contasPagar.data.document)},
            issue_date = ${parseDate(contasPagar.data.issue_date)},
            due_date = ${parseDate(contasPagar.data.due_date)},
            value = ${toSqlDecimal(contasPagar.data.value)},
            installment_period = ${toSqlValue(contasPagar.data.installment_period)},
            comments = ${toSqlValue(contasPagar.data.comments)},
            corporation_id = ${idFilial || 'NULL'},
            receiver_id = ${idFornecedor || 'NULL'},
            accounting_planning_id = ${idContaContabil || 'NULL'},
            installment_count = ${installmentCount},
            processed = 0,
            updated_at = GETDATE()
          WHERE Id = ${idFatura};
        `;

        await prisma.$executeRawUnsafe(sql);
        logger.info({ idFatura, document: contasPagar.data.document }, 'Fatura atualizada com sucesso');
        tabelasInseridas.push('Debit Invoice (Fatura)');
      } else {
        // INSERT via stored procedure
        idFatura = await inserirFatura(contasPagar, idFilial, idFornecedor, idContaContabil);
        if (idFatura > 0) {
          tabelasInseridas.push('Debit Invoice (Fatura)');
          
          // Garantir que processed = 0 para que o worker processe
          try {
            await prisma.$executeRawUnsafe(`UPDATE dbo.debit_invoices SET processed = 0 WHERE Id = ${idFatura}`);
          } catch (e) {
            logger.warn({ idFatura, error: (e as Error).message }, 'Erro ao forçar processed = 0 após insert');
          }
        } else {
          tabelasFalhadas.push({ tabela: 'Debit Invoice (Fatura)', erro: 'ID não retornado' });
        }
      }
    } catch (error: any) {
      tabelasFalhadas.push({ tabela: 'Debit Invoice (Fatura)', erro: error.message || 'Erro desconhecido' });
      logger.error({ error: error.message }, 'Erro ao inserir/atualizar fatura');
    }

    if (idFatura <= 0) {
      // Atualizar evento com falha
      if (eventId) {
        const temFalhas = tabelasFalhadas.length > 0;
        const mensagemErro = temFalhas
          ? `Tabelas inseridas: ${tabelasInseridas.join(', ')}. Tabelas com erro: ${tabelasFalhadas.map(t => `${t.tabela} (${t.erro})`).join(', ')}.`
          : 'Fatura não incluída, favor verificar lançamento!';

        const metadata: any = {
          idFatura: 0,
          document: contasPagar.data.document,
          id: contasPagar.data.id,
          etapa: isUpdate ? 'backend_atualizado_erro' : 'backend_inserido_erro',
          operacao: isUpdate ? 'UPDATE' : 'INSERT',
          tabelasInseridas,
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
          '/api/ContasPagar/InserirContasPagar',
          'failed',
          mensagemErro,
          metadata
        );
      }

      return {
        status: false,
        mensagem: 'Fatura não incluída, favor verificar lançamento!',
      };
    }

    // Processar Itens e Parcelas em Background (Assíncrono)
    // Não aguardamos essa promise para o retorno da função principal, mas iniciamos ela
    // O await aqui é apenas se quiséssemos que fosse síncrono. Como queremos assíncrono, não damos await.
    // Mas para manter a assinatura da função e a lógica anterior de "híbrido" (cabeçalho sync, itens async),
    // vamos deixar a responsabilidade do async para quem chama, ou fazemos fire-and-forget aqui.
    // Como a função `inserirContasPagar` retorna uma Promise, se dermos await aqui, ela vai esperar tudo.
    // Então, fazemos o fire-and-forget explicitamente:
    processarItensEParcelas(contasPagar, idFatura, eventId, isUpdate, tabelasInseridas, tabelasFalhadas, startProcessing).catch(err => {
       logger.error({ eventId, error: err.message }, 'Erro no processamento assíncrono de itens/parcelas');
    });

    // Retorno Síncrono Imediato (após inserir cabeçalho)
    return {
      status: true,
      mensagem: isUpdate ? 'Registro atualizado com sucesso!' : 'Registro criado com sucesso!',
      idFatura,
      created: !isUpdate,
    };

  } catch (error: any) {
    logger.error({ error: error.message }, 'Erro ao inserir conta a pagar');

    // Atualizar evento com erro
    if (eventId) {
      const metadata: any = {
        document: contasPagar.data?.document,
        id: contasPagar.data?.id,
        etapa: isUpdate ? 'backend_atualizado_erro' : 'backend_inserido_erro',
        operacao: isUpdate ? 'UPDATE' : 'INSERT',
        tabelasInseridas,
        resumo: {
          totalTabelas: tabelasInseridas.length + tabelasFalhadas.length,
          sucesso: tabelasInseridas.length,
          falhas: tabelasFalhadas.length,
        }
      };

      if (tabelasFalhadas.length > 0) {
        metadata.tabelasFalhadas = tabelasFalhadas;
      }

      await createOrUpdateWebhookEvent(
        eventId,
        '/api/ContasPagar/InserirContasPagar',
        'failed',
        error.message || 'Erro desconhecido',
        metadata
      );
    }

    return {
      status: false,
      mensagem: 'Registro não inserido, favor verificar log!',
    };
  }
}

/**
 * Processa Itens e Parcelas em Background
 */
async function processarItensEParcelas(
  contasPagar: ContasPagar,
  idFatura: number,
  eventId: string | null | undefined,
  isUpdate: boolean,
  tabelasInseridas: string[],
  tabelasFalhadas: Array<{ tabela: string; erro: string }>,
  startProcessing: number
) {
    // Inserir Centro de Custo (se informado)
    if (contasPagar.data.cost_centers) {
      try {
        const idCentroCusto = await inserirCentroCusto(contasPagar);
        if (idCentroCusto > 0) {
          await inserirCentroCustoAssociacao(idFatura, idCentroCusto);
          tabelasInseridas.push('Cost Center (Centro de Custo)');
        } else {
          tabelasFalhadas.push({ tabela: 'Cost Center (Centro de Custo)', erro: 'ID não retornado' });
        }
      } catch (error: any) {
        tabelasFalhadas.push({ tabela: 'Cost Center (Centro de Custo)', erro: error.message || 'Erro desconhecido' });
      }
    }

    // Inserir Parcelas (se informadas)
    if (contasPagar.data.installments && contasPagar.data.installments.length > 0) {
      for (const parcela of contasPagar.data.installments) {
        try {
          await inserirParcela(parcela, idFatura);
          if (!tabelasInseridas.includes('Debit Installments (Parcelas)')) {
            tabelasInseridas.push('Debit Installments (Parcelas)');
          }
        } catch (error: any) {
          if (!tabelasFalhadas.some(t => t.tabela === 'Debit Installments (Parcelas)')) {
            tabelasFalhadas.push({ tabela: 'Debit Installments (Parcelas)', erro: error.message || 'Erro desconhecido' });
          }
        }
      }
    }

    // Inserir Itens da Fatura (se informados)
    if (contasPagar.data.invoice_items && contasPagar.data.invoice_items.length > 0) {
      for (const item of contasPagar.data.invoice_items) {
        try {
          await inserirFaturaItem(item, idFatura);
          if (!tabelasInseridas.includes('Debit Invoice Items (Itens da Fatura)')) {
            tabelasInseridas.push('Debit Invoice Items (Itens da Fatura)');
          }
        } catch (error: any) {
          if (!tabelasFalhadas.some(t => t.tabela === 'Debit Invoice Items (Itens da Fatura)')) {
            tabelasFalhadas.push({ tabela: 'Debit Invoice Items (Itens da Fatura)', erro: error.message || 'Erro desconhecido' });
          }
        }
      }
    }

    // Atualizar evento WebhookEvent com informações detalhadas das tabelas
    if (eventId) {
      const temFalhas = tabelasFalhadas.length > 0;
      
      let mensagemErro: string | null = null;
      if (temFalhas) {
        const tabelasOk = tabelasInseridas.length > 0 
          ? `Tabelas inseridas/atualizadas com sucesso: ${tabelasInseridas.join(', ')}. `
          : '';
        const tabelasErro = `Tabelas com erro: ${tabelasFalhadas.map(t => `${t.tabela} (${t.erro})`).join(', ')}.`;
        mensagemErro = tabelasOk + tabelasErro;
      }
      
      const metadata: any = {
        idFatura: idFatura,
        document: contasPagar.data.document,
        id: contasPagar.data.id,
        installmentCount: contasPagar.data.installments?.length || 0,
        invoiceItemsCount: contasPagar.data.invoice_items?.length || 0,
        etapa: isUpdate ? 'backend_atualizado' : 'backend_inserido',
        operacao: isUpdate ? 'UPDATE' : 'INSERT',
        tabelasInseridas: tabelasInseridas,
        resumo: {
          totalTabelas: tabelasInseridas.length + tabelasFalhadas.length,
          sucesso: tabelasInseridas.length,
          falhas: tabelasFalhadas.length,
        }
      };
      
      if (temFalhas) {
        metadata.tabelasFalhadas = tabelasFalhadas;
      }
      
      await createOrUpdateWebhookEvent(
        eventId,
        '/api/ContasPagar/InserirContasPagar',
        'processed',
        mensagemErro,
        metadata
      );
    }
}

