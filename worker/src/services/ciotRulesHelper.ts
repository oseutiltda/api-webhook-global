/**
 * Helper functions para aplicar as regras de processamento de Carta Frete (SOFTRAN)
 * conforme documento técnico REGRAS_DE_PROCESSAMENTO_DE_CARTA_FRETE_SOFTRAN
 */

import type { PrismaClient, Prisma } from '@prisma/client';
import { logger } from '../utils/logger';
import { env } from '../config/env';

type PrismaExecutor = PrismaClient | Prisma.TransactionClient;

/**
 * Busca os valores dos eventos da tabela FTRCFTMV para calcular acréscimos e descontos
 */
export async function buscarValoresEventos(
  prisma: PrismaExecutor,
  cdEmpresa: number,
  cdCartaFrete: number,
): Promise<{
  vlEvento3: number; // INSS
  vlEvento4: number; // IRRF
  vlEvento5: number; // SEST SENAT
  vlEvento8: number; // Acréscimo saldo
  vlEvento10: number; // Combustível
  vlEvento12: number; // Desconto - outros eventos
}> {
  try {
    const sql = `
      SELECT 
        CdEvento,
        VlEvento
      FROM [${env.SENIOR_DATABASE}].dbo.FTRCFTMV
      WHERE CdEmpresa = ${cdEmpresa}
        AND CdCartaFrete = ${cdCartaFrete}
        AND CdEvento IN (3, 4, 5, 8, 10, 12)
        AND VlEvento <> 0
    `;

    const eventos = await prisma.$queryRawUnsafe<
      Array<{
        CdEvento: number;
        VlEvento: number;
      }>
    >(sql);

    const resultado = {
      vlEvento3: 0,
      vlEvento4: 0,
      vlEvento5: 0,
      vlEvento8: 0,
      vlEvento10: 0,
      vlEvento12: 0,
    };

    eventos.forEach((evento) => {
      switch (evento.CdEvento) {
        case 3:
          resultado.vlEvento3 = Number(evento.VlEvento) || 0;
          break;
        case 4:
          resultado.vlEvento4 = Number(evento.VlEvento) || 0;
          break;
        case 5:
          resultado.vlEvento5 = Number(evento.VlEvento) || 0;
          break;
        case 8:
          resultado.vlEvento8 = Number(evento.VlEvento) || 0;
          break;
        case 10:
          resultado.vlEvento10 = Number(evento.VlEvento) || 0;
          break;
        case 12:
          resultado.vlEvento12 = Number(evento.VlEvento) || 0;
          break;
      }
    });

    return resultado;
  } catch (error: any) {
    logger.warn(
      { error: error?.message, cdEmpresa, cdCartaFrete },
      'Erro ao buscar valores dos eventos FTRCFTMV, usando valores zero',
    );
    return {
      vlEvento3: 0,
      vlEvento4: 0,
      vlEvento5: 0,
      vlEvento8: 0,
      vlEvento10: 0,
      vlEvento12: 0,
    };
  }
}

/**
 * Calcula os valores de acréscimo e desconto baseado nos eventos conforme regras
 * CENÁRIO 1: Para o título do saldo (InTpTitulo = 2)
 *   - VlAcrescimo = evento 8
 *   - VlDesconto = soma dos eventos 3,4,5
 */
export function calcularAcrescimoDesconto(eventos: {
  vlEvento3: number;
  vlEvento4: number;
  vlEvento5: number;
  vlEvento8: number;
}): { vlAcrescimo: number; vlDesconto: number } {
  return {
    vlAcrescimo: eventos.vlEvento8,
    vlDesconto: eventos.vlEvento3 + eventos.vlEvento4 + eventos.vlEvento5,
  };
}

/**
 * Retorna o CdPlanoConta conforme tipo de título conforme regras gerais fixas
 */
export function getCdPlanoContaPorTipoTitulo(inTpTitulo: number): number {
  switch (inTpTitulo) {
    case 1: // Adiantamento
      return 11402001;
    case 2: // Saldo
      return 21101002;
    case 3: // Pedágio
      return 21101002;
    default:
      return 21101002;
  }
}

/**
 * Identifica qual cenário aplicar baseado nos eventos presentes
 * Retorna o número do cenário ou null se não identificado
 */
export function identificarCenario(eventos: {
  vlEvento10: number; // Combustível
  vlEvento12: number; // Desconto outros
  temAdiantamento: boolean;
  temPedagio: boolean;
  temSaldo: boolean;
  temNotaCredito: boolean; // Nota de Crédito (carga/descarga)
}): number | null {
  // CENÁRIO 1: Manifesto 1575 – Pessoa Física / Pessoa Física
  // Eventos: Adiantamento, Pedágio, Saldo (com impostos), Nota de Crédito (carga/descarga)
  if (eventos.temAdiantamento && eventos.temPedagio && eventos.temSaldo && eventos.temNotaCredito) {
    return 1;
  }

  // CENÁRIO 2: Manifesto 1568 – Pessoa Jurídica / Pessoa Física
  // Eventos: Saldo, Pedágio, Abastecimento (evento 10 obrigatório)
  if (eventos.vlEvento10 > 0 && eventos.temSaldo && eventos.temPedagio) {
    return 2;
  }

  // CENÁRIO 3: Manifesto 1527 – Pessoa Jurídica / Pessoa Física
  // Eventos: Adiantamento, Pedágio, Saldo, Nota de Débito (evento 12 obrigatório)
  if (eventos.vlEvento12 > 0 && eventos.temAdiantamento && eventos.temSaldo && eventos.temPedagio) {
    return 3;
  }

  // CENÁRIO 4: Manifesto 1612 – Pessoa Jurídica / Pessoa Física
  // Eventos: Adiantamento, Pedágio, Saldo (sem eventos especiais)
  if (
    eventos.temAdiantamento &&
    eventos.temPedagio &&
    eventos.temSaldo &&
    eventos.vlEvento10 === 0 &&
    eventos.vlEvento12 === 0
  ) {
    return 4;
  }

  // CENÁRIO 5: Manifesto 1611 – Pessoa Jurídica / Pessoa Física
  // Eventos: Pedágio, Saldo (sem eventos especiais)
  if (
    eventos.temPedagio &&
    eventos.temSaldo &&
    !eventos.temAdiantamento &&
    eventos.vlEvento10 === 0 &&
    eventos.vlEvento12 === 0
  ) {
    return 5;
  }

  // CENÁRIO 6: Manifesto 1605 – Pessoa Física / Pessoa Física
  // Eventos: Saldo (com impostos) - apenas saldo, sem adiantamento, sem pedágio
  if (eventos.temSaldo && !eventos.temAdiantamento && !eventos.temPedagio) {
    return 6;
  }

  return null;
}
