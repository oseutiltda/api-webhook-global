import { PrismaClient } from '@prisma/client';
import type { EventType, ProcessResult } from '../types';
import { logger } from '../utils/logger';

// Cliente único que acessa staging e final (mesmo banco)
const prisma = new PrismaClient();

function getEventTypeFromSource(source: string): EventType | null {
  // Eventos de API que já foram processados pelo backend e não precisam de processamento adicional
  // Estes eventos são apenas para monitoramento e já estão marcados como 'processed' pelo backend
  if (source.includes('/api/CTe/InserirCte')) {
    // CT-e inserido via API já foi processado pelo backend (stored procedure)
    // Não precisa de processamento adicional no worker
    return null; // Retornar null fará com que seja ignorado
  }

  // Contas a Receber inserido via API já foi processado pelo backend
  // O worker processa via processPendingContasReceber que busca faturas pendentes do banco
  if (source.includes('/api/ContasReceber/InserirContasReceber')) {
    // Não precisa de processamento adicional no worker
    // O worker usa processPendingContasReceber que busca diretamente do banco
    return null; // Retornar null fará com que seja ignorado
  }

  // Mapear eventos de API de Pessoa para pessoa/upsert
  // Pessoa inserido via API já foi processado pelo backend (stored procedure)
  // Não precisa de processamento adicional no worker
  if (source.includes('/api/Pessoa/InserirPessoa') || source.includes('/api/Pessoa')) {
    return null; // Retornar null fará com que seja ignorado
  }

  if (source.includes('/cte/autorizado')) return 'cte/autorizado';
  if (source.includes('/cte/cancelado')) return 'cte/cancelado';
  if (source.includes('/ctrb/ciot/base')) return 'ctrb/ciot/base';
  if (source.includes('/ctrb/ciot/parcelas')) return 'ctrb/ciot/parcelas';
  if (source.includes('/faturas/pagar/criar')) return 'faturas/pagar/criar';
  if (source.includes('/faturas/pagar/baixar')) return 'faturas/pagar/baixar';
  if (source.includes('/faturas/pagar/cancelar')) return 'faturas/pagar/cancelar';
  if (source.includes('/faturas/receber/criar')) return 'faturas/receber/criar';
  if (source.includes('/faturas/receber/baixar')) return 'faturas/receber/baixar';
  if (source.includes('/nfse/autorizado')) return 'nfse/autorizado';
  if (source.includes('/pessoa/upsert')) return 'pessoa/upsert';
  return null;
}

async function processCteAutorizado(eventId: string): Promise<ProcessResult> {
  try {
    const staging = await prisma.cteAutorizado.findUnique({
      where: { id: eventId },
    });

    if (!staging) {
      return { success: false, error: `CT-e autorizado não encontrado para ID: ${eventId}` };
    }

    await prisma.finalCteAutorizado.upsert({
      where: { id: staging.id },
      update: {
        authorizationNumber: staging.authorizationNumber,
        status: staging.status,
        xml: staging.xml,
        eventXml: staging.eventXml,
        chCTe: staging.chCTe,
        nProt: staging.nProt,
        dhEmi: staging.dhEmi,
        dhRecbto: staging.dhRecbto,
        serie: staging.serie,
        nCT: staging.nCT,
        emitCnpj: staging.emitCnpj,
        destCnpj: staging.destCnpj,
        vTPrest: staging.vTPrest,
        vRec: staging.vRec,
        webhookEventId: eventId,
      },
      create: {
        id: staging.id,
        authorizationNumber: staging.authorizationNumber,
        status: staging.status,
        xml: staging.xml,
        eventXml: staging.eventXml,
        chCTe: staging.chCTe,
        nProt: staging.nProt,
        dhEmi: staging.dhEmi,
        dhRecbto: staging.dhRecbto,
        serie: staging.serie,
        nCT: staging.nCT,
        emitCnpj: staging.emitCnpj,
        destCnpj: staging.destCnpj,
        vTPrest: staging.vTPrest,
        vRec: staging.vRec,
        webhookEventId: eventId,
      },
    });

    return { success: true, recordsProcessed: 1 };
  } catch (error: any) {
    logger.error({ error, eventId }, 'Erro ao processar CT-e autorizado');
    return { success: false, error: error.message || 'Erro desconhecido' };
  }
}

async function processCiotParcelas(eventId: string): Promise<ProcessResult> {
  try {
    const staging = await prisma.ciotParcela.findUnique({
      where: { id: eventId },
      include: {
        parcelas: true,
        faturamento: true,
      },
    });

    if (!staging) {
      return { success: false, error: `CIOT parcelas não encontrado para ID: ${eventId}` };
    }

    await prisma.$transaction(async (tx) => {
      // Upsert manifesto principal
      await tx.finalCiotParcela.upsert({
        where: { id: staging.id },
        update: {
          nrciot: staging.nrciot,
          cdempresa: staging.cdempresa,
          cdcartafrete: staging.cdcartafrete,
          nrcgccpfprop: staging.nrcgccpfprop,
          tpformapagamentocontratado: staging.tpformapagamentocontratado,
          nrformapagamentocontratado: staging.nrformapagamentocontratado,
          nrcgccpfmot: staging.nrcgccpfmot,
          tpformapagamentomotorista: staging.tpformapagamentomotorista,
          nrformapagamentomotorista: staging.nrformapagamentomotorista,
          tpformapagamentosubcontratado: staging.tpformapagamentosubcontratado,
          nrformapagamentosubcontratado: staging.nrformapagamentosubcontratado,
          dtemissao: staging.dtemissao,
          vlcarga: staging.vlcarga,
          qtpesocarga: staging.qtpesocarga,
          nrmanifesto: staging.nrmanifesto,
          nrplaca: staging.nrplaca,
          nrceporigem: staging.nrceporigem,
          nrcepdestino: staging.nrcepdestino,
          fgemitida: staging.fgemitida,
          dtliberacaopagto: staging.dtliberacaopagto,
          cdcentrocusto: staging.cdcentrocusto,
          insituacao: staging.insituacao,
          cdcondicaovencto: staging.cdcondicaovencto,
          dsobservacao: staging.dsobservacao,
          cdtipotransporte: staging.cdtipotransporte,
          cdremetente: staging.cdremetente,
          cddestinatario: staging.cddestinatario,
          cdnaturezacarga: staging.cdnaturezacarga,
          cdespeciecarga: staging.cdespeciecarga,
          clmercadoria: staging.clmercadoria,
          qtpeso: staging.qtpeso,
          cdempresaconhec: staging.cdempresaconhec,
          nrseqcontrole: staging.nrseqcontrole,
          nrnotafiscal: staging.nrnotafiscal,
          cdhistorico: staging.cdhistorico,
          dsusuarioinc: staging.dsusuarioinc,
          dsusuariocanc: staging.dsusuariocanc,
          dtinclusao: staging.dtinclusao,
          dtcancelamento: staging.dtcancelamento,
          intipoorigem: staging.intipoorigem,
          nrplacareboque1: staging.nrplacareboque1,
          nrplacareboque2: staging.nrplacareboque2,
          nrplacareboque3: staging.nrplacareboque3,
          cdtarifa: staging.cdtarifa,
          dsusuarioacerto: staging.dsusuarioacerto,
          dtacerto: staging.dtacerto,
          cdinscricaocomp: staging.cdinscricaocomp,
          nrseriecomp: staging.nrseriecomp,
          nrcomprovante: staging.nrcomprovante,
          vlfrete: staging.vlfrete,
          insestsenat: staging.insestsenat,
          cdmotivocancelamento: staging.cdmotivocancelamento,
          dsobscancelamento: staging.dsobscancelamento,
          inveiculoproprio: staging.inveiculoproprio,
          dsusuarioimpressao: staging.dsusuarioimpressao,
          dtimpressao: staging.dtimpressao,
          dtprazomaxentrega: staging.dtprazomaxentrega,
          nrseloautenticidade: staging.nrseloautenticidade,
          hrmaxentrega: staging.hrmaxentrega,
          cdvinculacaoiss: staging.cdvinculacaoiss,
          dthrretornociot: staging.dthrretornociot,
          cdciot: staging.cdciot,
          serie: staging.serie,
          cdmsgretornociot: staging.cdmsgretornociot,
          dsmsgretornociot: staging.dsmsgretornociot,
          inenvioarquivociot: staging.inenvioarquivociot,
          dsavisotransportador: staging.dsavisotransportador,
          nrprotocolocancciot: staging.nrprotocolocancciot,
          cdndot: staging.cdndot,
          nrprotocoloautndot: staging.nrprotocoloautndot,
          inoperacaoperiodo: staging.inoperacaoperiodo,
          vlfreteestimado: staging.vlfreteestimado,
          inoperacaodistribuicao: staging.inoperacaodistribuicao,
          nrprotocoloenctociot: staging.nrprotocoloenctociot,
          indotimpresso: staging.indotimpresso,
          inveiculo: staging.inveiculo,
          cdrota: staging.cdrota,
          inoperadorapagtoctrb: staging.inoperadorapagtoctrb,
          inrespostaquesttacagreg: staging.inrespostaquesttacagreg,
          cdmoeda: staging.cdmoeda,
          nrprotocolointerroociot: staging.nrprotocolointerroociot,
          inretimposto: staging.inretimposto,
          cdintersenior: staging.cdintersenior,
          nrcodigooperpagtociot: staging.nrcodigooperpagtociot,
          cdseqhcm: staging.cdseqhcm,
          insitcalcpedagio: staging.insitcalcpedagio,
          nrrepom: staging.nrrepom,
          vlmanifesto: staging.vlmanifesto,
          vlcombustivel: staging.vlcombustivel,
          vlpedagio: staging.vlpedagio,
          vlnotacreditodebito: staging.vlnotacreditodebito,
          vldesconto: staging.vldesconto,
          vlcsll: staging.vlcsll,
          vlpis: staging.vlpis,
          vlirff: staging.vlirff,
          vlinss: staging.vlinss,
          vltotalmanifesto: staging.vltotalmanifesto,
          vlabastecimento: staging.vlabastecimento,
          vladiantamento: staging.vladiantamento,
          vlir: staging.vlir,
          vlsaldoapagar: staging.vlsaldoapagar,
          vlsaldofrete: staging.vlsaldofrete,
          vlcofins: staging.vlcofins,
          vlsestsenat: staging.vlsestsenat,
          vliss: staging.vliss,
          cdtributacao: staging.cdtributacao,
          vlcsl: staging.vlcsl,
          webhookEventId: eventId,
        },
        create: {
          id: staging.id,
          nrciot: staging.nrciot,
          cdempresa: staging.cdempresa,
          cdcartafrete: staging.cdcartafrete,
          nrcgccpfprop: staging.nrcgccpfprop,
          tpformapagamentocontratado: staging.tpformapagamentocontratado,
          nrformapagamentocontratado: staging.nrformapagamentocontratado,
          nrcgccpfmot: staging.nrcgccpfmot,
          tpformapagamentomotorista: staging.tpformapagamentomotorista,
          nrformapagamentomotorista: staging.nrformapagamentomotorista,
          tpformapagamentosubcontratado: staging.tpformapagamentosubcontratado,
          nrformapagamentosubcontratado: staging.nrformapagamentosubcontratado,
          dtemissao: staging.dtemissao,
          vlcarga: staging.vlcarga,
          qtpesocarga: staging.qtpesocarga,
          nrmanifesto: staging.nrmanifesto,
          nrplaca: staging.nrplaca,
          nrceporigem: staging.nrceporigem,
          nrcepdestino: staging.nrcepdestino,
          fgemitida: staging.fgemitida,
          dtliberacaopagto: staging.dtliberacaopagto,
          cdcentrocusto: staging.cdcentrocusto,
          insituacao: staging.insituacao,
          cdcondicaovencto: staging.cdcondicaovencto,
          dsobservacao: staging.dsobservacao,
          cdtipotransporte: staging.cdtipotransporte,
          cdremetente: staging.cdremetente,
          cddestinatario: staging.cddestinatario,
          cdnaturezacarga: staging.cdnaturezacarga,
          cdespeciecarga: staging.cdespeciecarga,
          clmercadoria: staging.clmercadoria,
          qtpeso: staging.qtpeso,
          cdempresaconhec: staging.cdempresaconhec,
          nrseqcontrole: staging.nrseqcontrole,
          nrnotafiscal: staging.nrnotafiscal,
          cdhistorico: staging.cdhistorico,
          dsusuarioinc: staging.dsusuarioinc,
          dsusuariocanc: staging.dsusuariocanc,
          dtinclusao: staging.dtinclusao,
          dtcancelamento: staging.dtcancelamento,
          intipoorigem: staging.intipoorigem,
          nrplacareboque1: staging.nrplacareboque1,
          nrplacareboque2: staging.nrplacareboque2,
          nrplacareboque3: staging.nrplacareboque3,
          cdtarifa: staging.cdtarifa,
          dsusuarioacerto: staging.dsusuarioacerto,
          dtacerto: staging.dtacerto,
          cdinscricaocomp: staging.cdinscricaocomp,
          nrseriecomp: staging.nrseriecomp,
          nrcomprovante: staging.nrcomprovante,
          vlfrete: staging.vlfrete,
          insestsenat: staging.insestsenat,
          cdmotivocancelamento: staging.cdmotivocancelamento,
          dsobscancelamento: staging.dsobscancelamento,
          inveiculoproprio: staging.inveiculoproprio,
          dsusuarioimpressao: staging.dsusuarioimpressao,
          dtimpressao: staging.dtimpressao,
          dtprazomaxentrega: staging.dtprazomaxentrega,
          nrseloautenticidade: staging.nrseloautenticidade,
          hrmaxentrega: staging.hrmaxentrega,
          cdvinculacaoiss: staging.cdvinculacaoiss,
          dthrretornociot: staging.dthrretornociot,
          cdciot: staging.cdciot,
          serie: staging.serie,
          cdmsgretornociot: staging.cdmsgretornociot,
          dsmsgretornociot: staging.dsmsgretornociot,
          inenvioarquivociot: staging.inenvioarquivociot,
          dsavisotransportador: staging.dsavisotransportador,
          nrprotocolocancciot: staging.nrprotocolocancciot,
          cdndot: staging.cdndot,
          nrprotocoloautndot: staging.nrprotocoloautndot,
          inoperacaoperiodo: staging.inoperacaoperiodo,
          vlfreteestimado: staging.vlfreteestimado,
          inoperacaodistribuicao: staging.inoperacaodistribuicao,
          nrprotocoloenctociot: staging.nrprotocoloenctociot,
          indotimpresso: staging.indotimpresso,
          inveiculo: staging.inveiculo,
          cdrota: staging.cdrota,
          inoperadorapagtoctrb: staging.inoperadorapagtoctrb,
          inrespostaquesttacagreg: staging.inrespostaquesttacagreg,
          cdmoeda: staging.cdmoeda,
          nrprotocolointerroociot: staging.nrprotocolointerroociot,
          inretimposto: staging.inretimposto,
          cdintersenior: staging.cdintersenior,
          nrcodigooperpagtociot: staging.nrcodigooperpagtociot,
          cdseqhcm: staging.cdseqhcm,
          insitcalcpedagio: staging.insitcalcpedagio,
          nrrepom: staging.nrrepom,
          vlmanifesto: staging.vlmanifesto,
          vlcombustivel: staging.vlcombustivel,
          vlpedagio: staging.vlpedagio,
          vlnotacreditodebito: staging.vlnotacreditodebito,
          vldesconto: staging.vldesconto,
          vlcsll: staging.vlcsll,
          vlpis: staging.vlpis,
          vlirff: staging.vlirff,
          vlinss: staging.vlinss,
          vltotalmanifesto: staging.vltotalmanifesto,
          vlabastecimento: staging.vlabastecimento,
          vladiantamento: staging.vladiantamento,
          vlir: staging.vlir,
          vlsaldoapagar: staging.vlsaldoapagar,
          vlsaldofrete: staging.vlsaldofrete,
          vlcofins: staging.vlcofins,
          vlsestsenat: staging.vlsestsenat,
          vliss: staging.vliss,
          cdtributacao: staging.cdtributacao,
          vlcsl: staging.vlcsl,
          webhookEventId: eventId,
        },
      });

      // Recriar parcelas
      await tx.finalCiotParcelaItem.deleteMany({ where: { manifestoId: staging.id } });
      if (staging.parcelas.length > 0) {
        await tx.finalCiotParcelaItem.createMany({
          data: staging.parcelas.map((p) => ({
            manifestoId: staging.id,
            idparcela: p.idparcela,
            nrciotsistema: p.nrciotsistema,
            nrciot: p.nrciot,
            dstipo: p.dstipo,
            dsstatus: p.dsstatus,
            cdfavorecido: p.cdfavorecido,
            cdcartafrete: p.cdcartafrete,
            cdevento: p.cdevento,
            dtpagto: p.dtpagto,
            indesconto: p.indesconto,
            vlbasecalculo: p.vlbasecalculo,
            dtrecebimento: p.dtrecebimento,
            vloriginal: p.vloriginal,
            dtinclusao: p.dtinclusao,
            hrinclusao: p.hrinclusao,
            dsusuarioinc: p.dsusuarioinc,
            dtreferenciacalculo: p.dtreferenciacalculo,
            dsobservacao: p.dsobservacao,
            vlprovisionado: p.vlprovisionado,
          })),
        });
      }

      // Upsert faturamento
      if (staging.faturamento) {
        await tx.finalCiotFaturamento.upsert({
          where: { manifestoId: staging.id },
          update: {
            cdempresa: staging.faturamento.cdempresa,
            cdcartafrete: staging.faturamento.cdcartafrete,
            cdempresaFV: staging.faturamento.cdempresaFV,
            nrficha: staging.faturamento.nrficha,
          },
          create: {
            manifestoId: staging.id,
            cdempresa: staging.faturamento.cdempresa,
            cdcartafrete: staging.faturamento.cdcartafrete,
            cdempresaFV: staging.faturamento.cdempresaFV,
            nrficha: staging.faturamento.nrficha,
          },
        });
      }
    });

    return {
      success: true,
      recordsProcessed: 1 + staging.parcelas.length + (staging.faturamento ? 1 : 0),
    };
  } catch (error: any) {
    logger.error({ error, eventId }, 'Erro ao processar CIOT parcelas');
    return { success: false, error: error.message || 'Erro desconhecido' };
  }
}

async function processFaturaPagarCriar(eventId: string): Promise<ProcessResult> {
  try {
    const staging = await prisma.faturaPagar.findUnique({
      where: { id: eventId },
      include: { parcelas: true },
    });

    if (!staging) {
      return { success: false, error: `Fatura pagar não encontrada para ID: ${eventId}` };
    }

    await prisma.$transaction(async (tx) => {
      await tx.finalFaturaPagar.upsert({
        where: { id: staging.id },
        update: {
          fornecedorCnpj: staging.fornecedorCnpj,
          numero: staging.numero,
          emissao: staging.emissao,
          valor: staging.valor,
          webhookEventId: eventId,
        },
        create: {
          id: staging.id,
          fornecedorCnpj: staging.fornecedorCnpj,
          numero: staging.numero,
          emissao: staging.emissao,
          valor: staging.valor,
          webhookEventId: eventId,
        },
      });

      await tx.finalFaturaPagarParcela.deleteMany({ where: { faturaId: staging.id } });
      if (staging.parcelas.length > 0) {
        await tx.finalFaturaPagarParcela.createMany({
          data: staging.parcelas.map((p) => ({
            faturaId: staging.id,
            posicao: p.posicao,
            dueDate: p.dueDate,
            valor: p.valor,
            interestValue: p.interestValue,
            discountValue: p.discountValue,
            paymentMethod: p.paymentMethod,
            comments: p.comments,
            installmentId: p.installmentId,
          })),
        });
      }
    });

    return { success: true, recordsProcessed: 1 + staging.parcelas.length };
  } catch (error: any) {
    logger.error({ error, eventId }, 'Erro ao processar fatura pagar criar');
    return { success: false, error: error.message || 'Erro desconhecido' };
  }
}

async function processFaturaReceberCriar(eventId: string): Promise<ProcessResult> {
  try {
    const staging = await prisma.faturaReceber.findUnique({
      where: { id: eventId },
      include: { parcelas: true, itens: true },
    });

    if (!staging) {
      return { success: false, error: `Fatura receber não encontrada para ID: ${eventId}` };
    }

    await prisma.$transaction(async (tx) => {
      await tx.finalFaturaReceber.upsert({
        where: { id: staging.id },
        update: {
          clienteCnpj: staging.clienteCnpj,
          numero: staging.numero,
          emissao: staging.emissao,
          valor: staging.valor,
          webhookEventId: eventId,
        },
        create: {
          id: staging.id,
          clienteCnpj: staging.clienteCnpj,
          numero: staging.numero,
          emissao: staging.emissao,
          valor: staging.valor,
          webhookEventId: eventId,
        },
      });

      await tx.finalFaturaReceberParcela.deleteMany({ where: { faturaId: staging.id } });
      if (staging.parcelas.length > 0) {
        await tx.finalFaturaReceberParcela.createMany({
          data: staging.parcelas.map((p) => ({
            faturaId: staging.id,
            posicao: p.posicao,
            dueDate: p.dueDate,
            valor: p.valor,
            interestValue: p.interestValue,
            discountValue: p.discountValue,
            paymentMethod: p.paymentMethod,
            comments: p.comments,
            installmentId: p.installmentId,
          })),
        });
      }

      await tx.finalFaturaReceberItem.deleteMany({ where: { faturaId: staging.id } });
      if (staging.itens.length > 0) {
        await tx.finalFaturaReceberItem.createMany({
          data: staging.itens.map((it) => ({
            faturaId: staging.id,
            cteKey: it.cteKey,
            cteNumber: it.cteNumber,
            cteSeries: it.cteSeries,
            payerName: it.payerName,
            draftNumber: it.draftNumber,
            nfseNumber: it.nfseNumber,
            nfseSeries: it.nfseSeries,
            total: it.total,
            type: it.type,
          })),
        });
      }
    });

    return { success: true, recordsProcessed: 1 + staging.parcelas.length + staging.itens.length };
  } catch (error: any) {
    logger.error({ error, eventId }, 'Erro ao processar fatura receber criar');
    return { success: false, error: error.message || 'Erro desconhecido' };
  }
}

async function processNfseAutorizado(eventId: string): Promise<ProcessResult> {
  try {
    // NFSe usa chave composta, precisa buscar por prestadorCnpj-numero
    // Assumindo que eventId contém essa informação ou precisamos buscar de outra forma
    const staging = await prisma.nfseAutorizado.findFirst({
      where: { id: { contains: eventId } },
    });

    if (!staging) {
      return { success: false, error: `NFSe autorizado não encontrado para ID: ${eventId}` };
    }

    await prisma.finalNfseAutorizado.upsert({
      where: { id: staging.id },
      update: {
        numero: staging.numero,
        codigoVerificacao: staging.codigoVerificacao,
        dataEmissao: staging.dataEmissao,
        rpsNumero: staging.rpsNumero,
        rpsSerie: staging.rpsSerie,
        rpsTipo: staging.rpsTipo,
        dataEmissaoRps: staging.dataEmissaoRps,
        naturezaOperacao: staging.naturezaOperacao,
        optanteSimples: staging.optanteSimples,
        incentivadorCultural: staging.incentivadorCultural,
        competencia: staging.competencia,
        itemListaServico: staging.itemListaServico,
        codTribMunicipio: staging.codTribMunicipio,
        discriminacao: staging.discriminacao,
        codMunicipioServ: staging.codMunicipioServ,
        valorServicos: staging.valorServicos,
        valorDeducoes: staging.valorDeducoes,
        issRetido: staging.issRetido,
        valorIss: staging.valorIss,
        valorIssRetido: staging.valorIssRetido,
        baseCalculo: staging.baseCalculo,
        aliquota: staging.aliquota,
        valorLiquidoNfse: staging.valorLiquidoNfse,
        valorCredito: staging.valorCredito,
        prestadorCnpj: staging.prestadorCnpj,
        prestadorIM: staging.prestadorIM,
        prestadorRazao: staging.prestadorRazao,
        prestadorFantasia: staging.prestadorFantasia,
        prestadorLogradouro: staging.prestadorLogradouro,
        prestadorNumero: staging.prestadorNumero,
        prestadorComplemento: staging.prestadorComplemento,
        prestadorBairro: staging.prestadorBairro,
        prestadorCodMunicipio: staging.prestadorCodMunicipio,
        prestadorUf: staging.prestadorUf,
        prestadorCep: staging.prestadorCep,
        prestadorTelefone: staging.prestadorTelefone,
        prestadorEmail: staging.prestadorEmail,
        tomadorCnpj: staging.tomadorCnpj,
        tomadorIM: staging.tomadorIM,
        tomadorRazao: staging.tomadorRazao,
        tomadorLogradouro: staging.tomadorLogradouro,
        tomadorNumero: staging.tomadorNumero,
        tomadorComplemento: staging.tomadorComplemento,
        tomadorBairro: staging.tomadorBairro,
        tomadorCodMunicipio: staging.tomadorCodMunicipio,
        tomadorUf: staging.tomadorUf,
        tomadorCep: staging.tomadorCep,
        tomadorTelefone: staging.tomadorTelefone,
        tomadorEmail: staging.tomadorEmail,
        orgaoCodMunicipio: staging.orgaoCodMunicipio,
        orgaoUf: staging.orgaoUf,
        // Campos de logística
        pesoReal: staging.pesoReal,
        pesoCubado: staging.pesoCubado,
        quantidadeVolumes: staging.quantidadeVolumes,
        valorProduto: staging.valorProduto,
        valorNota: staging.valorNota,
        valorFretePeso: staging.valorFretePeso,
        valorAdv: staging.valorAdv,
        valorOutros: staging.valorOutros,
        comentarioFrete: staging.comentarioFrete,
        usuarioAlteracao: staging.usuarioAlteracao,
        dataAlteracao: staging.dataAlteracao,
        dataCancelamento: staging.dataCancelamento,
        motivoCancelamento: staging.motivoCancelamento,
        filialCancelamento: staging.filialCancelamento,
        cnpjConsignatario: staging.cnpjConsignatario,
        cnpjRedespacho: staging.cnpjRedespacho,
        cnpjExpedidor: staging.cnpjExpedidor,
        valorBaseCalculoPis: staging.valorBaseCalculoPis,
        aliqPis: staging.aliqPis,
        valorPis: staging.valorPis,
        aliqCofins: staging.aliqCofins,
        valorCofins: staging.valorCofins,
        dataPrazo: staging.dataPrazo,
        diasEntrega: staging.diasEntrega,
        cnpjRemetente: staging.cnpjRemetente,
        cepRemetente: staging.cepRemetente,
        cnpjDestinatario: staging.cnpjDestinatario,
        cepDestinatario: staging.cepDestinatario,
        logradouroDestinatario: staging.logradouroDestinatario,
        webhookEventId: eventId,
      },
      create: {
        id: staging.id,
        numero: staging.numero,
        codigoVerificacao: staging.codigoVerificacao,
        dataEmissao: staging.dataEmissao,
        rpsNumero: staging.rpsNumero,
        rpsSerie: staging.rpsSerie,
        rpsTipo: staging.rpsTipo,
        dataEmissaoRps: staging.dataEmissaoRps,
        naturezaOperacao: staging.naturezaOperacao,
        optanteSimples: staging.optanteSimples,
        incentivadorCultural: staging.incentivadorCultural,
        competencia: staging.competencia,
        itemListaServico: staging.itemListaServico,
        codTribMunicipio: staging.codTribMunicipio,
        discriminacao: staging.discriminacao,
        codMunicipioServ: staging.codMunicipioServ,
        valorServicos: staging.valorServicos,
        valorDeducoes: staging.valorDeducoes,
        issRetido: staging.issRetido,
        valorIss: staging.valorIss,
        valorIssRetido: staging.valorIssRetido,
        baseCalculo: staging.baseCalculo,
        aliquota: staging.aliquota,
        valorLiquidoNfse: staging.valorLiquidoNfse,
        valorCredito: staging.valorCredito,
        prestadorCnpj: staging.prestadorCnpj,
        prestadorIM: staging.prestadorIM,
        prestadorRazao: staging.prestadorRazao,
        prestadorFantasia: staging.prestadorFantasia,
        prestadorLogradouro: staging.prestadorLogradouro,
        prestadorNumero: staging.prestadorNumero,
        prestadorComplemento: staging.prestadorComplemento,
        prestadorBairro: staging.prestadorBairro,
        prestadorCodMunicipio: staging.prestadorCodMunicipio,
        prestadorUf: staging.prestadorUf,
        prestadorCep: staging.prestadorCep,
        prestadorTelefone: staging.prestadorTelefone,
        prestadorEmail: staging.prestadorEmail,
        tomadorCnpj: staging.tomadorCnpj,
        tomadorIM: staging.tomadorIM,
        tomadorRazao: staging.tomadorRazao,
        tomadorLogradouro: staging.tomadorLogradouro,
        tomadorNumero: staging.tomadorNumero,
        tomadorComplemento: staging.tomadorComplemento,
        tomadorBairro: staging.tomadorBairro,
        tomadorCodMunicipio: staging.tomadorCodMunicipio,
        tomadorUf: staging.tomadorUf,
        tomadorCep: staging.tomadorCep,
        tomadorTelefone: staging.tomadorTelefone,
        tomadorEmail: staging.tomadorEmail,
        orgaoCodMunicipio: staging.orgaoCodMunicipio,
        orgaoUf: staging.orgaoUf,
        // Campos de logística
        pesoReal: staging.pesoReal,
        pesoCubado: staging.pesoCubado,
        quantidadeVolumes: staging.quantidadeVolumes,
        valorProduto: staging.valorProduto,
        valorNota: staging.valorNota,
        valorFretePeso: staging.valorFretePeso,
        valorAdv: staging.valorAdv,
        valorOutros: staging.valorOutros,
        comentarioFrete: staging.comentarioFrete,
        usuarioAlteracao: staging.usuarioAlteracao,
        dataAlteracao: staging.dataAlteracao,
        dataCancelamento: staging.dataCancelamento,
        motivoCancelamento: staging.motivoCancelamento,
        filialCancelamento: staging.filialCancelamento,
        cnpjConsignatario: staging.cnpjConsignatario,
        cnpjRedespacho: staging.cnpjRedespacho,
        cnpjExpedidor: staging.cnpjExpedidor,
        valorBaseCalculoPis: staging.valorBaseCalculoPis,
        aliqPis: staging.aliqPis,
        valorPis: staging.valorPis,
        aliqCofins: staging.aliqCofins,
        valorCofins: staging.valorCofins,
        dataPrazo: staging.dataPrazo,
        diasEntrega: staging.diasEntrega,
        cnpjRemetente: staging.cnpjRemetente,
        cepRemetente: staging.cepRemetente,
        cnpjDestinatario: staging.cnpjDestinatario,
        cepDestinatario: staging.cepDestinatario,
        logradouroDestinatario: staging.logradouroDestinatario,
        webhookEventId: eventId,
      },
    });

    return { success: true, recordsProcessed: 1 };
  } catch (error: any) {
    logger.error({ error, eventId }, 'Erro ao processar NFSe autorizado');
    return { success: false, error: error.message || 'Erro desconhecido' };
  }
}

async function processPessoaUpsert(eventId: string): Promise<ProcessResult> {
  // NOTA: Esta função não é mais usada porque eventos de /api/Pessoa são ignorados
  // Mantida apenas para compatibilidade caso seja chamada de outro lugar
  // A tabela Pessoa no banco AFS_INTEGRADOR não tem coluna 'id', usa 'CodPessoa' como chave primária
  // Por isso, não podemos usar prisma.pessoa.findUnique() que espera a coluna 'id'

  logger.warn(
    { eventId },
    'processPessoaUpsert chamada mas não é mais suportada. Eventos de /api/Pessoa devem ser processados apenas pelo backend.',
  );

  return {
    success: false,
    error: 'Processamento de Pessoa via worker não é suportado. Use apenas a API do backend.',
  };
}

export async function processEvent(eventId: string, source: string): Promise<ProcessResult> {
  const eventType = getEventTypeFromSource(source);

  if (!eventType) {
    logger.warn({ eventId, source }, 'Tipo de evento não reconhecido pelo worker');
    return {
      success: false,
      error: `Evento "${source}" não requer processamento pelo worker (pode ter sido processado diretamente pelo backend). Eventos suportados pelo worker: CT-e/cancelamento, NFSe, Faturas, CIOT.`,
    };
  }

  logger.info({ eventId, eventType }, 'Processando evento');

  switch (eventType) {
    case 'cte/autorizado':
      return processCteAutorizado(eventId);
    case 'ctrb/ciot/parcelas':
      return processCiotParcelas(eventId);
    case 'faturas/pagar/criar':
      return processFaturaPagarCriar(eventId);
    case 'faturas/receber/criar':
      return processFaturaReceberCriar(eventId);
    case 'nfse/autorizado':
      return processNfseAutorizado(eventId);
    case 'pessoa/upsert':
      return processPessoaUpsert(eventId);
    default:
      logger.warn({ eventId, eventType }, 'Tipo de evento ainda não implementado');
      return { success: false, error: `Processador não implementado para: ${eventType}` };
  }
}
