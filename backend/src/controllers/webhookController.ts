import type { Request, Response } from 'express';
import { PrismaClient } from '@prisma/client';
import {
  cteAutorizadoSchema,
  cteCanceladoSchema,
  ciotBaseSchema,
  ciotParcelasSchema,
  faturaPagarBaixarSchema,
  faturaPagarCancelarSchema,
  faturaPagarCriarSchema,
  faturaReceberBaixarSchema,
  faturaReceberCriarSchema,
  nfseAutorizadoSchema,
  pessoaSchema,
} from '../schemas';
import { parseCteAutorizado, parseCteCancelado } from '../utils/xml';
import { logger } from '../utils/logger';

const prisma = new PrismaClient();

// Helper para converter undefined em null (Prisma requer null, não undefined)
const toNull = <T>(value: T | undefined): T | null => value === undefined ? null : value;
// Helper para campos obrigatórios string - retorna string vazia se undefined
const toString = (value: string | undefined): string => value || '';
// Helper para converter string | number para number | null (para campos de número do endereço)
const toNumberOrNull = (value: string | number | null | undefined): number | null => {
  if (value === null || value === undefined) return null;
  if (typeof value === 'number') return value;
  if (typeof value === 'string') {
    const parsed = parseInt(value, 10);
    return isNaN(parsed) ? null : parsed;
  }
  return null;
};

export async function cteAutorizado(req: Request, res: Response) {
  const data = cteAutorizadoSchema.parse(req.body);
  const id = String(data.id);
  await prisma.cteAutorizado.upsert({
    where: { id },
    update: {
      authorizationNumber: data.authorization_number,
      status: data.status,
      xml: data.xml.substring(0, 4000),
      eventXml: data.event_xml ? data.event_xml.substring(0, 4000) : null,
      ...parseCteAutorizado(data.xml),
    },
    create: {
      id,
      authorizationNumber: data.authorization_number,
      status: data.status,
      xml: data.xml.substring(0, 4000),
      eventXml: data.event_xml ? data.event_xml.substring(0, 4000) : null,
      ...parseCteAutorizado(data.xml),
    },
  });
  return res.status(202).json({ status: 'accepted' });
}

export async function cteCancelado(req: Request, res: Response) {
  const data = cteCanceladoSchema.parse(req.body);
  const id = String(data.id);
  await prisma.cteCancelado.upsert({
    where: { id },
    update: {
      authorizationNumber: data.authorization_number,
      status: data.status,
      xml: data.xml.substring(0, 4000),
      eventXml: data.event_xml ? data.event_xml.substring(0, 4000) : null,
      ...parseCteCancelado(data.xml),
    },
    create: {
      id,
      authorizationNumber: data.authorization_number,
      status: data.status,
      xml: data.xml.substring(0, 4000),
      eventXml: data.event_xml ? data.event_xml.substring(0, 4000) : null,
      ...parseCteCancelado(data.xml),
    },
  });
  return res.status(202).json({ status: 'accepted' });
}

export async function ciotBase(req: Request, res: Response) {
  const data = ciotBaseSchema.parse(req.body);
  await prisma.ciotBase.upsert({ where: { id: data.id }, update: data, create: data });
  return res.status(202).json({ status: 'accepted' });
}

export async function ciotParcelas(req: Request, res: Response) {
  const d = ciotParcelasSchema.parse(req.body);

  // Garantir null em campos opcionais (Prisma não aceita undefined com exactOptionalPropertyTypes)
  const nullableKeys = [
    'vlcarga','qtpesocarga','fgemitida','dtliberacaopagto','cdcentrocusto','dsobservacao','cdtipotransporte',
    'clmercadoria','qtpeso','cdempresaconhec','nrseqcontrole','cdhistorico','dsusuariocanc','dtcancelamento',
    'nrplacareboque2','nrplacareboque3','dsusuarioacerto','dtacerto','cdinscricaocomp','nrseriecomp',
    'nrcomprovante','insestsenat','cdmotivocancelamento','dsobscancelamento','dsusuarioimpressao','dtimpressao',
    'nrseloautenticidade','hrmaxentrega','cdvinculacaoiss','dthrretornociot','cdciot','cdmsgretornociot',
    'dsmsgretornociot','inenvioarquivociot','dsavisotransportador','nrprotocolocancciot','cdndot','nrprotocoloautndot',
    'inoperacaoperiodo','vlfreteestimado','nrprotocoloenctociot','indotimpresso','cdmoeda','nrprotocolointerroociot',
    'inretimposto','cdintersenior','nrcodigooperpagtociot','cdseqhcm','insitcalcpedagio','nrrepom','cdtributacao'
  ] as const;

  const manifestoData: any = { ...d };
  for (const k of nullableKeys) {
    if (manifestoData[k] === undefined) manifestoData[k] = null;
  }

  await prisma.$transaction(async (tx) => {
    await tx.ciotParcela.upsert({
      where: { id: d.id },
      update: manifestoData,
      create: manifestoData,
    });

    // recriar parcelas vinculadas
    await tx.ciotParcelaItem.deleteMany({ where: { manifestoId: d.id } });
    await tx.ciotParcelaItem.createMany({
      data: (d.parcelas || []).map((p) => ({
        manifestoId: d.id,
        idparcela: p.idparcela,
        nrciotsistema: p.nrciotsistema,
        nrciot: p.nrciot,
        dstipo: p.dstipo,
        dsstatus: p.dsstatus,
        cdfavorecido: p.cdfavorecido,
        cdcartafrete: p.cdcartafrete,
        cdevento: p.cdevento,
        dtpagto: p.dtpagto,
        indesconto: p.indesconto ?? null,
        vlbasecalculo: p.vlbasecalculo ?? null,
        dtrecebimento: p.dtrecebimento ?? null,
        vloriginal: p.vloriginal ?? null,
        dtinclusao: p.dtinclusao,
        hrinclusao: p.hrinclusao,
        dsusuarioinc: p.dsusuarioinc,
        dtreferenciacalculo: p.dtreferenciacalculo,
        dsobservacao: p.dsobservacao ?? null,
        vlprovisionado: p.vlprovisionado ?? null,
      })),
    });

    // upsert faturamento (1:1)
    await tx.ciotFaturamento.upsert({
      where: { manifestoId: d.id },
      update: {
        cdempresa: d.dadosFaturamento.cdempresa,
        cdcartafrete: d.dadosFaturamento.cdcartafrete,
        cdempresaFV: d.dadosFaturamento.cdempresaFV,
        nrficha: d.dadosFaturamento.nrficha ?? null,
      },
      create: {
        manifestoId: d.id,
        cdempresa: d.dadosFaturamento.cdempresa,
        cdcartafrete: d.dadosFaturamento.cdcartafrete,
        cdempresaFV: d.dadosFaturamento.cdempresaFV,
        nrficha: d.dadosFaturamento.nrficha ?? null,
      },
    });
  });

  return res.status(202).json({ status: 'accepted' });
}

export async function faturaPagarCriar(req: Request, res: Response) {
  const payload = faturaPagarCriarSchema.parse(req.body);
  const d = payload.data;
  const faturaId = String(d.id);

  await prisma.$transaction(async (tx) => {
    await tx.faturaPagar.upsert({
      where: { id: faturaId },
      update: {
        fornecedorCnpj: d.corporation?.cnpj || '',
        numero: d.document,
        emissao: d.issue_date,
        valor: parseFloat(d.value),
      },
      create: {
        id: faturaId,
        fornecedorCnpj: d.corporation?.cnpj || '',
        numero: d.document,
        emissao: d.issue_date,
        valor: parseFloat(d.value),
      },
    });

    await tx.faturaPagarParcela.deleteMany({ where: { faturaId } });
    if (d.installments && d.installments.length > 0) {
      await tx.faturaPagarParcela.createMany({
        data: d.installments.map((p) => ({
          faturaId,
          posicao: p.position,
          dueDate: p.due_date,
          valor: parseFloat(p.value),
          interestValue: p.interest_value ? parseFloat(p.interest_value) : null,
          discountValue: p.discount_value ? parseFloat(p.discount_value) : null,
          paymentMethod: p.payment_method || null,
          comments: p.comments || null,
          installmentId: p.id,
        })),
      });
    }
  });

  return res.status(202).json({ status: 'accepted' });
}

export async function faturaPagarBaixar(req: Request, res: Response) {
  const d = faturaPagarBaixarSchema.parse(req.body);

  const parcela = await prisma.faturaPagarParcela.findFirst({ where: { installmentId: d.installment_id } });
  if (!parcela) {
    return res.status(404).json({ error: 'Parcela não encontrada para installment_id' });
  }

  await prisma.faturaPagarBaixa.create({
    data: {
      parcelaId: parcela.id,
      pagamentoEm: d.payment_date,
      valorPago: d.payment_value,
      desconto: d.discount_value ?? null,
      juros: d.interest_value ?? null,
      formaPagamento: d.payment_method ?? null,
      contaBancaria: d.bank_account ?? null,
      banco: d.bankname ?? null,
      numeroConta: d.accountnumber ?? null,
      digitoConta: d.accountdigit ?? null,
      observacao: d.comments ?? null,
    },
  });

  return res.status(202).json({ status: 'accepted' });
}

export async function faturaPagarCancelar(req: Request, res: Response) {
  const data = faturaPagarCancelarSchema.parse(req.body);
  await prisma.faturaPagarCancelamento.upsert({ where: { id: data.id }, update: data, create: data });
  return res.status(202).json({ status: 'accepted' });
}

export async function faturaReceberCriar(req: Request, res: Response) {
  const payload = faturaReceberCriarSchema.parse(req.body);
  const d = payload.data;

  const faturaId = String(d.id);

  await prisma.$transaction(async (tx) => {
    await tx.faturaReceber.upsert({
      where: { id: faturaId },
      update: {
        clienteCnpj: d.customer?.cnpj || '',
        numero: d.document,
        emissao: d.issue_date,
        valor: parseFloat(d.value),
      },
      create: {
        id: faturaId,
        clienteCnpj: d.customer?.cnpj || '',
        numero: d.document,
        emissao: d.issue_date,
        valor: parseFloat(d.value),
      },
    });

    await tx.faturaReceberParcela.deleteMany({ where: { faturaId } });
    if (d.installments && d.installments.length > 0) {
      await tx.faturaReceberParcela.createMany({
        data: d.installments.map((p) => ({
          faturaId,
          posicao: p.position,
          dueDate: p.due_date,
          valor: parseFloat(p.value),
          interestValue: p.interest_value ? parseFloat(p.interest_value) : null,
          discountValue: p.discount_value ? parseFloat(p.discount_value) : null,
          paymentMethod: p.payment_method || null,
          comments: p.comments || null,
          installmentId: p.id,
        })),
      });
    }

    await tx.faturaReceberItem.deleteMany({ where: { faturaId } });
    if (d.invoice_items && d.invoice_items.length > 0) {
      await tx.faturaReceberItem.createMany({
        data: d.invoice_items.map((it) => ({
          faturaId,
          cteKey: it.cte_key || null,
          cteNumber: it.cte_number || null,
          cteSeries: it.cte_series || null,
          payerName: it.payer_name || null,
          draftNumber: it.draft_number || null,
          nfseNumber: it.nfse_number || null,
          nfseSeries: it.nfse_series || null,
          total: it.total ? parseFloat(it.total) : null,
          type: it.type || null,
        })),
      });
    }
  });

  return res.status(202).json({ status: 'accepted' });
}

export async function faturaReceberBaixar(req: Request, res: Response) {
  const d = faturaReceberBaixarSchema.parse(req.body);

  // Encontrar parcela por installment_id
  const parcela = await prisma.faturaReceberParcela.findFirst({ where: { installmentId: d.installment_id } });
  if (!parcela) {
    return res.status(404).json({ error: 'Parcela não encontrada para installment_id' });
  }

  await prisma.faturaReceberBaixa.create({
    data: {
      parcelaId: parcela.id,
      recebimentoEm: d.payment_date,
      valorRecebido: d.payment_value,
      desconto: d.discount_value ?? null,
      juros: d.interest_value ?? null,
      formaPagamento: d.payment_method ?? null,
      contaBancaria: d.bank_account ?? null,
      banco: d.bankname ?? null,
      numeroConta: d.accountnumber ?? null,
      digitoConta: d.accountdigit ?? null,
      observacao: d.comments ?? null,
    },
  });

  return res.status(202).json({ status: 'accepted' });
}

export async function nfseAutorizado(req: Request, res: Response) {
  const body = nfseAutorizadoSchema.parse(req.body);
  const n = body.infNFeCte;
  const id = `${n.prestadorServico.identificacaoPrestador.cnpj}-${n.numero}`;

  const valores = n.servico.valores;
  const logistica = body.dadosLogisticaFrete;

  await prisma.nfseAutorizado.upsert({
    where: { id },
    update: {
      numero: n.numero,
      codigoVerificacao: toNull(n.codigoVerificacao),
      dataEmissao: n.dataEmissao,
      rpsNumero: n.identificacaoRps.numero,
      rpsSerie: n.identificacaoRps.serie,
      rpsTipo: n.identificacaoRps.tipo,
      dataEmissaoRps: n.dataEmissaoRps,
      naturezaOperacao: n.naturezaOperacao,
      optanteSimples: n.optanteSimplesNacional,
      incentivadorCultural: n.incentivadorCultural,
      competencia: n.competencia,
      itemListaServico: n.servico.itemListaServico,
      codTribMunicipio: toString(n.servico.codigoTributacaoMunicipio),
      discriminacao: n.servico.discriminacao,
      codMunicipioServ: n.servico.codigoMunicipio,
      valorServicos: valores.valorServicos,
      valorDeducoes: valores.valorDeducoes,
      issRetido: valores.issRetido,
      valorIss: valores.valorIss,
      valorIssRetido: valores.valorIssRetido,
      baseCalculo: valores.baseCalculo,
      aliquota: valores.aliquota,
      valorLiquidoNfse: valores.valorLiquidoNfse,
      valorCredito: n.valorCredito,
      prestadorCnpj: n.prestadorServico.identificacaoPrestador.cnpj,
      prestadorIM: toString(n.prestadorServico.identificacaoPrestador.inscricaoMunicipal),
      prestadorRazao: n.prestadorServico.razaoSocial,
      prestadorFantasia: toString(n.prestadorServico.nomeFantasia),
      prestadorLogradouro: toNull(n.prestadorServico.endereco.logradouro),
      prestadorNumero: toNumberOrNull(n.prestadorServico.endereco.numero),
      prestadorComplemento: toNull(n.prestadorServico.endereco.complemento),
      prestadorBairro: toNull(n.prestadorServico.endereco.bairro),
      prestadorCodMunicipio: toNull(n.prestadorServico.endereco.codigoMunicipio),
      prestadorUf: toNull(n.prestadorServico.endereco.uf),
      prestadorCep: toNull(n.prestadorServico.endereco.cep),
      prestadorTelefone: toNull(n.prestadorServico.contato.telefone),
      prestadorEmail: toNull(n.prestadorServico.contato.email),
      tomadorCnpj: n.tomadorServico.identificacaoTomador.cnpj,
      tomadorIM: toString(n.tomadorServico.identificacaoTomador.inscricaoMunicipal),
      tomadorRazao: toString(n.tomadorServico.razaoSocial),
      tomadorLogradouro: toNull(n.tomadorServico.endereco.logradouro),
      tomadorNumero: toNumberOrNull(n.tomadorServico.endereco.numero),
      tomadorComplemento: toNull(n.tomadorServico.endereco.complemento),
      tomadorBairro: toNull(n.tomadorServico.endereco.bairro),
      tomadorCodMunicipio: toNull(n.tomadorServico.endereco.codigoMunicipio),
      tomadorUf: toNull(n.tomadorServico.endereco.uf),
      tomadorCep: toNull(n.tomadorServico.endereco.cep),
      tomadorTelefone: toNull(n.tomadorServico.contato.telefone),
      tomadorEmail: toNull(n.tomadorServico.contato.email),
      orgaoCodMunicipio: n.orgaoGerador.codigoMunicipio,
      orgaoUf: toString(n.orgaoGerador.uf),
      // Campos de logística
      pesoReal: toNull(logistica?.pesoreal),
      pesoCubado: toNull(logistica?.pesocubado),
      quantidadeVolumes: toNull(logistica?.quantidadevolumes),
      valorProduto: toNull(logistica?.valorproduto),
      valorNota: toNull(logistica?.valornota),
      valorFretePeso: toNull(logistica?.valorfretepeso),
      valorAdv: toNull(logistica?.valoradv),
      valorOutros: toNull(logistica?.valoroutros),
      comentarioFrete: logistica?.comentariofrete ? logistica.comentariofrete.substring(0, 4000) : null,
      usuarioAlteracao: toNull(logistica?.usuarioalteracao),
      dataAlteracao: toNull(logistica?.datadealteracao),
      dataCancelamento: toNull(logistica?.datacancelamento),
      motivoCancelamento: toNull(logistica?.motivocancelamento),
      filialCancelamento: toNull(logistica?.filialcancelamento),
      cnpjConsignatario: toNull(logistica?.cnpjConsignatario),
      cnpjRedespacho: logistica?.cnpjRedespacho && !logistica.cnpjRedespacho.includes('#') ? logistica.cnpjRedespacho : null,
      cnpjExpedidor: toNull(logistica?.cnpjexpedidor),
      valorBaseCalculoPis: toNull(logistica?.valorbasecalculopis),
      aliqPis: toNull(logistica?.aliqpis),
      valorPis: toNull(logistica?.valorpis),
      aliqCofins: toNull(logistica?.aliqcofins),
      valorCofins: toNull(logistica?.valorcofins),
      dataPrazo: toNull(logistica?.dataPrazo),
      diasEntrega: toNull(logistica?.diasentrega),
      cnpjRemetente: toNull(logistica?.cnpjRemetente),
      cepRemetente: toNull(logistica?.cepRemetente),
      cnpjDestinatario: toNull(logistica?.cnpjDestinatario),
      cepDestinatario: toNull(logistica?.cepDestinatario),
      logradouroDestinatario: toNull(logistica?.logradouroDestinatario),
    },
    create: {
      id,
      numero: n.numero,
      codigoVerificacao: toNull(n.codigoVerificacao),
      dataEmissao: n.dataEmissao,
      rpsNumero: n.identificacaoRps.numero,
      rpsSerie: n.identificacaoRps.serie,
      rpsTipo: n.identificacaoRps.tipo,
      dataEmissaoRps: n.dataEmissaoRps,
      naturezaOperacao: n.naturezaOperacao,
      optanteSimples: n.optanteSimplesNacional,
      incentivadorCultural: n.incentivadorCultural,
      competencia: n.competencia,
      itemListaServico: n.servico.itemListaServico,
      codTribMunicipio: toString(n.servico.codigoTributacaoMunicipio),
      discriminacao: n.servico.discriminacao,
      codMunicipioServ: n.servico.codigoMunicipio,
      valorServicos: valores.valorServicos,
      valorDeducoes: valores.valorDeducoes,
      issRetido: valores.issRetido,
      valorIss: valores.valorIss,
      valorIssRetido: valores.valorIssRetido,
      baseCalculo: valores.baseCalculo,
      aliquota: valores.aliquota,
      valorLiquidoNfse: valores.valorLiquidoNfse,
      valorCredito: n.valorCredito,
      prestadorCnpj: n.prestadorServico.identificacaoPrestador.cnpj,
      prestadorIM: toString(n.prestadorServico.identificacaoPrestador.inscricaoMunicipal),
      prestadorRazao: n.prestadorServico.razaoSocial,
      prestadorFantasia: toString(n.prestadorServico.nomeFantasia),
      prestadorLogradouro: toNull(n.prestadorServico.endereco.logradouro),
      prestadorNumero: toNumberOrNull(n.prestadorServico.endereco.numero),
      prestadorComplemento: toNull(n.prestadorServico.endereco.complemento),
      prestadorBairro: toNull(n.prestadorServico.endereco.bairro),
      prestadorCodMunicipio: toNull(n.prestadorServico.endereco.codigoMunicipio),
      prestadorUf: toNull(n.prestadorServico.endereco.uf),
      prestadorCep: toNull(n.prestadorServico.endereco.cep),
      prestadorTelefone: toNull(n.prestadorServico.contato.telefone),
      prestadorEmail: toNull(n.prestadorServico.contato.email),
      tomadorCnpj: n.tomadorServico.identificacaoTomador.cnpj,
      tomadorIM: toString(n.tomadorServico.identificacaoTomador.inscricaoMunicipal),
      tomadorRazao: toString(n.tomadorServico.razaoSocial),
      tomadorLogradouro: toNull(n.tomadorServico.endereco.logradouro),
      tomadorNumero: toNumberOrNull(n.tomadorServico.endereco.numero),
      tomadorComplemento: toNull(n.tomadorServico.endereco.complemento),
      tomadorBairro: toNull(n.tomadorServico.endereco.bairro),
      tomadorCodMunicipio: toNull(n.tomadorServico.endereco.codigoMunicipio),
      tomadorUf: toNull(n.tomadorServico.endereco.uf),
      tomadorCep: toNull(n.tomadorServico.endereco.cep),
      tomadorTelefone: toNull(n.tomadorServico.contato.telefone),
      tomadorEmail: toNull(n.tomadorServico.contato.email),
      orgaoCodMunicipio: n.orgaoGerador.codigoMunicipio,
      orgaoUf: toString(n.orgaoGerador.uf),
      // Campos de logística
      pesoReal: toNull(logistica?.pesoreal),
      pesoCubado: toNull(logistica?.pesocubado),
      quantidadeVolumes: toNull(logistica?.quantidadevolumes),
      valorProduto: toNull(logistica?.valorproduto),
      valorNota: toNull(logistica?.valornota),
      valorFretePeso: toNull(logistica?.valorfretepeso),
      valorAdv: toNull(logistica?.valoradv),
      valorOutros: toNull(logistica?.valoroutros),
      comentarioFrete: logistica?.comentariofrete ? logistica.comentariofrete.substring(0, 4000) : null,
      usuarioAlteracao: toNull(logistica?.usuarioalteracao),
      dataAlteracao: toNull(logistica?.datadealteracao),
      dataCancelamento: toNull(logistica?.datacancelamento),
      motivoCancelamento: toNull(logistica?.motivocancelamento),
      filialCancelamento: toNull(logistica?.filialcancelamento),
      cnpjConsignatario: toNull(logistica?.cnpjConsignatario),
      cnpjRedespacho: logistica?.cnpjRedespacho && !logistica.cnpjRedespacho.includes('#') ? logistica.cnpjRedespacho : null,
      cnpjExpedidor: toNull(logistica?.cnpjexpedidor),
      valorBaseCalculoPis: toNull(logistica?.valorbasecalculopis),
      aliqPis: toNull(logistica?.aliqpis),
      valorPis: toNull(logistica?.valorpis),
      aliqCofins: toNull(logistica?.aliqcofins),
      valorCofins: toNull(logistica?.valorcofins),
      dataPrazo: toNull(logistica?.dataPrazo),
      diasEntrega: toNull(logistica?.diasentrega),
      cnpjRemetente: toNull(logistica?.cnpjRemetente),
      cepRemetente: toNull(logistica?.cepRemetente),
      cnpjDestinatario: toNull(logistica?.cnpjDestinatario),
      cepDestinatario: toNull(logistica?.cepDestinatario),
      logradouroDestinatario: toNull(logistica?.logradouroDestinatario),
    },
  });
  return res.status(202).json({ status: 'accepted' });
}

export async function pessoaUpsert(req: Request, res: Response) {
  const data = pessoaSchema.parse(req.body);

  const enderecos = (data.enderecoList || []).map((e) => ({
    codTipoEndereco: e.tipoEndereco.codTipoEndereco,
    descricaoTipoEndereco: e.tipoEndereco.descricao,
    cep: e.cep ?? null,
    logradouro: e.logradouro ?? null,
    numero: e.numero ?? null,
    complemento: e.complemento ?? null,
    bairro: e.bairro ?? null,
    cidade: e.cidade ?? null,
    estado: e.estado ?? null,
  }));

  const pessoaData = {
    id: data.codPessoaEsl,
    nomeRazaoSocial: data.nomeRazaoSocial ?? null,
    nomeFantasia: data.nomeFantasia ?? null,
    cpf: data.cpf || null,
    cnpj: data.cnpj || null,
    inscricaoMunicipal: data.inscricaoMunicipal || null,
    inscricaoEstadual: data.inscricaoEstadual || null,
    ativo: data.ativo ?? null,
    dataCadastro: data.dataCadastro ?? null,
    usuarioCadastro: data.usuarioCadastro ?? null,
    payload: JSON.stringify(data),
  } as const;

  await prisma.$transaction(async (tx) => {
    await tx.pessoa.upsert({
      where: { id: pessoaData.id },
      update: pessoaData,
      create: pessoaData,
    });

    // Remove endereços anteriores e recria (estratégia simples)
    await tx.pessoaEndereco.deleteMany({ where: { pessoaId: pessoaData.id } });
    if (enderecos.length > 0) {
      await tx.pessoaEndereco.createMany({ data: enderecos.map((e) => ({ ...e, pessoaId: pessoaData.id })) });
    }
  });

  return res.status(202).json({ status: 'accepted' });
}

// Função para inserir na tabela nfse (estrutura do banco do cliente)
export async function nfseInserir(req: Request, res: Response) {
  try {
    logger.info({ 
      bodyKeys: Object.keys(req.body || {}),
      hasInfNFeCte: !!req.body?.infNFeCte,
    }, 'Iniciando inserção de NFSe');
    
    const body = nfseAutorizadoSchema.parse(req.body);
    const n = body.infNFeCte;
    const logistica = body.dadosLogisticaFrete;

    // Converter datas de string para DateTime
    const parseDate = (dateStr: string | undefined): Date | null => {
      if (!dateStr) return null;
      try {
        return new Date(dateStr);
      } catch {
        return null;
      }
    };

    // Extrair CPF do CNPJ do tomador se necessário (assumindo que pode vir como CPF)
    const tomadorCnpj = n.tomadorServico.identificacaoTomador.cnpj || '';
    const tomadorCpf = tomadorCnpj.length === 11 ? tomadorCnpj : null;
    const tomadorCnpjFinal = tomadorCnpj.length === 14 ? tomadorCnpj : null;

    await prisma.nfse.create({
      data: {
        NumeroNfse: n.numero,
        CodigoVerificacao: n.codigoVerificacao || null,
        DataEmissao: parseDate(n.dataEmissao),
        NumeroIdentificacaoRps: n.identificacaoRps.numero,
        SerieIdentificacaoRps: n.identificacaoRps.serie || null,
        TipoIdentificacaoRps: n.identificacaoRps.tipo,
        DataEmissaoRps: parseDate(n.dataEmissaoRps),
        NaturezaOperacao: n.naturezaOperacao,
        OptanteSimplesNacional: n.optanteSimplesNacional,
        IncentivadorCultural: n.incentivadorCultural,
        DtCompetencia: parseDate(n.competencia),
        ValorServicos: n.servico.valores.valorServicos,
        ValorDeducoes: n.servico.valores.valorDeducoes,
        IssRetido: n.servico.valores.issRetido,
        ValorIss: n.servico.valores.valorIss,
        ValorIssRetido: n.servico.valores.valorIssRetido,
        BaseCalculo: n.servico.valores.baseCalculo,
        Aliquota: n.servico.valores.aliquota,
        ValorLiquidoNfse: n.servico.valores.valorLiquidoNfse,
        ItemListaServico: n.servico.itemListaServico,
        CdTributacaoMunicipio: n.servico.codigoTributacaoMunicipio || null,
        Discriminacao: n.servico.discriminacao ? n.servico.discriminacao.substring(0, 255) : null,
        CodigoMunicipio: n.servico.codigoMunicipio,
        ValorCredito: n.valorCredito,
        // Campos obrigatórios - usando valores padrão se não vierem no JSON
        corporation_id: (req.body.corporation_id as number) || 1,
        external_idPrestador: (req.body.external_idPrestador as number) || 0,
        person_idPrestador: (req.body.person_idPrestador as number) || 0,
        CnpjIdentPrestador: n.prestadorServico.identificacaoPrestador.cnpj,
        InscMunicipalPrestador: n.prestadorServico.identificacaoPrestador.inscricaoMunicipal || null,
        RazaoSocialPrestador: n.prestadorServico.razaoSocial,
        NomeFantasiaPrestador: n.prestadorServico.nomeFantasia || null,
        CodEnderecoPrestador: (req.body.codEnderecoPrestador as number) || null,
        CodContatoPrestador: (req.body.codContatoPrestador as number) || null,
        customer_id: (req.body.customer_id as number) || 1,
        external_idTomador: (req.body.external_idTomador as number) || 0,
        CnpjIdentTomador: tomadorCnpjFinal,
        CpfIdentTomador: tomadorCpf,
        InscMunicipalTomador: n.tomadorServico.identificacaoTomador.inscricaoMunicipal || null,
        RazaoSocialTomador: n.tomadorServico.razaoSocial || null,
        CodEnderecoTomador: (req.body.codEnderecoTomador as number) || null,
        CodContatoTomador: (req.body.codContatoTomador as number) || null,
        CdMunicipioOrgaoGerador: n.orgaoGerador.codigoMunicipio,
        UFOrgaoGerador: n.orgaoGerador.uf || null,
        status: 'received',
        processed: false,
        cancelado: false,
        // Endereço do prestador
        Prestador_Logradouro: n.prestadorServico.endereco.logradouro || null,
        Prestador_Numero: String(n.prestadorServico.endereco.numero) || null,
        Prestador_Complemento: n.prestadorServico.endereco.complemento || null,
        Prestador_Bairro: n.prestadorServico.endereco.bairro || null,
        Prestador_CodigoMunicipio: n.prestadorServico.endereco.codigoMunicipio,
        Prestador_UF: n.prestadorServico.endereco.uf || null,
        Prestador_CEP: n.prestadorServico.endereco.cep || null,
        Prestador_Telefone: n.prestadorServico.contato.telefone || null,
        Prestador_Email: n.prestadorServico.contato.email || null,
        // Endereço do tomador
        Tomador_Logradouro: n.tomadorServico.endereco.logradouro || null,
        Tomador_Numero: String(n.tomadorServico.endereco.numero) || null,
        Tomador_Complemento: n.tomadorServico.endereco.complemento || null,
        Tomador_Bairro: n.tomadorServico.endereco.bairro || null,
        Tomador_CodigoMunicipio: n.tomadorServico.endereco.codigoMunicipio,
        Tomador_UF: n.tomadorServico.endereco.uf || null,
        Tomador_CEP: n.tomadorServico.endereco.cep || null,
        Tomador_Telefone: n.tomadorServico.contato.telefone || null,
        Tomador_Email: n.tomadorServico.contato.email || null,
        // Campos de logística
        PesoReal: logistica?.pesoreal || null,
        PesoCubado: logistica?.pesocubado || null,
        QuantidadeVolumes: logistica?.quantidadevolumes || null,
        ValorProduto: logistica?.valorproduto || null,
        ValorNota: logistica?.valornota || null,
        ValorFretePeso: logistica?.valorfretepeso || null,
        ValorAdv: logistica?.valoradv || null,
        ValorOutros: logistica?.valoroutros || null,
        ComentarioFrete: logistica?.comentariofrete || null,
        UsuarioAlteracao: logistica?.usuarioalteracao || null,
        DataDeAlteracao: parseDate(logistica?.datadealteracao),
        DataCancelamento: parseDate(logistica?.datacancelamento || undefined),
        MotivoCancelamento: logistica?.motivocancelamento || null,
        FilialCancelamento: logistica?.filialcancelamento || null,
        CnpjConsignatario: logistica?.cnpjConsignatario || null,
        CnpjRedespacho: logistica?.cnpjRedespacho && !logistica.cnpjRedespacho.includes('#') ? logistica.cnpjRedespacho : null,
        CnpjExpedidor: logistica?.cnpjexpedidor || null,
        ValorBaseCalculoPIS: logistica?.valorbasecalculopis || null,
        AliqPIS: logistica?.aliqpis || null,
        ValorPIS: logistica?.valorpis || null,
        AliqCOFINS: logistica?.aliqcofins || null,
        ValorCOFINS: logistica?.valorcofins || null,
        DataPrazo: parseDate(logistica?.dataPrazo),
        DiasEntrega: logistica?.diasentrega || null,
        CnpjRemetente: logistica?.cnpjRemetente || null,
        CepRemetente: logistica?.cepRemetente || null,
        CnpjDestinatario: logistica?.cnpjDestinatario || null,
        CepDestinatario: logistica?.cepDestinatario || null,
        LogradouroDestinatario: logistica?.logradouroDestinatario || null,
        NumeroJsonOriginal: n.numero,
      },
    });

    return res.status(201).json({ status: 'created', message: 'NFSe inserida com sucesso' });
  } catch (error: any) {
    // Erro de validação Zod
    if (error.name === 'ZodError') {
      const errorDetails = error.errors.map((err: any) => ({
        campo: err.path.join('.'),
        mensagem: err.message,
        valorRecebido: err.input,
      }));
      
      logger.warn({ 
        errors: error.errors,
        errorDetails,
        bodyKeys: Object.keys(req.body || {}),
      }, 'Erro de validação no schema NFSe');
      
      return res.status(400).json({
        Status: false,
        Mensagem: 'Dados inválidos',
        Erros: errorDetails,
      });
    }

    logger.error({ error: error.message, stack: error.stack }, 'Erro ao inserir NFSe');
    return res.status(500).json({ 
      error: 'Erro ao inserir NFSe', 
      details: process.env.NODE_ENV === 'development' ? error.message : undefined 
    });
  }
}


