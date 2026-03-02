import type { Prisma, PrismaClient } from '@prisma/client';
import { logger } from '../utils/logger';
import { env } from '../config/env';
import type {
  Manifest,
  ManifestParcelas,
  ManifestFaturamento,
  ContasPagarCIOTPayload,
} from '../types/ciot';
import {
  buscarValoresEventos,
  calcularAcrescimoDesconto,
  identificarCenario,
} from './ciotRulesHelper';

type PrismaExecutor = PrismaClient | Prisma.TransactionClient;

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

const currentAno2 = (): string => {
  return new Date().getFullYear().toString().slice(-2);
};

const padLeft = (value: string | null | undefined, length: number): string => {
  if (!value) return ''.padStart(length, '0');
  const onlyDigits = value.replace(/\D/g, '');
  return onlyDigits.padStart(length, '0');
};

const ensureNumber = (value: number | null | undefined): number => {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return 0;
  return Number(value);
};

const ensureDecimal = (value: number | null | undefined): string => {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '0';
  return String(value);
};

const buildCdEmpresaFromCnpj = async (
  prisma: PrismaExecutor,
  cnpj: string | null | undefined,
): Promise<number> => {
  if (!cnpj) {
    return 300;
  }
  const cnpjClean = padLeft(cnpj, 14);
  const sql = `EXEC dbo.P_EMPRESA_SENIOR_POR_CNPJ_LISTAR @Cnpj = ${toSqlValue(cnpjClean)}`;
  try {
    const rows = await prisma.$queryRawUnsafe<Array<{ codEmpresa: number }>>(sql);
    if (rows && rows[0] && rows[0].codEmpresa) {
      return Number(rows[0].codEmpresa);
    }
  } catch (error: any) {
    logger.warn(
      { error: error.message, cnpj: cnpjClean },
      'Erro ao buscar código de empresa na Senior, usando padrão 300',
    );
  }
  return 300;
};

/**
 * Calcula o código de carta frete/fatura no padrão:
 *   RetornaCodEmpresa(cdempresaFV) + ano2 + nrmanifesto
 */
const buildCdCartaFrete = async (
  prisma: PrismaExecutor,
  manifest: Manifest,
  faturamento?: ManifestFaturamento,
): Promise<number> => {
  const cnpjFV = faturamento?.cdempresaFV || faturamento?.cdempresa || manifest.cdempresa;
  const codEmpresaFV = await buildCdEmpresaFromCnpj(prisma, cnpjFV || null);
  const ano = currentAno2();
  const nrmanifesto = manifest.nrmanifesto || '0';
  const concat = `${codEmpresaFV}${ano}${nrmanifesto}`;
  return Number(concat);
};

/**
 * FTRCFT – manifesto principal na Senior.
 * Versão espelhada do InsereFTRCFT (C#), com pequenos simplificadores.
 */
const executarFTRCFT = async (prisma: PrismaExecutor, manifest: Manifest): Promise<void> => {
  const faturamento = manifest.dadosFaturamento;
  if (!faturamento) {
    throw new Error('Dados de faturamento não informados para execução do FTRCFT');
  }

  const cdEmpresa = await buildCdEmpresaFromCnpj(prisma, faturamento.cdempresa);
  const cdCartaFrete = await buildCdCartaFrete(prisma, manifest, faturamento);

  // Usar cdremetente e cddestinatario diretamente da tabela manifests (valor original)
  // Não converter - usar exatamente como está na tabela manifests
  const cdRemetente = manifest.cdremetente || '';
  const cdDestinatario = manifest.cddestinatario || '';

  logger.info(
    {
      cdremetenteOriginal: cdRemetente,
      cddestinatarioOriginal: cdDestinatario,
      manifestId: manifest.id || manifest.external_id,
    },
    'Usando cdremetente e cddestinatario originais da tabela manifests (sem conversão)',
  );

  // Excluir antes de incluir (idempotência)
  logger.info(
    {
      manifestId: manifest.id || manifest.external_id,
      cdEmpresa,
      cdCartaFrete,
      cdRemetente,
      cdDestinatario,
      timestamp: new Date().toISOString(),
    },
    'ANTES de excluir FTRCFT - valores que serão inseridos',
  );

  const excluirSql = `
    EXEC dbo.P_CONTAS_PAGAR_CIOT_FTRCFT_ESL_EXCLUIR
      @CdEmpresa = ${cdEmpresa},
      @CdCartaFrete = ${cdCartaFrete};
  `;
  await prisma.$executeRawUnsafe(excluirSql);

  logger.info(
    {
      manifestId: manifest.id || manifest.external_id,
      cdEmpresa,
      cdCartaFrete,
      timestamp: new Date().toISOString(),
    },
    'APÓS excluir FTRCFT - antes de inserir',
  );

  const vlTotal = ensureNumber(manifest.vltotalmanifesto);
  const vlPedagio = ensureNumber(manifest.vlpedagio);
  const vlAdiantamento = ensureNumber(manifest.vladiantamento);

  let cdCondicaoVencto = 1;
  if (vlTotal > 0 && vlAdiantamento > 0 && vlPedagio === 0) {
    cdCondicaoVencto = 3;
  } else if (vlTotal > 0 && vlAdiantamento > 0 && vlPedagio > 0) {
    cdCondicaoVencto = 5;
  }

  const nrCepOrigem = manifest.nrceporigem ? Number(manifest.nrceporigem.replace('-', '')) : 0;
  const nrCepDestino = manifest.nrcepdestino ? Number(manifest.nrcepdestino.replace('-', '')) : 0;

  const dtlib = manifest.dtliberacaopagto || manifest.dtemissao || null;

  // Usar valores originais como string (sem conversão)
  const cdRemetenteParam = toSqlValue(cdRemetente);
  const cdDestinatarioParam = toSqlValue(cdDestinatario);

  // Log do valor final que será passado para a stored procedure
  logger.info(
    {
      manifestId: manifest.id || manifest.external_id,
      cdEmpresa,
      cdCartaFrete,
      cdRemetenteParam: cdRemetente,
      cdDestinatarioParam: cdDestinatario,
    },
    'Valores finais que serão passados para P_CONTAS_PAGAR_CIOT_FTRCFT_ESL_INCLUIR (valores originais da tabela manifests)',
  );

  const sql = `
    EXEC dbo.P_CONTAS_PAGAR_CIOT_FTRCFT_ESL_INCLUIR
      @CdEmpresa = ${cdEmpresa},
      @CdCartaFrete = ${cdCartaFrete},
      @NrCGCCPFPropr = ${toSqlValue(padLeft(manifest.nrcgccpfprop, 14))},
      @NrCGCCPFMot = ${toSqlValue(padLeft(manifest.nrcgccpfmot, 14))},
      @DtEmissao = ${parseDate(manifest.dtemissao || null)},
      @VlCarga = ${ensureDecimal(manifest.vlcarga ?? 0)},
      @QtPesoCarga = ${ensureDecimal(manifest.qtpesocarga ?? 0)},
      @NrManifesto = ${toSqlValue(manifest.nrmanifesto)},
      @NrPlaca = ${toSqlValue(manifest.nrplaca)},
      @CdExpedidor = 2,
      @NrCepOrigem = ${nrCepOrigem},
      @NrCepDestino = ${nrCepDestino},
      @FgEmitida = ${ensureNumber(manifest.fgemitida ? Number(manifest.fgemitida) : 0)},
      @DtLiberacaoPagto = ${parseDate(dtlib)},
      @CdCentroCusto = ${ensureNumber(Number(manifest.cdcentrocusto) || 0)},
      @DtVencimento = ${parseDate(dtlib)},
      @InSituacao = 0,
      @CdCondicaoVencto = ${cdCondicaoVencto},
      @DsObservacao = ${toSqlValue(manifest.dsobservacao || '')},
      @CdTipoTransporte = ${ensureNumber(Number(manifest.cdtipotransporte) || 0)},
      @CdRemetente = ${cdRemetenteParam},
      @CdDestinatario = ${cdDestinatarioParam},
      @CdNaturezaCarga = 0,
      @CdEspecieCarga = 0,
      @VlMercadoria = ${ensureDecimal(manifest.clmercadoria ? Number(manifest.clmercadoria) : 0)},
      @QtPeso = ${ensureDecimal(manifest.qtpeso ?? 0)},
      @CdEmpresaConhec = ${ensureNumber(Number(manifest.cdempresaconhec) || 0)},
      @NrSeqControle = ${ensureNumber(Number(manifest.nrseqcontrole) || 0)},
      @NrNotaFiscal = ${toSqlValue(manifest.nrnotafiscal || '')},
      @CdHistorico = ${ensureNumber(Number(manifest.cdhistorico) || 0)},
      @DsUsuarioInc = ${toSqlValue((manifest.dsusuarioinc || 'importacaoAFS').substring(0, 10))},
      @DsUsuarioCanc = ${toSqlValue((manifest.dsusuariocanc || '').substring(0, 10))},
      @DtInclusao = ${parseDate(manifest.dtinclusao || manifest.dtemissao || null)},
      @InTipoOrigem = 3,
      @NrPlacaReboque1 = ${toSqlValue(manifest.nrplacareboque1 || '')},
      @NrPlacaReboque2 = ${toSqlValue(manifest.nrplacareboque2 || '')},
      @NrPlacaReboque3 = ${toSqlValue(manifest.nrplacareboque3 || '')},
      @CdTarifa = ${ensureNumber(manifest.cdtarifa || 0)},
      @DsUsuarioAcerto = ${toSqlValue((manifest.dsusuarioacerto || '').substring(0, 10))},
      @CdInscricaoComp = ${toSqlValue(manifest.cdinscricaocomp || '')},
      @NrSerieComp = ${toSqlValue(manifest.nrseriecomp || '')},
      @NrComprovante = ${ensureNumber(manifest.nrcomprovante ? Number(manifest.nrcomprovante) : 0)},
      @VlFrete = ${ensureDecimal(manifest.vlfrete ?? 0)},
      @InSestSenat = ${ensureNumber(manifest.insestsenat ? Number(manifest.insestsenat) : 0)},
      @CdMotivoCancelamento = ${ensureNumber(
        manifest.cdmotivocancelamento ? Number(manifest.cdmotivocancelamento) : 0,
      )},
      @DsObsCancelamento = ${toSqlValue(manifest.dsobscancelamento || '')},
      @InVeiculoProprio = ${ensureNumber(manifest.inveiculoproprio || 0)},
      @DsUsuarioImpressao = ${toSqlValue((manifest.dsusuarioimpressao || '').substring(0, 10))},
      @DtPrazoMaxEntrega = ${parseDate(manifest.dtprazomaxentrega || null)},
      @NrSeloAutenticidade = ${ensureNumber(
        manifest.nrseloautenticidade ? Number(manifest.nrseloautenticidade) : 0,
      )},
      @CdVinculacaoISS = ${ensureNumber(
        manifest.cdvinculacaoiss ? Number(manifest.cdvinculacaoiss) : 0,
      )},
      @CdCIOT = ${toSqlValue(manifest.cdciot || '')},
      @NrSerie = ${ensureNumber(
        manifest.nrseriecomp ? Number(manifest.nrseriecomp) : manifest.serie || 0,
      )},
      @CdMsgRetornoCIOT = ${ensureNumber(
        manifest.cdmsgretornociot ? Number(manifest.cdmsgretornociot) : 0,
      )},
      @DsMsgRetornoCIOT = ${toSqlValue(manifest.dsmsgretornociot || '')},
      @DsObsRetornoCIOT = ${toSqlValue(manifest.dsmsgretornociot || '')},
      @InEnvioArquivoCIOT = ${ensureNumber(
        manifest.inenvioarquivociot ? Number(manifest.inenvioarquivociot) : 0,
      )},
      @DsAvisoTransportador = ${toSqlValue(manifest.dsavisotransportador || '')},
      @NrProtocoloAutCIOT = ${toSqlValue(manifest.nrprotocoloautndot || '')},
      @NrProtocoloCancCIOT = ${toSqlValue(manifest.nrprotocolocancciot || '')},
      @CdNDOT = ${toSqlValue(manifest.cdndot || '')},
      @NrProtocoloAutNDOT = ${toSqlValue(manifest.nrprotocoloautndot || '')},
      @InOperacaoPeriodo = ${ensureNumber(
        manifest.inoperacaoperiodo ? Number(manifest.inoperacaoperiodo) : 0,
      )},
      @VlFreteEstimado = ${ensureDecimal(manifest.vlfreteestimado ?? 0)},
      @InOperacaoDistribuicao = ${ensureNumber(manifest.inoperacaodistribuicao || 0)},
      @NrProtocoloEnctoCIOT = ${toSqlValue(manifest.nrprotocoloenctociot || '')},
      @InDOTImpresso = ${ensureNumber(manifest.indotimpresso ? Number(manifest.indotimpresso) : 0)},
      @InVeiculo = ${ensureNumber(manifest.inveiculo || 0)},
      @CdRota = ${ensureNumber(manifest.cdrota ? Number(manifest.cdrota) : 0)},
      @InOperadoraPagtoCTRB = 5,
      @InRespostaQuestTACAgreg = ${ensureNumber(manifest.inrespostaquesttacagreg || 0)},
      @CdMoeda = ${ensureNumber(manifest.cdmoeda ? Number(manifest.cdmoeda) : 1)},
      @NrProtocoloInterropCIOT = ${toSqlValue(manifest.nrprotocolointerroociot || '')},
      @InRetImposto = ${ensureNumber(manifest.inretimposto ? Number(manifest.inretimposto) : 0)},
      @CdIntegSenior = ${ensureNumber(manifest.cdintersenior ? Number(manifest.cdintersenior) : 0)},
      @NrCodigoOperPagtoCIOT = ${toSqlValue(manifest.nrcodigooperpagtociot || '')},
      @CdSeqHCM = ${ensureNumber(manifest.cdseqhcm ? Number(manifest.cdseqhcm) : 0)},
      @inSitCalcPedagio = ${ensureNumber(
        manifest.insitcalcpedagio ? Number(manifest.insitcalcpedagio) : 0,
      )},
      @NRREPOM = ${toSqlValue(manifest.nrrepom || '')};
  `;

  logger.info(
    {
      manifestId: manifest.id || manifest.external_id,
      cdEmpresa,
      cdCartaFrete,
      cdRemetenteParam: cdRemetente,
      cdDestinatarioParam: cdDestinatario,
      timestamp: new Date().toISOString(),
    },
    'EXECUTANDO P_CONTAS_PAGAR_CIOT_FTRCFT_ESL_INCLUIR - valores sendo passados',
  );

  await prisma.$executeRawUnsafe(sql);

  logger.info(
    {
      manifestId: manifest.id || manifest.external_id,
      cdEmpresa,
      cdCartaFrete,
      timestamp: new Date().toISOString(),
    },
    'APÓS executar P_CONTAS_PAGAR_CIOT_FTRCFT_ESL_INCLUIR - inserção concluída',
  );
};

/**
 * FTRCFTFV – vínculo carta frete x ficha
 */
const executarFTRCFTFV = async (prisma: PrismaExecutor, manifest: Manifest): Promise<void> => {
  const faturamento = manifest.dadosFaturamento;
  if (!faturamento) return;

  const cdEmpresa = await buildCdEmpresaFromCnpj(prisma, faturamento.cdempresa);
  const cdEmpresaFV = await buildCdEmpresaFromCnpj(prisma, faturamento.cdempresa);
  const cdCartaFrete = await buildCdCartaFrete(prisma, manifest, faturamento);
  const nrFicha = cdCartaFrete;

  const excluirSql = `
    EXEC dbo.P_CONTAS_PAGAR_CIOT_FTRCFTFV_ESL_EXCLUIR
      @CdEmpresa = ${cdEmpresa},
      @CdCartaFrete = ${cdCartaFrete},
      @CdEmpresaFV = ${cdEmpresaFV},
      @NrFicha = ${nrFicha};
  `;
  await prisma.$executeRawUnsafe(excluirSql);

  const incluirSql = `
    EXEC dbo.P_CONTAS_PAGAR_CIOT_FTRCFTFV_ESL_INCLUIR
      @CdEmpresa = ${cdEmpresa},
      @CdCartaFrete = ${cdCartaFrete},
      @CdEmpresaFV = ${cdEmpresaFV},
      @NrFicha = ${nrFicha};
  `;
  await prisma.$executeRawUnsafe(incluirSql);
};

/**
 * FTRCFTFM – dados de rota / quilometragem (versão simplificada como no C#)
 */
const executarFTRCFTFM = async (prisma: PrismaExecutor, manifest: Manifest): Promise<void> => {
  const faturamento = manifest.dadosFaturamento;
  if (!faturamento) return;

  const cdEmpresa = await buildCdEmpresaFromCnpj(prisma, faturamento.cdempresa);
  const cdCartaFrete = await buildCdCartaFrete(prisma, manifest, faturamento);

  const excluirSql = `
    EXEC dbo.P_CONTAS_PAGAR_CIOT_FTRCFTFM_ESL_EXCLUIR
      @CdEmpresa = ${cdEmpresa},
      @CdCartaFrete = ${cdCartaFrete};
  `;
  await prisma.$executeRawUnsafe(excluirSql);

  const incluirSql = `
    EXEC dbo.P_CONTAS_PAGAR_CIOT_FTRCFTFM_ESL_INCLUIR
      @CdEmpresa = ${cdEmpresa},
      @CdCartaFrete = ${cdCartaFrete},
      @QtEixos = 0,
      @NrCepOrigem = 0,
      @NrCepDestino = 0,
      @CdRota = 0,
      @CdNaturezaCarga = 0,
      @QtQuilometragem = 0;
  `;
  await prisma.$executeRawUnsafe(incluirSql);
};

/**
 * FTRCFTMV – eventos (Saldo, Adiantamento, Pedágio, impostos etc.)
 */
const executarFTRCFTMV = async (
  prisma: PrismaExecutor,
  manifest: Manifest,
  parcelas: ManifestParcelas[],
): Promise<void> => {
  const faturamento = manifest.dadosFaturamento;
  if (!faturamento) return;

  const cdEmpresa = await buildCdEmpresaFromCnpj(prisma, faturamento.cdempresa);
  const cdCartaFrete = await buildCdCartaFrete(prisma, manifest, faturamento);

  const execEvento = async (codevento: number, parcela?: ManifestParcelas) => {
    const excluirSql = `
      EXEC dbo.P_CONTAS_PAGAR_CIOT_FTRCFTMV_ESL_EXCLUIR
        @CdEmpresa = ${cdEmpresa},
        @CdCartaFrete = ${cdCartaFrete},
        @CdEvento = ${codevento};
    `;
    await prisma.$executeRawUnsafe(excluirSql);

    let vlEvento = 0;
    let dtPagto: string | null = null;
    let dsObservacao = '';

    switch (codevento) {
      case 1: // saldo
        vlEvento = ensureNumber(manifest.vltotalmanifesto);
        dtPagto = parcela?.dtpagto || null;
        dsObservacao = parcela?.dsobservacao || manifest.dsobservacao || '';
        break;
      case 2: // adiantamento
        vlEvento = ensureNumber(manifest.vladiantamento);
        dtPagto = parcela?.dtvencimento || null;
        dsObservacao = parcela?.dsobservacao || manifest.dsobservacao || '';
        break;
      case 3: // inss
        vlEvento = ensureNumber(manifest.vlinss);
        dtPagto = parcelas[parcelas.length - 1]?.dtpagto || null;
        dsObservacao = manifest.dsobservacao || '';
        break;
      case 4: // irrf
        vlEvento = ensureNumber(manifest.vlir);
        dtPagto = parcelas[parcelas.length - 1]?.dtpagto || null;
        dsObservacao = manifest.dsobservacao || '';
        break;
      case 5: // sest senat
        vlEvento = ensureNumber(manifest.vlsestsenat);
        dtPagto = parcelas[parcelas.length - 1]?.dtpagto || null;
        dsObservacao = manifest.dsobservacao || '';
        break;
      case 6: // pedágio
        vlEvento = ensureNumber(manifest.vlpedagio);
        dtPagto = parcela?.dtpagto || null;
        dsObservacao = parcela?.dsobservacao || manifest.dsobservacao || '';
        break;
      case 8: // Nota de Crédito (carga/descarga)
        vlEvento = ensureNumber(manifest.vlnotacreditodebito);
        dtPagto = parcelas[parcelas.length - 1]?.dtpagto || null;
        dsObservacao = manifest.dsobservacao || '';
        break;
      case 10: // combustível
        vlEvento = ensureNumber(manifest.vlabastecimento);
        dtPagto = parcelas[parcelas.length - 1]?.dtpagto || null;
        dsObservacao = manifest.dsobservacao || '';
        break;
      case 12: // desconto - outros eventos (Nota de Débito)
        // O evento 12 será criado quando houver necessidade específica identificada
        // O valor pode ser baseado em campos específicos ou cálculos do manifesto
        // Por enquanto, verifica se há algum indicador de Nota de Débito
        // Se vlnotacreditodebito for negativo, pode indicar débito ao invés de crédito
        const vlNotaCreditoDebito = ensureNumber(manifest.vlnotacreditodebito);
        // Se for negativo, pode ser Nota de Débito
        if (vlNotaCreditoDebito < 0) {
          vlEvento = Math.abs(vlNotaCreditoDebito);
        } else {
          // Se não houver campo específico, usar valor 0 (evento não será criado)
          vlEvento = 0;
        }
        dtPagto = parcelas[parcelas.length - 1]?.dtpagto || null;
        dsObservacao = manifest.dsobservacao || '';
        break;
      default:
        return;
    }

    if (!vlEvento || vlEvento <= 0) return;

    const incluirSql = `
      EXEC dbo.P_CONTAS_PAGAR_CIOT_FTRCFTMV_ESL_INCLUIR
        @CdEmpresa = ${cdEmpresa},
        @CdCartaFrete = ${cdCartaFrete},
        @CdEvento = ${codevento},
        @VlEvento = ${ensureDecimal(vlEvento)},
        @DtPagto = ${parseDate(dtPagto)},
        @InDesconto = 0,
        @VlBaseCalculo = 0,
        @VlOriginal = 0,
        @DtInclusao = ${parseDate(manifest.dtemissao || null)},
        @HrInclusao = ${parseDate(manifest.dtemissao || null)},
        @DsUsuarioInc = ${toSqlValue('Integração ESL')},
        @DsObservacao = ${toSqlValue(dsObservacao)},
        @VlProvisionado = 0;
    `;
    await prisma.$executeRawUnsafe(incluirSql);
  };

  // Adiantamento, saldo e pedágio por parcela
  for (const parcela of parcelas) {
    if (parcela.dstipo === 'Adiantamento') {
      await execEvento(2, parcela);
    }
    if (parcela.dstipo === 'Saldo') {
      await execEvento(1, parcela);
    }
    if (parcela.dstipo === 'Pedagio') {
      await execEvento(6, parcela);
    }
  }

  // Impostos e outros (INSS, IR, SEST SENAT, Nota de Crédito, Combustível)
  if (ensureNumber(manifest.vlinss) > 0) await execEvento(3);
  if (ensureNumber(manifest.vlir) > 0) await execEvento(4);
  if (ensureNumber(manifest.vlsestsenat) > 0) await execEvento(5);

  // Evento 8 - Nota de Crédito (carga/descarga) - apenas se valor positivo
  const vlNotaCreditoDebito = ensureNumber(manifest.vlnotacreditodebito);
  if (vlNotaCreditoDebito > 0) {
    await execEvento(8);
  }

  if (ensureNumber(manifest.vlabastecimento) > 0) await execEvento(10);

  // Evento 12 - Desconto outros eventos (Nota de Débito) para CENÁRIO 3
  // Criar evento 12 se vlnotacreditodebito for negativo (indicando débito)
  // ou se houver outros indicadores de Nota de Débito
  if (vlNotaCreditoDebito < 0) {
    await execEvento(12);
  }
};

/**
 * GFAFATUR – fatura principal (CP). Retorna NrFatura.
 */
const executarGFAFATUR = async (prisma: PrismaExecutor, manifest: Manifest): Promise<string> => {
  const faturamento = manifest.dadosFaturamento;
  if (!faturamento) {
    throw new Error('Dados de faturamento não informados para execução do GFAFATUR');
  }

  const ano = currentAno2();
  const cdEmpresaFV = await buildCdEmpresaFromCnpj(prisma, faturamento.cdempresaFV);
  const nrmanifesto = manifest.nrmanifesto || '0';
  const nrFatura = Number(`${cdEmpresaFV}${ano}${nrmanifesto}`);

  const excluirSql = `
    EXEC dbo.P_INTEGRACAO_SENIOR_CP_GFAFATUR_EXCLUIR
      @CdInscricao = ${toSqlValue(padLeft(manifest.nrcgccpfprop, 14))},
      @NrFatura = ${nrFatura};
  `;
  await prisma.$executeRawUnsafe(excluirSql);

  const cdEmpresa = await buildCdEmpresaFromCnpj(prisma, faturamento.cdempresa);
  const vlFatura = ensureNumber(manifest.vltotalmanifesto) + ensureNumber(manifest.vlpedagio);

  // Mesma lógica de condição de vencimento usada em FTRCFT
  const vlTotal = ensureNumber(manifest.vltotalmanifesto);
  const vlPedagio = ensureNumber(manifest.vlpedagio);
  const vlAdiantamento = ensureNumber(manifest.vladiantamento);
  let cdCondicaoVencto = 1;
  if (vlTotal > 0 && vlAdiantamento > 0 && vlPedagio === 0) {
    cdCondicaoVencto = 3;
  } else if (vlTotal > 0 && vlAdiantamento > 0 && vlPedagio > 0) {
    cdCondicaoVencto = 5;
  }

  const incluirSql = `
    EXEC dbo.P_INTEGRACAO_SENIOR_CP_GFAFATUR_INCLUIR
      @CdInscricao = ${toSqlValue(padLeft(manifest.nrcgccpfprop, 14))},
      @NrFatura = ${nrFatura},
      @CdEmpresa = ${cdEmpresa},
      @CdCarteira = 1,
      @DtEmissao = ${parseDate(manifest.dtemissao || null)},
      @DtCompetencia = ${parseDate(manifest.dtemissao || null)},
      @CdCentroCusto = 900011,
      @CdCondicaoVencto = ${cdCondicaoVencto},
      @CdEspecieDocumento = 3,
      @CdPortador = NULL,
      @CdInstrucao = 1,
      @CdHistorico = 22,
      @DsComplemento = ${toSqlValue('')},
      @CdMoeda = 1,
      @CdPlanoConta = 11402001,
      @VlFatura = ${ensureDecimal(vlFatura)},
      @VlBruto = ${ensureDecimal(vlFatura)},
      @VlIrrf = ${ensureDecimal(manifest.vlir ?? 0)},
      @VlInss = ${ensureDecimal(manifest.vlinss ?? 0)},
      @VlDesconto = ${ensureDecimal(manifest.vldesconto ?? 0)},
      @InOrigem = 2,
      @InSituacao = 0,
      @DtCancelamento = NULL,
      @VlSestSenat = ${ensureDecimal(manifest.vlsestsenat ?? 0)},
      @VLCOFINS = 0,
      @VlPIS = 0,
      @VlCSLL = 0,
      @VlISS = 0,
      @CdTributacao = 0,
      @VlCSL = 0,
      @InOrigemEmissao = 1,
      @DsUsuarioAut = ${toSqlValue('importacaoAFS')},
      @DtInclusao = ${parseDate(new Date().toISOString())},
      @DsUsuarioInc = ${toSqlValue('importacaoAFS')};
  `;

  await prisma.$executeRawUnsafe(incluirSql);
  return String(nrFatura);
};

/**
 * GFAFATCF – vínculo fatura x carta frete
 */
const executarGFAFATCF = async (
  prisma: PrismaExecutor,
  manifest: Manifest,
  nrFatura: string,
): Promise<void> => {
  const faturamento = manifest.dadosFaturamento;
  if (!faturamento) return;

  const cdEmpresaCF = await buildCdEmpresaFromCnpj(prisma, faturamento.cdempresa);
  const cdCartaFrete = await buildCdCartaFrete(prisma, manifest, faturamento);

  const excluirSql = `
    EXEC dbo.P_INTEGRACAO_SENIOR_CP_GFAFATCF_EXCLUIR
      @CdInscricao = ${toSqlValue(padLeft(manifest.nrcgccpfprop, 14))},
      @CdCartaFrete = ${Number(nrFatura)};
  `;
  await prisma.$executeRawUnsafe(excluirSql);

  const incluirSql = `
    EXEC dbo.P_INTEGRACAO_SENIOR_CP_GFAFATCF_INCLUIR
      @CdInscricao = ${toSqlValue(padLeft(manifest.nrcgccpfprop, 14))},
      @NrFatura = ${Number(nrFatura)},
      @CdEmpresaCF = ${cdEmpresaCF},
      @CdCartaFrete = ${cdCartaFrete};
  `;
  await prisma.$executeRawUnsafe(incluirSql);
};

/**
 * GFAFATRA – rateio da fatura
 */
const executarGFAFATRA = async (
  prisma: PrismaExecutor,
  manifest: Manifest,
  nrFatura: string,
): Promise<void> => {
  const excluirSql = `
    EXEC dbo.P_INTEGRACAO_SENIOR_CP_GFAFATRA_EXCLUIR
      @CdInscricao = ${toSqlValue(padLeft(manifest.nrcgccpfprop, 14))},
      @NrFatura = ${Number(nrFatura)};
  `;
  await prisma.$executeRawUnsafe(excluirSql);

  const vlLancamento = ensureNumber(manifest.vltotalmanifesto) + ensureNumber(manifest.vlpedagio);

  const incluirSql = `
    EXEC dbo.P_INTEGRACAO_SENIOR_CP_GFAFATRA_INCLUIR
      @CdInscricao = ${toSqlValue(padLeft(manifest.nrcgccpfprop, 14))},
      @NrFatura = ${Number(nrFatura)},
      @CdPlanoConta = 21101002,
      @CdCentroCusto = 900011,
      @VlLancamento = ${ensureDecimal(vlLancamento)},
      @VlLancamentoAux = 0;
  `;
  await prisma.$executeRawUnsafe(incluirSql);
};

/**
 * Exclui títulos anteriores de uma fatura (GFATITU)
 */
const excluirGFATITU = async (
  prisma: PrismaExecutor,
  manifest: Manifest,
  nrFatura: string,
): Promise<void> => {
  const sql = `
    EXEC dbo.P_INTEGRACAO_SENIOR_CR_GFATITU_EXCLUIR
      @CdInscricao = ${toSqlValue(padLeft(manifest.nrcgccpfprop, 14))},
      @NrFatura = ${Number(nrFatura)};
  `;
  await prisma.$executeRawUnsafe(sql);
};

/**
 * Insere um título (GFATITU) + rateio (GFATITRA) para uma parcela
 */
const executarGFATITUeGFATITRA = async (
  prisma: PrismaExecutor,
  manifest: Manifest,
  parcela: ManifestParcelas,
  seqParcela: number,
  nrFatura: string,
  valoresEventos?: {
    vlEvento3: number;
    vlEvento4: number;
    vlEvento5: number;
    vlEvento8: number;
    vlEvento10: number;
    vlEvento12: number;
  },
  cenario?: number | null,
): Promise<void> => {
  const ano = currentAno2();
  const codEmpresa = await buildCdEmpresaFromCnpj(
    prisma,
    manifest.dadosFaturamento?.cdempresa || manifest.cdempresa,
  );

  let seq = seqParcela;
  if (parcela.dstipo === 'Adiantamento') seq = 1;
  if (parcela.dstipo === 'Saldo') seq = 2;
  if (parcela.dstipo === 'Pedagio') seq = 3;

  // Formatar parcela com 2 dígitos (01, 02, 03)
  const parcelaFormatada = String(seq).padStart(2, '0');

  // CdTitulo = cdEmpresa + Ano + nrmanifesto + parcela
  const nrmanifesto = manifest.nrmanifesto || '0';
  const cdTitulo = `${codEmpresa}${ano}${nrmanifesto}${parcelaFormatada}`;

  let dtVencimento: string | null = null;
  if (parcela.dstipo === 'Adiantamento') {
    dtVencimento = parcela.dtvencimento || manifest.dtemissao || null;
  } else if (parcela.dstipo === 'Saldo') {
    dtVencimento = parcela.dtpagto || manifest.dtemissao || null;
  } else if (parcela.dstipo === 'Pedagio') {
    dtVencimento = manifest.dtemissao || null;
  }

  // REGRA: vlOriginal na GFATITU = vlPrevisao na GFATITRA (ambos iguais)
  // REGRA: vlPrevisao na GFATITRA = vlRealizado (para todos, exceto evento 2 - Saldo)
  // REGRA: vlPrevisao na GFATITRA = 0 (para evento 2 - Saldo)
  let vlSaldoTitulo = 0;
  let vlOriginalTitulo = 0; // vlOriginal = vlPrevisao na GFATITRA
  let inTpTitulo = 0;
  let cdPlanoConta = 41110007;
  let cdHistorico = 22;

  if (parcela.dstipo === 'Adiantamento') {
    vlSaldoTitulo = ensureNumber(manifest.vladiantamento);
    vlOriginalTitulo = ensureNumber(manifest.vladiantamento); // vlOriginal = vlPrevisao (que será igual a vlRealizado)
    inTpTitulo = 1;
    cdPlanoConta = 11402001; // Regra geral fixa: InTpTitulo = 1 → 11402001
  } else if (parcela.dstipo === 'Saldo') {
    vlSaldoTitulo = ensureNumber(manifest.vlsaldofrete);
    // Evento 2 (Saldo): vlOriginal = vlPrevisao = vlRealizado da GFATITRA
    const vlOriginalSaldo =
      ensureNumber(manifest.vltotalmanifesto) - ensureNumber(manifest.vladiantamento);
    vlOriginalTitulo = vlOriginalSaldo; // igual ao vlRealizado da GFATITRA
    inTpTitulo = 2;
    cdPlanoConta = 21101002; // Regra geral fixa: InTpTitulo = 2 → 21101002
  } else if (parcela.dstipo === 'Pedagio') {
    vlSaldoTitulo = ensureNumber(manifest.vlpedagio);
    vlOriginalTitulo = ensureNumber(manifest.vlpedagio); // vlOriginal = vlPrevisao (que será igual a vlRealizado)
    inTpTitulo = 3;
    cdPlanoConta = 21101002; // Regra geral fixa: InTpTitulo = 3 → 21101002
    cdHistorico = 22; // Ajustado de 25 para 22 conforme padrão
  }

  if (!vlOriginalTitulo || vlOriginalTitulo <= 0) {
    // Para Saldo, pode ser que vlPrevisao seja 0, então não retornar aqui
    // Mas para outros tipos, se não houver valor, não criar título
    if (parcela.dstipo !== 'Saldo') {
      return;
    }
  }

  const incluirTituloSql = `
    EXEC dbo.P_INTEGRACAO_SENIOR_CR_GFATITU_INCLUIR
      @InPagarReceber = 0,
      @CdInscricao = ${toSqlValue(padLeft(manifest.nrcgccpfprop, 14))},
      @CdTitulo = ${toSqlValue(cdTitulo)},
      @CdFilial = ${codEmpresa},
      @CdCarteira = 1,
      @CdCentroCusto = 900011,
      @CdPortador = NULL,
      @CdSituacao = 1,
      @CdPlanoConta = ${cdPlanoConta},
      @CdEspecieDocumento = 3,
      @CdMoeda = 1,
      @NrFatura = ${Number(nrFatura)},
      @CdHistorico = ${cdHistorico},
      @DsComplemento = ${toSqlValue('')},
      @CdInstrucao = 1,
      @VlPrevisao = ${ensureDecimal(vlOriginalTitulo)},
      @VlOriginal = ${ensureDecimal(vlOriginalTitulo)},
      @VlSaldo = ${ensureDecimal(vlSaldoTitulo)},
      @InRejeitado = 0,
      @DtVencimento = ${parseDate(dtVencimento)},
      @DtEmissao = ${parseDate(manifest.dtemissao || null)},
      @DtCompetencia = ${parseDate(manifest.dtemissao || null)},
      @DtPagto = NULL,
      @InSituacao = 0,
      @DsUsuarioInc = ${toSqlValue('importacaoAFS')},
      @DtGeracao = ${parseDate(manifest.dtemissao || null)},
      @HrGeracao = ${parseDate(manifest.dtemissao || null)},
      @InTpTitulo = ${inTpTitulo};
  `;
  await prisma.$executeRawUnsafe(incluirTituloSql);

  // GFATITRA – rateio do título
  // REGRA: Para TODOS os eventos EXCETO evento 2 (Saldo):
  //   - vlPrevisao = vlRealizado (como era antes)
  //   - vlSaldo = vlPrevisao
  // REGRA: Para evento 2 (Saldo): manter regra atual
  //   - vlPrevisao = 0
  //   - vlSaldo = vlsaldofrete
  let vlRealizado = 0;
  let vlSaldoRateio = 0;
  let vlDesconto = 0;
  let vlAcrescimo = 0;
  let planoContaRateio = 21101002;
  let vlSaldoFinal = 0;
  let vlPrevisaoFinal = 0;

  if (parcela.dstipo === 'Saldo') {
    // SALDO (Evento 2, CdTitulo termina em 02) - MANTER REGRA ATUAL
    // VlPrevisao = 0 (manter regra atual para evento 2)
    // VlRealizado recebe o valor que estava em VlPrevisao
    // VlSaldo = vlsaldofrete
    // VlAcrescimo = vlnotacreditodebito quando positivo
    // VlDesconto = soma dos impostos (vlinss + vlir + vlsestsenat)
    const vlOriginalSaldo =
      ensureNumber(manifest.vltotalmanifesto) - ensureNumber(manifest.vladiantamento);
    vlRealizado = vlOriginalSaldo; // Valor que seria o vlOriginal original
    vlSaldoRateio = ensureNumber(manifest.vlsaldofrete);
    vlSaldoFinal = vlSaldoRateio; // vlSaldo para Saldo = vlsaldofrete
    vlPrevisaoFinal = 0; // Evento 2: manter regra atual (vlPrevisao = 0)
    planoContaRateio = 21101002;

    // VlAcrescimo = vlnotacreditodebito quando for positivo
    const vlNotaCreditoDebito = ensureNumber(manifest.vlnotacreditodebito);
    if (vlNotaCreditoDebito > 0) {
      vlAcrescimo = vlNotaCreditoDebito;
    }

    // VlDesconto = valor dos impostos somados (INSS + IRRF + SEST SENAT)
    const vlImpostos =
      ensureNumber(manifest.vlinss) +
      ensureNumber(manifest.vlir) +
      ensureNumber(manifest.vlsestsenat);

    // VlDesconto = impostos + combustível (quando ambos existirem)
    // Verificar se há desconto de combustível (evento 10) quando cenario = 2
    const vlDescontoCombustivel =
      valoresEventos && cenario === 2 && valoresEventos.vlEvento10 > 0
        ? valoresEventos.vlEvento10
        : 0;

    // Só somar combustível se também houver impostos (ambos devem existir)
    if (vlImpostos > 0 && vlDescontoCombustivel > 0) {
      vlDesconto = vlImpostos + vlDescontoCombustivel;
    } else {
      vlDesconto = vlImpostos;
    }
  } else if (parcela.dstipo === 'Adiantamento') {
    // ADIANTAMENTO (CdTitulo termina em 01) - REGRA: vlPrevisao = vlRealizado, vlSaldo = vlPrevisao
    vlRealizado = ensureNumber(manifest.vladiantamento);
    vlPrevisaoFinal = vlRealizado; // vlPrevisao = vlRealizado (como era antes)
    vlSaldoFinal = vlPrevisaoFinal; // vlSaldo = vlPrevisao
    planoContaRateio = 11402001;
  } else if (parcela.dstipo === 'Pedagio') {
    // PEDÁGIO (CdTitulo termina em 03) - REGRA: vlPrevisao = vlRealizado, vlSaldo = vlPrevisao
    vlRealizado = ensureNumber(manifest.vlpedagio);
    vlPrevisaoFinal = vlRealizado; // vlPrevisao = vlRealizado (como era antes)
    vlSaldoFinal = vlPrevisaoFinal; // vlSaldo = vlPrevisao
    planoContaRateio = 21101002; // Regra fixa: InTpTitulo = 3 → 21101002
  }

  const incluirRateioSql = `
    EXEC dbo.P_INTEGRACAO_SENIOR_CP_GFATITRA_INCLUIR
      @InPagarReceber = 0,
      @CdInscricao = ${toSqlValue(padLeft(manifest.nrcgccpfprop, 14))},
      @CdTitulo = ${toSqlValue(cdTitulo)},
      @CdPlanoConta = ${planoContaRateio},
      @CdCentroCusto = 900011,
      @VlRealizado = ${ensureDecimal(vlRealizado)},
      @VlPago = 0,
      @VlSaldo = ${ensureDecimal(vlSaldoFinal)},
      @VlAcrescimo = ${ensureDecimal(vlAcrescimo)},
      @VlDesconto = ${ensureDecimal(vlDesconto)},
      @VlRealizadoAux = 0,
      @VlPrevisao = ${ensureDecimal(vlPrevisaoFinal)};
  `;
  await prisma.$executeRawUnsafe(incluirRateioSql);
};

/**
 * GFAMovTi – movimentações de impostos vinculados ao título (INSS, IRRF, SEST SENAT)
 * Inclui regras específicas por cenário para eventos 10 (combustível) e 12 (desconto outros)
 */
const executarGFAMovTi = async (
  prisma: PrismaExecutor,
  manifest: Manifest,
  nrFatura: string,
  valoresEventos?: {
    vlEvento3: number;
    vlEvento4: number;
    vlEvento5: number;
    vlEvento8: number;
    vlEvento10: number;
    vlEvento12: number;
  },
  cenario?: number | null,
): Promise<void> => {
  const cdEmpresaCF = await buildCdEmpresaFromCnpj(
    prisma,
    manifest.dadosFaturamento?.cdempresaFV || manifest.cdempresa,
  );

  const ano = currentAno2();
  const codEmpresa = await buildCdEmpresaFromCnpj(
    prisma,
    manifest.dadosFaturamento?.cdempresa || manifest.cdempresa,
  );

  // cdTituloBase = cdEmpresa + Ano + nrmanifesto + parcela (02 para Saldo)
  const nrmanifesto = manifest.nrmanifesto || '0';
  const cdTituloBase = `${codEmpresa}${ano}${nrmanifesto}02`;

  let sequencia = 0;

  const execMov = async (cdTipoLancamento: number, valor: number, dsObs: string) => {
    if (!valor || valor <= 0) return;
    sequencia += 1;

    // Inserir o registro via stored procedure
    // A procedure define DtDigitacao = getdate() (já está correto)
    // Passamos @DtBaixa = NULL para que a procedure use o mesmo valor de DtDigitacao
    const sql = `
      EXEC dbo.P_CONTAS_PAGAR_CIOT_GFAMOVTI_ESL_INCLUIR
        @InPagarReceber = 0,
        @CdInscricao = ${toSqlValue(padLeft(manifest.nrcgccpfprop, 14))},
        @CdTitulo = ${toSqlValue(cdTituloBase)},
        @CdSequencia = ${sequencia},
        @CdTipoLancamento = ${cdTipoLancamento},
        @VlMovimento = ${ensureDecimal(valor)},
        @DsObservacao = ${toSqlValue(dsObs)},
        @DsUsuario = ${toSqlValue(manifest.dsusuarioinc || 'importacaoAFS')},
        @CdEmpresaCF = ${cdEmpresaCF},
        @CdCartaFrete = ${Number(nrFatura)},
        @DtBaixa = NULL;
    `;
    await prisma.$executeRawUnsafe(sql);
  };

  // Impostos padrão (INSS, SEST SENAT, IRRF) - apenas gravar quando houver
  if (ensureNumber(manifest.vlinss) > 0) {
    await execMov(
      25,
      ensureNumber(manifest.vlinss),
      `DESCONTO DE INSS Referente a CTRB / RPA: ${nrFatura}`,
    );
  }
  if (ensureNumber(manifest.vlsestsenat) > 0) {
    await execMov(
      36,
      ensureNumber(manifest.vlsestsenat),
      `DESCONTO DE SESTSENAT Referente a CTRB / RPA: ${nrFatura}`,
    );
  }
  if (ensureNumber(manifest.vlir) > 0) {
    await execMov(
      26,
      ensureNumber(manifest.vlir),
      `DESCONTO DE IRRF Referente a CTRB / RPA: ${nrFatura}`,
    );
  }

  // Regras específicas por cenário
  if (valoresEventos && cenario) {
    // CENÁRIO 2: Evento 10 (combustível) - CdTipoLancamento = 105
    // Grava os dados do título (IntpTitulo = 2 (saldo))
    if (cenario === 2 && valoresEventos.vlEvento10 > 0) {
      await execMov(
        105,
        valoresEventos.vlEvento10,
        `DESCONTO DE COMBUSTÍVEL Referente a CTRB / RPA: ${nrFatura}`,
      );
    }

    // CENÁRIO 3: Evento 12 (desconto outros) - CdTipoLancamento = 107
    // Grava os dados do título (IntpTitulo = 2 (saldo))
    if (cenario === 3 && valoresEventos.vlEvento12 > 0) {
      await execMov(
        107,
        valoresEventos.vlEvento12,
        `DESCONTO DE OUTROS EVENTOS Referente a CTRB / RPA: ${nrFatura}`,
      );
    }
  }

  // Quando vlnotacreditodebito for positivo, criar lançamento com CdTipLancamento = 171
  const vlNotaCreditoDebito = ensureNumber(manifest.vlnotacreditodebito);
  if (vlNotaCreditoDebito > 0) {
    await execMov(171, vlNotaCreditoDebito, `NOTA DE CRÉDITO Referente a CTRB / RPA: ${nrFatura}`);
  }
};

/**
 * Fluxo principal de integração de Contas a Pagar na Senior (equivalente ao InserirContasPagar do C#).
 * Retorna informações sobre quais tabelas foram inseridas com sucesso e quais falharam.
 */
const integrarContasPagarSenior = async (
  prisma: PrismaExecutor,
  manifest: Manifest,
): Promise<{
  tabelasInseridas: string[];
  tabelasFalhadas: Array<{ tabela: string; erro: string }>;
}> => {
  const tabelasInseridas: string[] = [];
  const tabelasFalhadas: Array<{ tabela: string; erro: string }> = [];

  if (!manifest.parcelas || manifest.parcelas.length === 0) {
    throw new Error('Fatura sem itens (manifesto sem parcelas vinculadas)');
  }

  if (!manifest.cdempresa || !manifest.nrmanifesto) {
    throw new Error('Manifesto sem empresa ou número de manifesto (cdempresa/nrmanifesto)');
  }

  // FTRCFT, FTRCFTFV, FTRCFTFM
  try {
    await executarFTRCFT(prisma, manifest);
    tabelasInseridas.push('FTRCFT');
  } catch (error: any) {
    tabelasFalhadas.push({ tabela: 'FTRCFT', erro: error.message || 'Erro desconhecido' });
    throw error; // FTRCFT é crítico, não continuar
  }

  try {
    await executarFTRCFTFV(prisma, manifest);
    tabelasInseridas.push('FTRCFTFV');
  } catch (error: any) {
    tabelasFalhadas.push({ tabela: 'FTRCFTFV', erro: error.message || 'Erro desconhecido' });
    // Continuar mesmo se falhar
  }

  try {
    await executarFTRCFTFM(prisma, manifest);
    tabelasInseridas.push('FTRCFTFM');
  } catch (error: any) {
    tabelasFalhadas.push({ tabela: 'FTRCFTFM', erro: error.message || 'Erro desconhecido' });
    // Continuar mesmo se falhar
  }

  // Eventos (saldo, adiantamento, pedágio, impostos, combustível, carga/descarga)
  try {
    await executarFTRCFTMV(prisma, manifest, manifest.parcelas);
    tabelasInseridas.push('FTRCFTMV');
  } catch (error: any) {
    tabelasFalhadas.push({ tabela: 'FTRCFTMV', erro: error.message || 'Erro desconhecido' });
    throw error; // FTRCFTMV é crítico, não continuar
  }

  // Fatura e vínculos
  let nrFatura: string;
  try {
    nrFatura = await executarGFAFATUR(prisma, manifest);
    tabelasInseridas.push('GFAFATUR');
  } catch (error: any) {
    tabelasFalhadas.push({ tabela: 'GFAFATUR', erro: error.message || 'Erro desconhecido' });
    throw error; // GFAFATUR é crítico, não continuar
  }

  try {
    await executarGFAFATCF(prisma, manifest, nrFatura);
    tabelasInseridas.push('GFAFATCF');
  } catch (error: any) {
    tabelasFalhadas.push({ tabela: 'GFAFATCF', erro: error.message || 'Erro desconhecido' });
    // Continuar mesmo se falhar
  }

  try {
    await executarGFAFATRA(prisma, manifest, nrFatura);
    tabelasInseridas.push('GFAFATRA');
  } catch (error: any) {
    tabelasFalhadas.push({ tabela: 'GFAFATRA', erro: error.message || 'Erro desconhecido' });
    // Continuar mesmo se falhar
  }

  // Buscar valores dos eventos criados para calcular acréscimos/descontos e identificar cenário
  const faturamento = manifest.dadosFaturamento;
  let valoresEventos:
    | {
        vlEvento3: number;
        vlEvento4: number;
        vlEvento5: number;
        vlEvento8: number;
        vlEvento10: number;
        vlEvento12: number;
      }
    | undefined;
  let cenario: number | null = null;

  if (faturamento) {
    const cdEmpresa = await buildCdEmpresaFromCnpj(prisma, faturamento.cdempresa);
    const cdCartaFrete = await buildCdCartaFrete(prisma, manifest, faturamento);

    // Buscar valores dos eventos
    valoresEventos = await buscarValoresEventos(prisma, cdEmpresa, cdCartaFrete);

    // Identificar cenário
    const temAdiantamento = manifest.parcelas?.some((p) => p.dstipo === 'Adiantamento') || false;
    const temPedagio = manifest.parcelas?.some((p) => p.dstipo === 'Pedagio') || false;
    const temSaldo = manifest.parcelas?.some((p) => p.dstipo === 'Saldo') || false;
    const temNotaCredito = valoresEventos.vlEvento8 > 0;

    cenario = identificarCenario({
      vlEvento10: valoresEventos.vlEvento10,
      vlEvento12: valoresEventos.vlEvento12,
      temAdiantamento,
      temPedagio,
      temSaldo,
      temNotaCredito,
    });

    logger.debug(
      {
        cdCartaFrete,
        cenario,
        valoresEventos,
        temAdiantamento,
        temPedagio,
        temSaldo,
        temNotaCredito,
      },
      'Cenário identificado e valores dos eventos obtidos',
    );
  }

  // Títulos por parcela
  try {
    await excluirGFATITU(prisma, manifest, nrFatura);
    let seqParcela = 0;
    for (const parcela of manifest.parcelas) {
      seqParcela += 1;
      try {
        await executarGFATITUeGFATITRA(
          prisma,
          manifest,
          parcela,
          seqParcela,
          nrFatura,
          valoresEventos,
          cenario,
        );
        tabelasInseridas.push(`GFATITU_${seqParcela}`);
      } catch (error: any) {
        tabelasFalhadas.push({
          tabela: `GFATITU_${seqParcela}`,
          erro: error.message || 'Erro desconhecido',
        });
        // Continuar com outras parcelas mesmo se uma falhar
      }
    }
  } catch (error: any) {
    tabelasFalhadas.push({ tabela: 'GFATITU', erro: error.message || 'Erro desconhecido' });
    // Continuar mesmo se falhar
  }

  // Movimentações de impostos (INSS, SEST SENAT, IRRF) e regras específicas por cenário
  try {
    await executarGFAMovTi(prisma, manifest, nrFatura, valoresEventos, cenario);
    tabelasInseridas.push('GFAMovTi');
  } catch (error: any) {
    tabelasFalhadas.push({ tabela: 'GFAMovTi', erro: error.message || 'Erro desconhecido' });
    // Continuar mesmo se falhar
  }

  return { tabelasInseridas, tabelasFalhadas };
};

export async function cancelarContasPagarCIOT(
  prisma: PrismaExecutor,
  nrciot: string,
  obscancelado: string | null | undefined,
  dsUsuarioCan: string | null | undefined,
): Promise<void> {
  const sql = `
    EXEC dbo.P_INTEGRACAO_SENIOR_CP_CANCELAMENTO_CIOT_ALTERAR
      @Nrciot = ${toSqlValue(nrciot)},
      @Obscancelado = ${toSqlValue(obscancelado)},
      @DsUsuarioCan = ${toSqlValue((dsUsuarioCan || '').substring(0, 10))};
  `;

  await prisma.$executeRawUnsafe(sql);
  logger.info({ nrciot }, 'CIOT cancelado com sucesso');
}

export async function inserirContasPagarCIOT(
  prisma: PrismaExecutor,
  data: ContasPagarCIOTPayload,
  existingManifestId?: number,
): Promise<{
  status: boolean;
  mensagem: string;
  alreadyExists?: boolean;
  seniorId?: string;
  integrationTimeMs?: number;
  tabelasInseridas?: string[];
  tabelasFalhadas?: Array<{ tabela: string; erro: string }>;
}> {
  try {
    if (data.cancelado === 1) {
      await cancelarContasPagarCIOT(
        prisma,
        data.Manifest.nrciot,
        data.Obscancelado,
        data.DsUsuarioCan,
      );
      return {
        status: true,
        mensagem: 'Registro cancelado com sucesso!',
      };
    }

    if (data.cancelado === 0 || data.cancelado === null || data.cancelado === undefined) {
      if (!data.Manifest) {
        return {
          status: false,
          mensagem: 'Manifesto não incluído, favor verificar lançamento!',
        };
      }

      // NÃO inserir o manifesto na tabela dbo.manifests - ele já foi inserido pelo backend
      // O worker apenas processa o registro existente nas tabelas da Senior
      // Usar o ID do registro que está sendo processado (vem do processManifest via existingManifestId)
      if (!existingManifestId) {
        logger.error(
          { nrciot: data.Manifest.nrciot, externalId: data.Manifest.id },
          'ID do manifesto existente não foi fornecido - não é possível processar',
        );
        return {
          status: false,
          mensagem: 'ID do manifesto existente não fornecido',
        };
      }
      if (!data.Manifest.parcelas || data.Manifest.parcelas.length === 0) {
        logger.error(
          { manifestId: existingManifestId, nrciot: data.Manifest.nrciot },
          'Manifesto sem parcelas vinculadas',
        );
        return {
          status: false,
          mensagem: 'Manifesto sem parcelas vinculadas',
        };
      }

      logger.info(
        {
          manifestId: existingManifestId,
          nrciot: data.Manifest.nrciot,
          parcelasCount: data.Manifest.parcelas.length,
        },
        'Processando manifesto CIOT nas tabelas da Senior via stored procedures',
      );

      const integrationStartTime = Date.now();
      try {
        const resultadoIntegracao = await integrarContasPagarSenior(prisma, data.Manifest);
        const integrationTimeMs = Date.now() - integrationStartTime;

        // Construir seniorId (cdCartaFrete é único por manifesto)
        const seniorId = data.Manifest.cdcartafrete
          ? `CIOT-${data.Manifest.nrciot}-${data.Manifest.cdcartafrete}`
          : `CIOT-${data.Manifest.nrciot}`;

        // processed = 1 será atualizado fora (em ciotSync/markProcessed) após sucesso
        const resultado: {
          status: boolean;
          mensagem: string;
          seniorId: string;
          integrationTimeMs: number;
          tabelasInseridas: string[];
          tabelasFalhadas?: Array<{ tabela: string; erro: string }>;
        } = {
          status: true,
          mensagem:
            resultadoIntegracao.tabelasFalhadas.length > 0
              ? `Manifesto integrado na Senior com sucesso, mas algumas tabelas falharam: ${resultadoIntegracao.tabelasFalhadas.map((t) => t.tabela).join(', ')}`
              : 'Manifesto integrado na Senior com sucesso',
          seniorId,
          integrationTimeMs,
          tabelasInseridas: resultadoIntegracao.tabelasInseridas,
        };

        if (resultadoIntegracao.tabelasFalhadas.length > 0) {
          resultado.tabelasFalhadas = resultadoIntegracao.tabelasFalhadas;
        }

        return resultado;
      } catch (integrationError: any) {
        const integrationTimeMs = Date.now() - integrationStartTime;
        const errorMessage = integrationError?.message || 'Erro desconhecido na integração';
        const errorCode = integrationError?.code || 'UNKNOWN';

        // Identificar qual stored procedure falhou baseado na mensagem de erro
        // IMPORTANTE: Verificar strings mais específicas PRIMEIRO (FTRCFTFV antes de FTRCFT)
        let failedStep = 'integrarContasPagarSenior';
        if (errorMessage.includes('FTRCFTFV')) failedStep = 'executarFTRCFTFV';
        else if (errorMessage.includes('FTRCFTFM')) failedStep = 'executarFTRCFTFM';
        else if (errorMessage.includes('FTRCFTMV')) failedStep = 'executarFTRCFTMV';
        else if (errorMessage.includes('FTRCFT')) failedStep = 'executarFTRCFT';
        else if (errorMessage.includes('GFAFATUR')) failedStep = 'executarGFAFATUR';
        else if (errorMessage.includes('GFAFATCF')) failedStep = 'executarGFAFATCF';
        else if (errorMessage.includes('GFAFATRA')) failedStep = 'executarGFAFATRA';
        else if (errorMessage.includes('GFATITU')) failedStep = 'executarGFATITU';
        else if (errorMessage.includes('GFAMovTi')) failedStep = 'executarGFAMovTi';

        logger.error(
          {
            error: integrationError,
            nrciot: data.Manifest.nrciot,
            failedStep,
            errorCode,
            integrationTimeMs,
          },
          'Erro ao integrar ContasPagarCIOT na Senior',
        );

        // Retornar mensagem mais detalhada
        const detailedMessage = errorMessage.includes('Invalid object name')
          ? `Erro de banco de dados: Tabela/objeto não encontrado (${failedStep}). Verifique se o banco ${env.SENIOR_DATABASE} está configurado corretamente.`
          : `Erro na integração Senior (${failedStep}): ${errorMessage}`;

        // Tentar identificar quais tabelas foram inseridas antes do erro
        const tabelasInseridas: string[] = [];
        const tabelasFalhadas: Array<{ tabela: string; erro: string }> = [];

        // Identificar tabelas baseado no step que falhou
        if (failedStep === 'executarFTRCFT') {
          // Nenhuma tabela foi inserida
        } else if (failedStep === 'executarFTRCFTFV') {
          tabelasInseridas.push('FTRCFT');
        } else if (failedStep === 'executarFTRCFTFM') {
          tabelasInseridas.push('FTRCFT', 'FTRCFTFV');
        } else if (failedStep === 'executarFTRCFTMV') {
          tabelasInseridas.push('FTRCFT', 'FTRCFTFV', 'FTRCFTFM');
        } else if (failedStep === 'executarGFAFATUR') {
          tabelasInseridas.push('FTRCFT', 'FTRCFTFV', 'FTRCFTFM', 'FTRCFTMV');
        } else if (failedStep === 'executarGFAFATCF') {
          tabelasInseridas.push('FTRCFT', 'FTRCFTFV', 'FTRCFTFM', 'FTRCFTMV', 'GFAFATUR');
        } else if (failedStep === 'executarGFAFATRA') {
          tabelasInseridas.push(
            'FTRCFT',
            'FTRCFTFV',
            'FTRCFTFM',
            'FTRCFTMV',
            'GFAFATUR',
            'GFAFATCF',
          );
        } else if (failedStep === 'executarGFATITU') {
          tabelasInseridas.push(
            'FTRCFT',
            'FTRCFTFV',
            'FTRCFTFM',
            'FTRCFTMV',
            'GFAFATUR',
            'GFAFATCF',
            'GFAFATRA',
          );
        } else if (failedStep === 'executarGFAMovTi') {
          tabelasInseridas.push(
            'FTRCFT',
            'FTRCFTFV',
            'FTRCFTFM',
            'FTRCFTMV',
            'GFAFATUR',
            'GFAFATCF',
            'GFAFATRA',
            'GFATITU',
          );
        }

        tabelasFalhadas.push({ tabela: failedStep, erro: errorMessage });

        const resultado: {
          status: boolean;
          mensagem: string;
          integrationTimeMs: number;
          tabelasInseridas?: string[];
          tabelasFalhadas: Array<{ tabela: string; erro: string }>;
        } = {
          status: false,
          mensagem: detailedMessage,
          integrationTimeMs,
          tabelasFalhadas: tabelasFalhadas,
        };

        if (tabelasInseridas.length > 0) {
          resultado.tabelasInseridas = tabelasInseridas;
        }

        return resultado;
      }
    }

    return {
      status: false,
      mensagem: 'Valor de cancelado inválido',
    };
  } catch (error: any) {
    logger.error({ error: error.message, stack: error.stack }, 'Erro ao inserir ContasPagarCIOT');
    return {
      status: false,
      mensagem: error?.message || 'Registro não inserido, favor verificar log!',
    };
  }
}
