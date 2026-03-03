import type { PrismaClient } from '@prisma/client';
import { logger } from '../utils/logger';
import { inserirContasPagarCIOT } from './ciotIntegration';
import { buildWorkerBypassMetadata, isPostgresSafeMode } from '../utils/integrationMode';
import type {
  Manifest,
  ManifestParcelas,
  ManifestFaturamento,
  ContasPagarCIOTPayload,
} from '../types/ciot';

const CIOT_BATCH_SIZE = Number(process.env.CIOT_WORKER_BATCH_SIZE ?? '3');
const CIOT_SOURCE_DATABASE = process.env.CIOT_SOURCE_DATABASE || 'AFS_INTEGRADOR';
const CIOT_TABLE = `[${CIOT_SOURCE_DATABASE}].[dbo].[manifests]`;

type RawRow = Record<string, any>;

const normalizeRow = (row: RawRow) => {
  const normalized: RawRow = {};
  Object.keys(row).forEach((key) => {
    normalized[key.toLowerCase()] = row[key];
  });
  return normalized;
};

const toStringValue = (value: any, fallback: string | null = null): string | null => {
  if (value === null || value === undefined) {
    return fallback;
  }
  const str = String(value).trim();
  if (!str.length) {
    return fallback;
  }
  return str;
};

const toNumberValue = (value: any): number | null => {
  if (value === null || value === undefined || value === '') {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const toSqlValue = (value: any): string => {
  if (value === null || value === undefined) return 'NULL';
  if (typeof value === 'string') {
    const escaped = value.replace(/'/g, "''");
    return `'${escaped}'`;
  }
  return `'${String(value)}'`;
};

const mapManifest = (row: RawRow): ContasPagarCIOTPayload | null => {
  const data = normalizeRow(row);
  const nrciot = toStringValue(data.nrciot);
  const cdempresa = toStringValue(data.cdempresa);
  const cdcartafrete = toStringValue(data.cdcartafrete);
  const nrcgccpfprop = toStringValue(data.nrcgccpfprop);
  const nrcgccpfmot = toStringValue(data.nrcgccpfmot);

  if (!nrciot || !cdempresa || !cdcartafrete || !nrcgccpfprop || !nrcgccpfmot) {
    return null;
  }

  const manifest: Manifest = {
    nrciot,
    cdempresa,
    cdcartafrete,
    nrcgccpfprop,
    nrcgccpfmot,
  };

  const idValue = toStringValue(data.id ?? data.manifest_id);
  if (idValue) {
    manifest.id = idValue;
  }
  if (data.external_id !== undefined) {
    manifest.external_id = data.external_id;
  }
  manifest.dtemissao = toStringValue(data.dtemissao);
  manifest.vlcarga = toNumberValue(data.vlcarga);
  manifest.qtpesocarga = toNumberValue(data.qtpesocarga);
  manifest.nrmanifesto = toStringValue(data.nrmanifesto);
  manifest.nrplaca = toStringValue(data.nrplaca);
  manifest.nrceporigem = toStringValue(data.nrceporigem);
  manifest.nrcepdestino = toStringValue(data.nrcepdestino);
  manifest.fgemitida = toStringValue(data.fgemitida);
  manifest.dtliberacaopagto = toStringValue(data.dtliberacaopagto);
  manifest.cdcentrocusto = toStringValue(data.cdcentrocusto);
  manifest.insituacao = toNumberValue(data.insituacao);
  manifest.cdcondicaovencto = toNumberValue(data.cdcondicaovencto);
  manifest.dsobservacao = toStringValue(data.dsobservacao);
  manifest.cdtipotransporte = toStringValue(data.cdtipotransporte);
  manifest.cdremetente = toStringValue(data.cdremetente);
  manifest.cddestinatario = toStringValue(data.cddestinatario);
  manifest.cdnaturezacarga = toStringValue(data.cdnaturezacarga);
  manifest.cdespeciecarga = toStringValue(data.cdespeciecarga);
  manifest.clmercadoria = toStringValue(data.clmercadoria);
  manifest.qtpeso = toNumberValue(data.qtpeso);
  manifest.cdempresaconhec = toStringValue(data.cdempresaconhec);
  manifest.nrseqcontrole = toStringValue(data.nrseqcontrole);
  manifest.nrnotafiscal = toStringValue(data.nrnotafiscal);
  manifest.cdhistorico = toStringValue(data.cdhistorico);
  manifest.dsusuarioinc = toStringValue(data.dsusuarioinc);
  manifest.dsusuariocanc = toStringValue(data.dsusuariocanc);
  manifest.dtinclusao = toStringValue(data.dtinclusao);
  manifest.dtcancelamento = toStringValue(data.dtcancelamento);
  manifest.intipoorigem = toStringValue(data.intipoorigem);
  manifest.nrplacareboque1 = toStringValue(data.nrplacareboque1);
  manifest.nrplacareboque2 = toStringValue(data.nrplacareboque2);
  manifest.nrplacareboque3 = toStringValue(data.nrplacareboque3);
  manifest.cdtarifa = toNumberValue(data.cdtarifa);
  manifest.dsusuarioacerto = toStringValue(data.dsusuarioacerto);
  manifest.dtacerto = toStringValue(data.dtacerto);
  manifest.cdinscricaocomp = toStringValue(data.cdinscricaocomp);
  manifest.nrseriecomp = toStringValue(data.nrseriecomp);
  manifest.nrcomprovante = toStringValue(data.nrcomprovante);
  manifest.vlfrete = toNumberValue(data.vlfrete);
  manifest.insestsenat = toStringValue(data.insestsenat);
  manifest.cdmotivocancelamento = toStringValue(data.cdmotivocancelamento);
  manifest.dsobscancelamento = toStringValue(data.dsobscancelamento);
  manifest.inveiculoproprio = toNumberValue(data.inveiculoproprio);
  manifest.dsusuarioimpressao = toStringValue(data.dsusuarioimpressao);
  manifest.dtimpressao = toStringValue(data.dtimpressao);
  manifest.dtprazomaxentrega = toStringValue(data.dtprazomaxentrega);
  manifest.nrseloautenticidade = toStringValue(data.nrseloautenticidade);
  manifest.hrmaxentrega = toStringValue(data.hrmaxentrega);
  manifest.cdvinculacaoiss = toStringValue(data.cdvinculacaoiss);
  manifest.dthrretornociot = toStringValue(data.dthrretornociot);
  manifest.cdciot = toStringValue(data.cdciot);
  manifest.serie = toNumberValue(data.serie);
  manifest.cdmsgretornociot = toStringValue(data.cdmsgretornociot);
  manifest.dsmsgretornociot = toStringValue(data.dsmsgretornociot);
  manifest.inenvioarquivociot = toStringValue(data.inenvioarquivociot);
  manifest.dsavisotransportador = toStringValue(data.dsavisotransportador);
  manifest.nrprotocolocancciot = toStringValue(data.nrprotocolocancciot);
  manifest.cdndot = toStringValue(data.cdndot);
  manifest.nrprotocoloautndot = toStringValue(data.nrprotocoloautndot);
  manifest.inoperacaoperiodo = toStringValue(data.inoperacaoperiodo);
  manifest.vlfreteestimado = toNumberValue(data.vlfreteestimado);
  manifest.inoperacaodistribuicao = toNumberValue(data.inoperacaodistribuicao);
  manifest.nrprotocoloenctociot = toStringValue(data.nrprotocoloenctociot);
  manifest.indotimpresso = toStringValue(data.indotimpresso);
  manifest.inveiculo = toNumberValue(data.inveiculo);
  manifest.cdrota = toStringValue(data.cdrota);
  manifest.inoperadorapagtoctrb = toStringValue(data.inoperadorapagtoctrb);
  manifest.inrespostaquesttacagreg = toNumberValue(data.inrespostaquesttacagreg);
  manifest.cdmoeda = toStringValue(data.cdmoeda);
  manifest.nrprotocolointerroociot = toStringValue(data.nrprotocolointerroociot);
  manifest.inretimposto = toStringValue(data.inretimposto);
  manifest.cdintersenior = toStringValue(data.cdintersenior);
  manifest.nrcodigooperpagtociot = toStringValue(data.nrcodigooperpagtociot);
  manifest.cdseqhcm = toStringValue(data.cdseqhcm);
  manifest.insitcalcpedagio = toStringValue(data.insitcalcpedagio);
  manifest.nrrepom = toStringValue(data.nrrepom);
  manifest.vlmanifesto = toNumberValue(data.vlmanifesto);
  manifest.vlcombustivel = toNumberValue(data.vlcombustivel);
  manifest.vlpedagio = toNumberValue(data.vlpedagio);
  manifest.vlnotacreditodebito = toNumberValue(data.vlnotacreditodebito);
  manifest.vldesconto = toNumberValue(data.vldesconto);
  manifest.vlcsll = toNumberValue(data.vlcsll);
  manifest.vlpis = toNumberValue(data.vlpis);
  manifest.vlirff = toNumberValue(data.vlirff);
  manifest.vlinss = toNumberValue(data.vlinss);
  manifest.vltotalmanifesto = toNumberValue(data.vltotalmanifesto);
  manifest.vlabastecimento = toNumberValue(data.vlabastecimento);
  manifest.vladiantamento = toNumberValue(data.vladiantamento);
  manifest.vlir = toNumberValue(data.vlir);
  manifest.vlsaldoapagar = toNumberValue(data.vlsaldoapagar);
  manifest.vlsaldofrete = toNumberValue(data.vlsaldofrete);
  manifest.vlcofins = toNumberValue(data.vlcofins);
  manifest.vlsestsenat = toNumberValue(data.vlsestsenat);
  manifest.vliss = toNumberValue(data.vliss);
  manifest.cdtributacao = toStringValue(data.cdtributacao);
  manifest.vlcsl = toNumberValue(data.vlcsl);
  manifest.status = toStringValue(data.status);
  manifest.error_message = toStringValue(data.error_message);

  return {
    cancelado: Number(data.cancelado ?? 0),
    Obscancelado: toStringValue(data.obscancelado),
    DsUsuarioCan: toStringValue(data.dsusuariocan),
    Manifest: manifest,
  };
};

const mapParcelas = (rows: RawRow[]): ManifestParcelas[] => {
  return rows.map((raw) => {
    const data = normalizeRow(raw);
    const parcela: ManifestParcelas = {};
    const idValue = toStringValue(data.id);
    if (idValue) parcela.ID = idValue;
    const idParcela = toStringValue(data.idparcela);
    if (idParcela) parcela.idparcela = idParcela;
    const nrciotSistema = toStringValue(data.nrciotsistema);
    if (nrciotSistema) parcela.nrciotsistema = nrciotSistema;
    const nrciot = toStringValue(data.nrciot);
    if (nrciot) parcela.nrciot = nrciot;
    const tipo = toStringValue(data.dstipo);
    if (tipo) parcela.dstipo = tipo;
    const status = toStringValue(data.dsstatus);
    if (status) parcela.dsstatus = status;
    const favorecido = toStringValue(data.cdfavorecido);
    if (favorecido) parcela.cdfavorecido = favorecido;
    const cartaFrete = toStringValue(data.cdcartafrete);
    if (cartaFrete) parcela.cdcartafrete = cartaFrete;
    const evento = toStringValue(data.cdevento);
    if (evento) parcela.cdevento = evento;
    parcela.dtpagto = toStringValue(data.dtpagto);
    parcela.indesconto = toStringValue(data.indesconto);
    parcela.vlbasecalculo = toNumberValue(data.vlbasecalculo);
    parcela.dtrecebimento = toStringValue(data.dtrecebimento);
    parcela.vloriginal = toNumberValue(data.vloriginal);
    parcela.dtinclusao = toStringValue(data.dtinclusao);
    parcela.hrinclusao = toStringValue(data.hrinclusao);
    parcela.dsusuarioinc = toStringValue(data.dsusuarioinc);
    parcela.dtreferenciacalculo = toStringValue(data.dtreferenciacalculo);
    parcela.dsobservacao = toStringValue(data.dsobservacao);
    parcela.vlprovisionado = toNumberValue(data.vlprovisionado);
    parcela.dtvencimento = toStringValue(data.dtvencimento);
    return parcela;
  });
};

const mapFaturamento = (row: RawRow | undefined): ManifestFaturamento | undefined => {
  if (!row) return undefined;
  const data = normalizeRow(row);
  const faturamento: ManifestFaturamento = {};
  const idValue = toStringValue(data.id);
  if (idValue) faturamento.ID = idValue;
  const cdEmpresa = toStringValue(data.cdempresa);
  if (cdEmpresa) faturamento.cdempresa = cdEmpresa;
  const cdCarta = toStringValue(data.cdcartafrete);
  if (cdCarta) faturamento.cdcartafrete = cdCarta;
  const cdEmpresaFV = toStringValue(data.cdempresafv);
  if (cdEmpresaFV) faturamento.cdempresaFV = cdEmpresaFV;
  const nroFicha = toStringValue(data.nrficha);
  if (nroFicha) faturamento.nrficha = nroFicha;
  return faturamento;
};

const fetchManifestRecord = async (prisma: PrismaClient, manifestId: number) => {
  try {
    // Primeiro, tentar buscar via stored procedure (fluxo original)
    const rows = await prisma.$queryRawUnsafe<RawRow[]>(
      `EXEC dbo.P_INTEGRACAO_SENIOR_CIOT_MANIFESTO_LISTAR @manifest_id = ${manifestId}`,
    );
    if (rows && rows.length > 0) {
      logger.debug(
        { manifestId, rowsCount: rows.length },
        'Dados obtidos via stored procedure P_INTEGRACAO_SENIOR_CIOT_MANIFESTO_LISTAR',
      );
      return rows[0];
    }

    // Se a stored procedure não retornou dados, buscar diretamente da tabela final
    logger.warn(
      { manifestId },
      'Stored procedure não retornou dados, buscando diretamente da tabela final',
    );
    const directRows = await prisma.$queryRawUnsafe<RawRow[]>(
      `SELECT * FROM dbo.manifests WHERE Id = ${manifestId}`,
    );
    if (directRows && directRows.length > 0) {
      logger.debug({ manifestId }, 'Dados obtidos diretamente da tabela final');
      return directRows[0];
    }

    logger.error(
      { manifestId },
      'Nenhum dado encontrado nem via stored procedure nem diretamente da tabela',
    );
    return null;
  } catch (error: any) {
    logger.error({ manifestId, error: error.message }, 'Erro ao buscar dados do manifesto');
    // Em caso de erro na stored procedure, tentar buscar diretamente
    try {
      const directRows = await prisma.$queryRawUnsafe<RawRow[]>(
        `SELECT * FROM dbo.manifests WHERE Id = ${manifestId}`,
      );
      if (directRows && directRows.length > 0) {
        logger.debug(
          { manifestId },
          'Dados obtidos diretamente da tabela final após erro na stored procedure',
        );
        return directRows[0];
      }
    } catch (directError: any) {
      logger.error(
        { manifestId, error: directError.message },
        'Erro ao buscar diretamente da tabela final',
      );
    }
    return null;
  }
};

const fetchParcelas = async (prisma: PrismaClient, manifestId: number) => {
  try {
    // Primeiro, tentar buscar diretamente da tabela manifest_parcelas (tabela intermediária)
    // O backend insere as parcelas nessa tabela antes do worker processar
    const directRows = await prisma.$queryRawUnsafe<RawRow[]>(
      `SELECT * FROM dbo.manifest_parcelas WHERE manifest_id = ${manifestId}`,
    );
    if (directRows && directRows.length > 0) {
      logger.debug(
        { manifestId, parcelasCount: directRows.length },
        'Parcelas obtidas diretamente da tabela manifest_parcelas',
      );
      return directRows;
    }

    // Se não encontrou na tabela intermediária, tentar via stored procedure (fallback)
    logger.warn(
      { manifestId },
      'Nenhuma parcela encontrada na tabela manifest_parcelas, tentando via stored procedure',
    );
    const spRows = await prisma.$queryRawUnsafe<RawRow[]>(
      `EXEC dbo.P_INTEGRACAO_SENIOR_CIOT_PARCELAS_LISTAR @manifest_id = ${manifestId}`,
    );
    if (spRows && spRows.length > 0) {
      logger.debug(
        { manifestId, parcelasCount: spRows.length },
        'Parcelas obtidas via stored procedure (fallback)',
      );
      return spRows;
    }

    logger.warn(
      { manifestId },
      'Nenhuma parcela encontrada nem na tabela nem via stored procedure',
    );
    return [];
  } catch (error: any) {
    logger.error({ manifestId, error: error.message }, 'Erro ao buscar parcelas');
    return [];
  }
};

const fetchFaturamento = async (prisma: PrismaClient, manifestId: number) => {
  const rows = await prisma.$queryRawUnsafe<RawRow[]>(
    `EXEC dbo.P_INTEGRACAO_SENIOR_CIOT_FATURAMENTO_LISTAR @manifest_id = ${manifestId}`,
  );
  return rows && rows.length > 0 ? rows[0] : undefined;
};

const markProcessed = async (prisma: PrismaClient, manifestId: number) => {
  // Atualizar diretamente na tabela final dbo.manifests
  // Apenas atualizar processed = 1 (campo único para controle)
  await prisma.$executeRawUnsafe(`
    UPDATE dbo.manifests
    SET processed = 1,
        updated_at = GETDATE(),
        error_message = NULL
    WHERE Id = ${manifestId}
  `);
  // Se houver tabela staging, também atualizar via stored procedure (mantendo compatibilidade)
  try {
    await prisma.$executeRawUnsafe(
      `EXEC dbo.P_INTEGRACAO_SENIOR_CP_CIOT_PROCESSADO_ALTERAR @idManifesto = ${manifestId}`,
    );
  } catch (error: any) {
    // Se a stored procedure não existir ou falhar, apenas logar (não é crítico)
    logger.debug(
      { manifestId, error: error.message },
      'Stored procedure de staging não disponível ou falhou',
    );
  }
};

const markError = async (prisma: PrismaClient, manifestId: number, message: string, code = '1') => {
  const trimmed = message.substring(0, 100);
  // Atualizar diretamente na tabela final dbo.manifests
  // Manter processed = 0 (pendente) para permitir reprocessamento
  await prisma.$executeRawUnsafe(`
    UPDATE dbo.manifests
    SET processed = 0,
        updated_at = GETDATE(),
        error_message = ${toSqlValue(trimmed)}
    WHERE Id = ${manifestId}
  `);
  // Se houver tabela staging, também atualizar via stored procedure (mantendo compatibilidade)
  try {
    await prisma.$executeRawUnsafe(`
      EXEC dbo.P_INTEGRACAO_SENIOR_CP_CIOT_PROCESSADO_COM_ERRO_ALTERAR
        @idManifesto = ${manifestId},
        @codErro = ${toSqlValue(code)},
        @msgErro = ${toSqlValue(trimmed)}
    `);
  } catch (error: any) {
    // Se a stored procedure não existir ou falhar, apenas logar (não é crítico)
    logger.debug(
      { manifestId, error: error.message },
      'Stored procedure de staging não disponível ou falhou',
    );
  }
};

/**
 * Verifica se já existe um manifesto processado com os mesmos dados únicos
 * Usa a mesma lógica de prioridade do backend: external_id primeiro, depois chaves únicas
 */
const checkIfAlreadyProcessed = async (
  prisma: PrismaClient,
  manifest: ContasPagarCIOTPayload,
): Promise<boolean> => {
  if (
    !manifest.Manifest.nrciot ||
    !manifest.Manifest.cdempresa ||
    !manifest.Manifest.cdcartafrete
  ) {
    return false;
  }

  try {
    // PRIORIDADE 1: Se external_id foi fornecido, verificar PRIMEIRO por ele (mais específico)
    if (manifest.Manifest.id) {
      const checkByExternalIdSql = `
        SELECT TOP 1 Id, external_id
        FROM dbo.manifests WITH (NOLOCK)
        WHERE external_id = ${toSqlValue(manifest.Manifest.id)}
        ORDER BY Id DESC;
      `;

      const existingByExternalId =
        await prisma.$queryRawUnsafe<Array<{ Id: number; external_id: string | null }>>(
          checkByExternalIdSql,
        );

      if (existingByExternalId && existingByExternalId.length > 0 && existingByExternalId[0]) {
        logger.info(
          {
            existingId: existingByExternalId[0].Id,
            externalId: manifest.Manifest.id,
            nrciot: manifest.Manifest.nrciot,
          },
          'Manifesto já existe na tabela manifests (encontrado por external_id), será marcado como processado',
        );
        return true;
      }
    }

    // PRIORIDADE 2: Se não encontrou por external_id, verificar por nrciot + cdempresa + cdcartafrete
    const checkByKeysSql = `
      SELECT TOP 1 Id, external_id
      FROM dbo.manifests WITH (NOLOCK)
      WHERE nrciot = ${toSqlValue(manifest.Manifest.nrciot)}
        AND cdempresa = ${toSqlValue(manifest.Manifest.cdempresa)}
        AND cdcartafrete = ${toSqlValue(manifest.Manifest.cdcartafrete)}
      ORDER BY Id DESC;
    `;

    const existingByKeys =
      await prisma.$queryRawUnsafe<Array<{ Id: number; external_id: string | null }>>(
        checkByKeysSql,
      );

    if (existingByKeys && existingByKeys.length > 0 && existingByKeys[0]) {
      logger.info(
        {
          existingId: existingByKeys[0].Id,
          existingExternalId: existingByKeys[0].external_id,
          requestedExternalId: manifest.Manifest.id,
          nrciot: manifest.Manifest.nrciot,
          cdempresa: manifest.Manifest.cdempresa,
          cdcartafrete: manifest.Manifest.cdcartafrete,
        },
        'Manifesto já existe na tabela manifests (encontrado por chaves únicas), será marcado como processado',
      );
      return true;
    }

    return false;
  } catch (error: any) {
    logger.warn(
      { error: error.message },
      'Erro ao verificar manifesto existente, continuando processamento',
    );
    return false;
  }
};

/**
 * Verifica se o registro já está processado na tabela staging
 */
const isAlreadyProcessedInStaging = async (
  prisma: PrismaClient,
  manifestId: number,
): Promise<boolean> => {
  try {
    const checkSql = `
      SELECT TOP 1 processed, status
      FROM ${CIOT_TABLE}
      WHERE id = ${manifestId};
    `;
    const result =
      await prisma.$queryRawUnsafe<Array<{ processed: number; status: string | null }>>(checkSql);
    if (result && result.length > 0 && result[0]) {
      return result[0].processed === 1 || result[0].status === 'processed';
    }
    return false;
  } catch (error: any) {
    logger.warn({ error: error.message, manifestId }, 'Erro ao verificar status na staging');
    return false;
  }
};

const processManifest = async (prisma: PrismaClient, manifestId: number) => {
  // Declarar variáveis no escopo da função para que estejam acessíveis no catch
  // Usar manifestId como base para eventId para evitar duplicatas
  // Se já existe um evento para este manifestId, reutilizar ou atualizar
  const eventIdBase = `ciot-${manifestId}`;
  const processingStartTime = Date.now();
  let webhookEvent = null;
  let payload: any = null;

  // Usar eventId fixo baseado no manifestId
  // O backend já cria eventos com este padrão, então o worker deve reutilizar
  const eventId = eventIdBase; // `ciot-${manifestId}` - ID fixo sem timestamp

  // Tentar encontrar evento existente criado pelo backend
  try {
    const existingEvent = await prisma.webhookEvent.findUnique({
      where: { id: eventId },
    });

    if (existingEvent) {
      webhookEvent = existingEvent;
      logger.debug({ manifestId, eventId }, 'Reutilizando evento WebhookEvent criado pelo backend');
    } else {
      logger.debug({ manifestId, eventId }, 'Evento não encontrado, será criado pelo worker');
    }
  } catch (error: any) {
    logger.warn({ error: error?.message, manifestId, eventId }, 'Erro ao buscar evento existente');
  }

  try {
    logger.info({ manifestId }, 'Iniciando processamento do manifesto CIOT');

    // Verificar primeiro se já está processado na tabela final
    // Considerar APENAS o campo processed: 1 = processado, 0 ou NULL = pendente
    const checkProcessedSql = `
      SELECT TOP 1 processed
      FROM dbo.manifests WITH (NOLOCK)
      WHERE Id = ${manifestId};
    `;
    const processedResult =
      await prisma.$queryRawUnsafe<Array<{ processed: number | null }>>(checkProcessedSql);
    if (processedResult && processedResult.length > 0 && processedResult[0]) {
      // Considerar APENAS processed: 1 = processado, 0 ou NULL = pendente
      const isProcessed = processedResult[0].processed === 1;
      if (isProcessed) {
        logger.info(
          { manifestId, processed: processedResult[0].processed },
          'Manifesto já processado na tabela final (processed = 1), pulando',
        );
        return;
      }
      logger.debug(
        { manifestId, processed: processedResult[0].processed },
        'Manifesto pendente (processed = 0 ou NULL), continuando processamento',
      );
    }

    // Buscar dados do manifesto usando stored procedure (fluxo original)
    logger.debug({ manifestId }, 'Buscando dados do manifesto via stored procedure');
    const manifestRow = await fetchManifestRecord(prisma, manifestId);
    if (!manifestRow) {
      logger.error(
        { manifestId },
        'Manifesto não encontrado via stored procedure P_INTEGRACAO_SENIOR_CIOT_MANIFESTO_LISTAR',
      );
      await markError(prisma, manifestId, 'Manifesto não encontrado');
      return;
    }
    logger.debug({ manifestId }, 'Dados do manifesto obtidos via stored procedure');

    payload = mapManifest(manifestRow);
    if (!payload) {
      logger.error({ manifestId }, 'Dados do manifesto incompletos após mapeamento');
      await markError(prisma, manifestId, 'Dados do manifesto incompletos');
      return;
    }
    logger.debug(
      { manifestId, nrciot: payload.Manifest.nrciot },
      'Payload do manifesto mapeado com sucesso',
    );

    // NÃO verificar se já existe na tabela manifests - o registro JÁ está lá (inserido pelo backend)
    // O worker precisa processar o registro nas tabelas da Senior, não apenas verificar se existe

    // Marcar como processando ANTES de inserir para evitar processamento paralelo
    // Usar UPDLOCK para garantir que apenas um worker processe por vez
    // IMPORTANTE: Só adquirir lock se processed = 0 (não processado)
    // Atualizar diretamente na tabela final dbo.manifests
    logger.debug({ manifestId }, 'Tentando adquirir lock para processamento');
    const lockResult = await prisma.$executeRawUnsafe(`
      UPDATE dbo.manifests WITH (UPDLOCK, ROWLOCK)
      SET updated_at = GETDATE()
      WHERE Id = ${manifestId} 
        AND (processed = 0 OR processed IS NULL);
    `);

    // Se nenhuma linha foi atualizada, significa que já está sendo processado ou já foi processado
    if (lockResult === 0) {
      logger.warn(
        { manifestId },
        'Não foi possível adquirir lock - manifesto já está sendo processado ou já foi processado, pulando',
      );
      return;
    }
    logger.debug({ manifestId, lockResult }, 'Lock adquirido com sucesso');

    logger.debug({ manifestId }, 'Buscando parcelas da tabela manifest_parcelas');
    const parcelasRows = await fetchParcelas(prisma, manifestId);
    if (!parcelasRows || parcelasRows.length === 0) {
      logger.error(
        { manifestId },
        'Manifesto sem parcelas vinculadas - nenhuma parcela encontrada na tabela manifest_parcelas',
      );
      await markError(prisma, manifestId, 'Manifesto sem parcelas vinculadas', '3');
      return;
    }
    logger.debug(
      { manifestId, parcelasCount: parcelasRows.length },
      'Parcelas obtidas da tabela manifest_parcelas',
    );

    const parcelas = mapParcelas(parcelasRows);
    payload.Manifest.parcelas = parcelas;

    const faturamentoRow = await fetchFaturamento(prisma, manifestId);
    const faturamento = mapFaturamento(faturamentoRow);
    if (faturamento) {
      payload.Manifest.dadosFaturamento = faturamento;
    }

    logger.info(
      { manifestId, nrciot: payload.Manifest.nrciot },
      'Processando CIOT nas tabelas da Senior via stored procedures',
    );

    // Criar/atualizar registro no WebhookEvent para monitoramento
    // Usar upsert para evitar duplicatas - se já existe evento para este manifestId, atualizar
    try {
      if (!webhookEvent) {
        // Se não encontrou evento existente, criar novo
        webhookEvent = await prisma.webhookEvent.upsert({
          where: { id: eventId },
          create: {
            id: eventId,
            source: `/api/CIOT/InserirContasPagarCIOT`,
            receivedAt: new Date(),
            status: 'processing',
            retryCount: 0,
            tipoIntegracao: 'Worker',
          },
          update: {
            status: 'processing',
            receivedAt: new Date(), // Atualizar timestamp ao reprocessar
            // Não atualizar tipoIntegracao se já foi criado pelo backend
          },
        });
      } else {
        // Se já existe, apenas atualizar status (não alterar tipoIntegracao que foi definido pelo backend)
        webhookEvent = await prisma.webhookEvent.update({
          where: { id: eventId },
          data: {
            status: 'processing',
            receivedAt: new Date(), // Atualizar timestamp ao reprocessar
          },
        });
      }
    } catch (error: any) {
      if (error?.code !== 'P2021' && error?.code !== 'P2003') {
        logger.warn(
          { error: error?.message, manifestId, eventId },
          'Erro ao criar/atualizar WebhookEvent para CIOT',
        );
      }
    }

    // Passar o manifestId original para evitar que seja criado um novo registro
    const result = await inserirContasPagarCIOT(prisma, payload, manifestId);
    const processingTimeMs = Date.now() - processingStartTime;

    logger.debug(
      {
        manifestId,
        status: result.status,
        mensagem: result.mensagem,
        alreadyExists: result.alreadyExists,
      },
      'Resultado da inserção CIOT',
    );
    if (result.status) {
      // Se já existia, também marcar como processado
      if (result.alreadyExists) {
        // Buscar o ID do registro existente na tabela final para atualizar processed = 1
        let findExistingIdSql = '';
        if (payload.Manifest.id) {
          findExistingIdSql = `
            SELECT TOP 1 Id
            FROM dbo.manifests WITH (NOLOCK)
            WHERE external_id = ${toSqlValue(payload.Manifest.id)}
            ORDER BY Id DESC;
          `;
        } else {
          findExistingIdSql = `
            SELECT TOP 1 Id
            FROM dbo.manifests WITH (NOLOCK)
            WHERE nrciot = ${toSqlValue(payload.Manifest.nrciot)}
              AND cdempresa = ${toSqlValue(payload.Manifest.cdempresa)}
              AND cdcartafrete = ${toSqlValue(payload.Manifest.cdcartafrete)}
            ORDER BY Id DESC;
          `;
        }
        const existingIdResult =
          await prisma.$queryRawUnsafe<Array<{ Id: number }>>(findExistingIdSql);
        if (existingIdResult && existingIdResult.length > 0 && existingIdResult[0]) {
          const existingFinalId = existingIdResult[0].Id;
          try {
            await prisma.$executeRawUnsafe(`
              UPDATE dbo.manifests
              SET processed = 1
              WHERE Id = ${existingFinalId} AND (processed IS NULL OR processed = 0);
            `);
            logger.debug(
              { manifestId: existingFinalId },
              'Campo processed = 1 setado na tabela final para registro existente',
            );
          } catch (updateError: any) {
            logger.warn(
              { manifestId: existingFinalId, error: updateError.message },
              'Erro ao setar processed = 1 na tabela final',
            );
          }
        }
        await markProcessed(prisma, manifestId);
        logger.info(
          { manifestId, nrciot: payload.Manifest.nrciot },
          'Manifesto CIOT já existia na tabela destino, marcado como processado',
        );

        // Atualizar WebhookEvent
        if (webhookEvent) {
          try {
            await prisma.webhookEvent.update({
              where: { id: eventId },
              data: {
                status: 'processed',
                processedAt: new Date(),
                processingTimeMs,
                integrationStatus: 'integrated',
                integrationTimeMs: result.integrationTimeMs || null,
                seniorId: result.seniorId || null,
                metadata: JSON.stringify({
                  manifestId,
                  nrciot: payload.Manifest.nrciot,
                  alreadyExists: true,
                }).substring(0, 2000),
              },
            });
          } catch (error: any) {
            logger.warn({ error: error?.message }, 'Erro ao atualizar WebhookEvent para CIOT');
          }
        }
      } else {
        // Registro foi inserido com sucesso, o processed = 1 já foi setado em inserirContasPagarCIOT
        await markProcessed(prisma, manifestId);
        logger.info({ manifestId }, 'Manifesto CIOT integrado com sucesso');

        // Atualizar WebhookEvent com informações detalhadas das tabelas
        if (webhookEvent) {
          try {
            const tabelasInseridas = (result as any).tabelasInseridas || [];
            const tabelasFalhadas = (result as any).tabelasFalhadas || [];
            const temFalhas = tabelasFalhadas.length > 0;

            const metadata: any = {
              manifestId,
              nrciot: payload.Manifest.nrciot,
              parcelasCount: payload.Manifest.parcelas?.length || 0,
              etapa: 'concluido',
              tabelasInseridas: tabelasInseridas,
              resumo: {
                totalTabelas: tabelasInseridas.length + tabelasFalhadas.length,
                sucesso: tabelasInseridas.length,
                falhas: tabelasFalhadas.length,
              },
            };

            let mensagemErro: string | null = null;
            if (temFalhas) {
              metadata.tabelasFalhadas = tabelasFalhadas;
              mensagemErro = `Tabelas inseridas com sucesso: ${tabelasInseridas.join(', ')}. Tabelas com erro: ${tabelasFalhadas.map((t: any) => `${t.tabela} (${t.erro})`).join(', ')}.`;
            } else if (tabelasInseridas.length > 0) {
              mensagemErro = null; // Sucesso completo
            }

            await prisma.webhookEvent.update({
              where: { id: eventId },
              data: {
                status: 'processed',
                processedAt: new Date(),
                processingTimeMs,
                integrationStatus: temFalhas ? 'partial' : 'integrated',
                integrationTimeMs: result.integrationTimeMs || null,
                seniorId: result.seniorId || null,
                errorMessage: mensagemErro,
                metadata: JSON.stringify(metadata).substring(0, 2000),
              },
            });
          } catch (error: any) {
            logger.warn({ error: error?.message }, 'Erro ao atualizar WebhookEvent para CIOT');
          }
        }
      }
    } else {
      await markError(prisma, manifestId, result.mensagem ?? 'Falha ao inserir CIOT');

      // Atualizar WebhookEvent com erro e informações das tabelas
      if (webhookEvent) {
        try {
          const tabelasInseridas = (result as any).tabelasInseridas || [];
          const tabelasFalhadas = (result as any).tabelasFalhadas || [];

          const mensagemErro = result.mensagem || 'Falha ao inserir CIOT';
          const metadata: any = {
            manifestId,
            nrciot: payload.Manifest.nrciot,
            parcelasCount: payload.Manifest.parcelas?.length || 0,
            erro: mensagemErro,
            etapa: 'falha',
          };

          if (tabelasInseridas.length > 0) {
            metadata.tabelasInseridas = tabelasInseridas;
          }

          if (tabelasFalhadas.length > 0) {
            metadata.tabelasFalhadas = tabelasFalhadas;
            const tabelasErro = tabelasFalhadas
              .map((t: any) => `${t.tabela} (${t.erro})`)
              .join(', ');
            metadata.erro =
              mensagemErro +
              (tabelasInseridas.length > 0
                ? `. Tabelas inseridas: ${tabelasInseridas.join(', ')}. Tabelas com erro: ${tabelasErro}`
                : `. Tabelas com erro: ${tabelasErro}`);
          }

          if (tabelasInseridas.length > 0 || tabelasFalhadas.length > 0) {
            metadata.resumo = {
              totalTabelas: tabelasInseridas.length + tabelasFalhadas.length,
              sucesso: tabelasInseridas.length,
              falhas: tabelasFalhadas.length,
            };
          }

          await prisma.webhookEvent.update({
            where: { id: eventId },
            data: {
              status: 'failed',
              processedAt: new Date(),
              processingTimeMs,
              integrationStatus: 'failed',
              errorMessage: metadata.erro.substring(0, 1000),
              metadata: JSON.stringify(metadata).substring(0, 2000),
            },
          });
        } catch (error: any) {
          logger.warn({ error: error?.message }, 'Erro ao atualizar WebhookEvent para CIOT');
        }
      }
    }
  } catch (error: any) {
    const message = error?.message || 'Erro desconhecido';
    const errorStack = error?.stack || '';
    logger.error({ manifestId, error, stack: errorStack }, 'Erro ao processar manifesto CIOT');
    await markError(prisma, manifestId, message);

    // Atualizar WebhookEvent com erro detalhado
    if (webhookEvent) {
      try {
        const metadata: any = {
          manifestId,
          nrciot: payload?.Manifest?.nrciot || null,
          erro: message,
          etapa: 'erro',
        };

        await prisma.webhookEvent.update({
          where: { id: eventId },
          data: {
            status: 'failed',
            processedAt: new Date(),
            processingTimeMs: Date.now() - processingStartTime,
            integrationStatus: 'failed',
            errorMessage: message.substring(0, 1000),
            metadata: JSON.stringify(metadata).substring(0, 2000),
          },
        });
      } catch (updateError: any) {
        logger.warn(
          { error: updateError?.message },
          'Erro ao atualizar WebhookEvent para CIOT no catch',
        );
      }
    }
  }
};

export async function processPendingCiot(prisma: PrismaClient) {
  if (isPostgresSafeMode()) {
    logger.info(
      buildWorkerBypassMetadata('ciotSync', { reason: 'legacy_flow_disabled' }),
      'Processamento CIOT legado desativado em modo PostgreSQL',
    );
    return;
  }

  try {
    logger.debug('Iniciando busca de manifestos CIOT pendentes');

    // Buscar registros pendentes diretamente da tabela final dbo.manifests
    // Apenas considerar processed = 0 ou NULL como pendente
    // processed = 1 = processado
    const query = `SELECT TOP (${CIOT_BATCH_SIZE}) Id
       FROM dbo.manifests WITH (UPDLOCK, READPAST)
       WHERE (processed = 0 OR processed IS NULL)
       ORDER BY Id ASC`;

    logger.debug({ query }, 'Executando query para buscar manifestos pendentes');

    const rows = await prisma.$queryRawUnsafe<Array<{ Id: number }>>(query);

    if (!rows || rows.length === 0) {
      logger.debug('Nenhum manifesto CIOT pendente na tabela final (processed = 0)');
      return;
    }

    const pendingIds = rows.map((row) => row.Id);
    logger.info(
      { count: pendingIds.length, ids: pendingIds },
      'Processando manifestos CIOT pendentes da tabela final',
    );

    for (const manifestId of pendingIds) {
      await processManifest(prisma, manifestId);
    }
  } catch (error: any) {
    logger.error(
      { error: error.message, stack: error.stack },
      'Erro ao buscar manifestos CIOT pendentes',
    );
  }
}
