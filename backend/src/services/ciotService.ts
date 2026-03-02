import { PrismaClient } from '@prisma/client';
import { logger } from '../utils/logger';
import type { ContasPagarCIOT, Manifest, ManifestParcelas, ManifestFaturamento } from '../schemas/ciot';
import { createOrUpdateWebhookEvent, generateCiotEventId } from '../utils/webhookEvent';

const prisma = new PrismaClient();

// Helper para converter valores para SQL
const toSqlValue = (value: any): string => {
  if (value === null || value === undefined) return 'NULL';
  if (typeof value === 'string') {
    // Escapar aspas simples
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

// Helper para garantir valor numérico não-nulo (usa 0 como padrão)
const toSqlNumber = (value: number | null | undefined, defaultValue: number = 0): string => {
  if (value === null || value === undefined) return String(defaultValue);
  return String(value);
};

// Helper para converter valores para TIME (HH:mm:ss)
const toSqlTime = (value: string | number | null | undefined, defaultValue = '00:00:00'): string => {
  if (value === null || value === undefined) {
    return `'${defaultValue}'`;
  }

  if (typeof value === 'number') {
    const hours = Math.max(0, Math.min(23, Math.floor(value)));
    return `'${hours.toString().padStart(2, '0')}:00:00'`;
  }

  const str = String(value).trim();
  if (str.length === 0 || str === '0') {
    return `'${defaultValue}'`;
  }

  const timeRegex = /^(\d{1,2})(?::(\d{1,2}))?(?::(\d{1,2}))?$/;
  const match = str.match(timeRegex);
  if (match) {
    const hours = match[1]?.padStart(2, '0') ?? '00';
    const minutes = match[2]?.padStart(2, '0') ?? '00';
    const seconds = match[3]?.padStart(2, '0') ?? '00';
    return `'${hours}:${minutes}:${seconds}'`;
  }

  return `'${defaultValue}'`;
};

// Helper para tentar localizar manifesto já inserido quando stored procedure não retorna o ID
const findManifestIdByUniqueKeys = async (manifest: Manifest): Promise<number> => {
  if (!manifest.nrciot) {
    return 0;
  }

  const whereClauses = [`nrciot = ${toSqlValue(manifest.nrciot)}`];

  if (manifest.cdempresa) {
    whereClauses.push(`cdempresa = ${toSqlValue(manifest.cdempresa)}`);
  }
  if (manifest.cdcartafrete) {
    whereClauses.push(`cdcartafrete = ${toSqlValue(manifest.cdcartafrete)}`);
  }
  if (manifest.id) {
    whereClauses.push(`external_id = ${toSqlValue(manifest.id)}`);
  }

  const fallbackSql = `
    SELECT TOP 1 Id
    FROM dbo.manifests WITH (NOLOCK)
    WHERE ${whereClauses.join(' AND ')}
    ORDER BY Id DESC;
  `;

  const fallbackResult = await prisma.$queryRawUnsafe<Array<{ Id: number }>>(fallbackSql);
  const fallbackId =
    fallbackResult && fallbackResult[0]?.Id ? Number(fallbackResult[0].Id) : 0;

  if (fallbackId > 0) {
    logger.warn(
      {
        fallbackId,
        nrciot: manifest.nrciot,
        cdempresa: manifest.cdempresa,
        cdcartafrete: manifest.cdcartafrete,
      },
      'Stored procedure retornou 0, mas manifesto existente foi localizado via fallback'
    );
  }

  return fallbackId;
};

// Helper para converter string de data para formato SQL
const parseDate = (dateStr: string | null | undefined, defaultValue?: string): string => {
  if (!dateStr) {
    // Se defaultValue for fornecido, usar ele; caso contrário, retornar NULL
    if (defaultValue) {
      return `'${defaultValue}'`;
    }
    return 'NULL';
  }
  try {
    const date = new Date(dateStr);
    if (isNaN(date.getTime())) {
      if (defaultValue) {
        return `'${defaultValue}'`;
      }
      return 'NULL';
    }
    return `'${date.toISOString().split('T')[0]}'`;
  } catch {
    if (defaultValue) {
      return `'${defaultValue}'`;
    }
    return 'NULL';
  }
};

/**
 * Atualiza Manifesto na tabela quando já existe
 */
export async function atualizarManifesto(manifest: Manifest, manifestId: number): Promise<void> {
  try {
    const cdespeciecarga = manifest.cdespeciecarga || '0';
    const cdrota = manifest.cdrota && manifest.cdrota.trim() !== '' ? manifest.cdrota : '01';

    const updateSql = `
      UPDATE dbo.manifests SET
        external_id = ${toSqlValue(manifest.id)},
        nrcgccpfprop = ${toSqlValue(manifest.nrcgccpfprop)},
        nrcgccpfmot = ${toSqlValue(manifest.nrcgccpfmot)},
        dtemissao = ${parseDate(manifest.dtemissao)},
        vlcarga = ${manifest.vlcarga ?? 'NULL'},
        qtpesocarga = ${manifest.qtpesocarga ?? 'NULL'},
        nrmanifesto = ${toSqlValue(manifest.nrmanifesto)},
        nrplaca = ${toSqlValue(manifest.nrplaca)},
        nrceporigem = ${toSqlValue(manifest.nrceporigem)},
        nrcepdestino = ${toSqlValue(manifest.nrcepdestino)},
        fgemitida = ${toSqlValue(manifest.fgemitida)},
        dtliberacaopagto = ${parseDate(manifest.dtliberacaopagto, new Date().toISOString().split('T')[0])},
        cdcentrocusto = ${toSqlValue(manifest.cdcentrocusto)},
        insituacao = ${manifest.insituacao ?? 'NULL'},
        cdcondicaovencto = ${manifest.cdcondicaovencto ?? 'NULL'},
        dsobservacao = ${toSqlValue(manifest.dsobservacao)},
        cdtipotransporte = ${toSqlValue(manifest.cdtipotransporte)},
        cdremetente = ${toSqlValue(manifest.cdremetente)},
        cddestinatario = ${toSqlValue(manifest.cddestinatario)},
        cdnaturezacarga = ${toSqlValue(manifest.cdnaturezacarga)},
        cdespeciecarga = ${toSqlValue(cdespeciecarga)},
        clmercadoria = ${toSqlValue(manifest.clmercadoria)},
        qtpeso = ${manifest.qtpeso ?? 'NULL'},
        cdempresaconhec = ${toSqlValue(manifest.cdempresaconhec)},
        nrseqcontrole = ${toSqlValue(manifest.nrseqcontrole)},
        nrnotafiscal = ${toSqlValue(manifest.nrnotafiscal)},
        cdhistorico = ${toSqlValue(manifest.cdhistorico)},
        dsusuarioinc = ${toSqlValue((manifest.dsusuarioinc || '').substring(0, 10))},
        dsusuariocanc = ${toSqlValue((manifest.dsusuariocanc || '').substring(0, 10))},
        dtinclusao = ${parseDate(manifest.dtinclusao)},
        dtcancelamento = ${parseDate(manifest.dtcancelamento)},
        intipoorigem = ${toSqlValue(manifest.intipoorigem)},
        nrplacareboque1 = ${toSqlValue(manifest.nrplacareboque1)},
        nrplacareboque2 = ${toSqlValue(manifest.nrplacareboque2)},
        nrplacareboque3 = ${toSqlValue(manifest.nrplacareboque3)},
        cdtarifa = ${manifest.cdtarifa ?? 'NULL'},
        dsusuarioacerto = ${toSqlValue((manifest.dsusuarioacerto || '').substring(0, 10))},
        dtacerto = ${parseDate(manifest.dtacerto)},
        cdinscricaocomp = ${toSqlValue(manifest.cdinscricaocomp)},
        nrseriecomp = ${toSqlValue(manifest.nrseriecomp)},
        nrcomprovante = ${toSqlValue(manifest.nrcomprovante)},
        vlfrete = ${manifest.vlfrete ?? 'NULL'},
        insestsenat = ${toSqlValue(manifest.insestsenat)},
        cdmotivocancelamento = ${toSqlValue(manifest.cdmotivocancelamento)},
        dsobscancelamento = ${toSqlValue(manifest.dsobscancelamento)},
        inveiculoproprio = ${manifest.inveiculoproprio ?? 'NULL'},
        dsusuarioimpressao = ${toSqlValue((manifest.dsusuarioimpressao || '').substring(0, 10))},
        dtimpressao = ${parseDate(manifest.dtimpressao)},
        dtprazomaxentrega = ${parseDate(manifest.dtprazomaxentrega)},
        nrseloautenticidade = ${toSqlValue(manifest.nrseloautenticidade)},
        hrmaxentrega = ${toSqlTime(manifest.hrmaxentrega)},
        cdvinculacaoiss = ${toSqlValue(manifest.cdvinculacaoiss)},
        dthrretornociot = ${parseDate(manifest.dthrretornociot)},
        cdciot = ${toSqlValue(manifest.cdciot)},
        serie = ${manifest.serie ?? 'NULL'},
        cdmsgretornociot = ${toSqlValue(manifest.cdmsgretornociot)},
        dsmsgretornociot = ${toSqlValue(manifest.dsmsgretornociot)},
        inenvioarquivociot = ${toSqlValue(manifest.inenvioarquivociot)},
        dsavisotransportador = ${toSqlValue(manifest.dsavisotransportador)},
        nrprotocolocancciot = ${toSqlValue(manifest.nrprotocolocancciot)},
        cdndot = ${toSqlValue(manifest.cdndot)},
        nrprotocoloautndot = ${toSqlValue(manifest.nrprotocoloautndot)},
        inoperacaoperiodo = ${toSqlValue(manifest.inoperacaoperiodo)},
        vlfreteestimado = ${manifest.vlfreteestimado ?? 'NULL'},
        inoperacaodistribuicao = ${toSqlNumber(manifest.inoperacaodistribuicao, 0)},
        nrprotocoloenctociot = ${toSqlValue(manifest.nrprotocoloenctociot)},
        indotimpresso = ${toSqlValue(manifest.indotimpresso)},
        inveiculo = ${toSqlNumber(manifest.inveiculo, 0)},
        cdrota = ${toSqlValue(cdrota)},
        inoperadorapagtoctrb = ${toSqlValue(manifest.inoperadorapagtoctrb)},
        inrespostaquesttacagreg = ${manifest.inrespostaquesttacagreg ?? 'NULL'},
        cdmoeda = ${toSqlValue(manifest.cdmoeda)},
        nrprotocolointerroociot = ${toSqlValue(manifest.nrprotocolointerroociot)},
        inretimposto = ${toSqlValue(manifest.inretimposto)},
        cdintersenior = ${toSqlValue(manifest.cdintersenior)},
        nrcodigooperpagtociot = ${toSqlValue(manifest.nrcodigooperpagtociot)},
        cdseqhcm = ${toSqlValue(manifest.cdseqhcm)},
        insitcalcpedagio = ${toSqlValue(manifest.insitcalcpedagio)},
        nrrepom = ${toSqlValue(manifest.nrrepom)},
        vlmanifesto = ${manifest.vlmanifesto ?? 'NULL'},
        vlcombustivel = ${manifest.vlcombustivel ?? 'NULL'},
        vlpedagio = ${manifest.vlpedagio ?? 'NULL'},
        vlnotacreditodebito = ${manifest.vlnotacreditodebito ?? 'NULL'},
        vldesconto = ${manifest.vldesconto ?? 'NULL'},
        vlcsll = ${manifest.vlcsll ?? 'NULL'},
        vlpis = ${manifest.vlpis ?? 'NULL'},
        vlirff = ${manifest.vlirff ?? 'NULL'},
        vlinss = ${manifest.vlinss ?? 'NULL'},
        vltotalmanifesto = ${manifest.vltotalmanifesto ?? 'NULL'},
        vlabastecimento = ${manifest.vlabastecimento ?? 'NULL'},
        vladiantamento = ${manifest.vladiantamento ?? 'NULL'},
        vlir = ${manifest.vlir ?? 'NULL'},
        vlsaldoapagar = ${manifest.vlsaldoapagar ?? 'NULL'},
        vlsaldofrete = ${manifest.vlsaldofrete ?? 'NULL'},
        vlcofins = ${manifest.vlcofins ?? 'NULL'},
        vlsestsenat = ${manifest.vlsestsenat ?? 'NULL'},
        vliss = ${manifest.vliss ?? 'NULL'},
        cdtributacao = ${toSqlValue(manifest.cdtributacao)},
        vlcsl = ${manifest.vlcsl ?? 'NULL'},
        status = ${toSqlValue(manifest.status)},
        error_message = ${toSqlValue(manifest.error_message)},
        processed = 0,
        updated_at = GETDATE()
      WHERE Id = ${manifestId};
    `;

    logger.debug({ manifestId, sql: updateSql.substring(0, 500) }, 'Executando UPDATE no manifesto');

    await prisma.$executeRawUnsafe(updateSql);

    logger.info({ manifestId }, 'Manifesto atualizado com sucesso');
  } catch (error: any) {
    logger.error({ error: error.message, manifestId }, 'Erro ao atualizar manifesto');
    throw error;
  }
}

/**
 * Insere Manifesto na tabela usando stored procedure
 * Retorna o ID do manifesto inserido e se foi uma inserção nova
 */
export async function inserirManifesto(manifest: Manifest): Promise<{ id: number; wasNewInsert: boolean }> {
  try {
    // Verificar ID máximo ANTES de inserir para detectar se foi inserção nova
    const maxIdBeforeSql = `
      SELECT ISNULL(MAX(Id), 0) AS MaxId
      FROM dbo.manifests WITH (NOLOCK);
    `;
    const maxIdBeforeResult = await prisma.$queryRawUnsafe<Array<{ MaxId: number }>>(maxIdBeforeSql);
    const maxIdBefore = maxIdBeforeResult && maxIdBeforeResult[0] ? maxIdBeforeResult[0].MaxId : 0;

    // Se cdespeciecarga estiver vazio, definir como "0"
    const cdespeciecarga = manifest.cdespeciecarga || '0';
    // Se cdrota não vier informado, usar fallback "01" (mesmo padrão do serviço .NET)
    const cdrota = manifest.cdrota && manifest.cdrota.trim() !== '' ? manifest.cdrota : '01';

    // Usar Prisma para executar stored procedure com parâmetros
    // SQL Server aceita EXEC com parâmetros nomeados
    const sql = `
      DECLARE @ReturnValue INT;
      EXEC @ReturnValue = dbo.P_CONTAS_PAGAR_CIOT_MANIFESTO_ESL_INCLUIR
        @external_id = ${toSqlValue(manifest.id)},
        @nrciot = ${toSqlValue(manifest.nrciot)},
        @cdempresa = ${toSqlValue(manifest.cdempresa)},
        @cdcartafrete = ${toSqlValue(manifest.cdcartafrete)},
        @nrcgccpfprop = ${toSqlValue(manifest.nrcgccpfprop)},
        @nrcgccpfmot = ${toSqlValue(manifest.nrcgccpfmot)},
        @dtemissao = ${parseDate(manifest.dtemissao)},
        @vlcarga = ${manifest.vlcarga ?? 'NULL'},
        @qtpesocarga = ${manifest.qtpesocarga ?? 'NULL'},
        @nrmanifesto = ${toSqlValue(manifest.nrmanifesto)},
        @nrplaca = ${toSqlValue(manifest.nrplaca)},
        @nrceporigem = ${toSqlValue(manifest.nrceporigem)},
        @nrcepdestino = ${toSqlValue(manifest.nrcepdestino)},
        @fgemitida = ${toSqlValue(manifest.fgemitida)},
        @dtliberacaopagto = ${parseDate(manifest.dtliberacaopagto, new Date().toISOString().split('T')[0])},
        @cdcentrocusto = ${toSqlValue(manifest.cdcentrocusto)},
        @insituacao = ${manifest.insituacao ?? 'NULL'},
        @cdcondicaovencto = ${manifest.cdcondicaovencto ?? 'NULL'},
        @dsobservacao = ${toSqlValue(manifest.dsobservacao)},
        @cdtipotransporte = ${toSqlValue(manifest.cdtipotransporte)},
        @cdremetente = ${toSqlValue(manifest.cdremetente)},
        @cddestinatario = ${toSqlValue(manifest.cddestinatario)},
        @cdnaturezacarga = ${toSqlValue(manifest.cdnaturezacarga)},
        @cdespeciecarga = ${toSqlValue(cdespeciecarga)},
        @clmercadoria = ${toSqlValue(manifest.clmercadoria)},
        @qtpeso = ${manifest.qtpeso ?? 'NULL'},
        @cdempresaconhec = ${toSqlValue(manifest.cdempresaconhec)},
        @nrseqcontrole = ${toSqlValue(manifest.nrseqcontrole)},
        @nrnotafiscal = ${toSqlValue(manifest.nrnotafiscal)},
        @cdhistorico = ${toSqlValue(manifest.cdhistorico)},
        @dsusuarioinc = ${toSqlValue((manifest.dsusuarioinc || '').substring(0, 10))},
        @dsusuariocanc = ${toSqlValue((manifest.dsusuariocanc || '').substring(0, 10))},
        @dtinclusao = ${parseDate(manifest.dtinclusao)},
        @dtcancelamento = ${parseDate(manifest.dtcancelamento)},
        @intipoorigem = ${toSqlValue(manifest.intipoorigem)},
        @nrplacareboque1 = ${toSqlValue(manifest.nrplacareboque1)},
        @nrplacareboque2 = ${toSqlValue(manifest.nrplacareboque2)},
        @nrplacareboque3 = ${toSqlValue(manifest.nrplacareboque3)},
        @cdtarifa = ${manifest.cdtarifa ?? 'NULL'},
        @dsusuarioacerto = ${toSqlValue((manifest.dsusuarioacerto || '').substring(0, 10))},
        @dtacerto = ${parseDate(manifest.dtacerto)},
        @cdinscricaocomp = ${toSqlValue(manifest.cdinscricaocomp)},
        @nrseriecomp = ${toSqlValue(manifest.nrseriecomp)},
        @nrcomprovante = ${toSqlValue(manifest.nrcomprovante)},
        @vlfrete = ${manifest.vlfrete ?? 'NULL'},
        @insestsenat = ${toSqlValue(manifest.insestsenat)},
        @cdmotivocancelamento = ${toSqlValue(manifest.cdmotivocancelamento)},
        @dsobscancelamento = ${toSqlValue(manifest.dsobscancelamento)},
        @inveiculoproprio = ${manifest.inveiculoproprio ?? 'NULL'},
        @dsusuarioimpressao = ${toSqlValue((manifest.dsusuarioimpressao || '').substring(0, 10))},
        @dtimpressao = ${parseDate(manifest.dtimpressao)},
        @dtprazomaxentrega = ${parseDate(manifest.dtprazomaxentrega)},
        @nrseloautenticidade = ${toSqlValue(manifest.nrseloautenticidade)},
        @hrmaxentrega = ${toSqlTime(manifest.hrmaxentrega)},
        @cdvinculacaoiss = ${toSqlValue(manifest.cdvinculacaoiss)},
        @dthrretornociot = ${parseDate(manifest.dthrretornociot)},
        @cdciot = ${toSqlValue(manifest.cdciot)},
        @serie = ${manifest.serie ?? 'NULL'},
        @cdmsgretornociot = ${toSqlValue(manifest.cdmsgretornociot)},
        @dsmsgretornociot = ${toSqlValue(manifest.dsmsgretornociot)},
        @inenvioarquivociot = ${toSqlValue(manifest.inenvioarquivociot)},
        @dsavisotransportador = ${toSqlValue(manifest.dsavisotransportador)},
        @nrprotocolocancciot = ${toSqlValue(manifest.nrprotocolocancciot)},
        @cdndot = ${toSqlValue(manifest.cdndot)},
        @nrprotocoloautndot = ${toSqlValue(manifest.nrprotocoloautndot)},
        @inoperacaoperiodo = ${toSqlValue(manifest.inoperacaoperiodo)},
        @vlfreteestimado = ${manifest.vlfreteestimado ?? 'NULL'},
        @inoperacaodistribuicao = ${toSqlNumber(manifest.inoperacaodistribuicao, 0)},
        @nrprotocoloenctociot = ${toSqlValue(manifest.nrprotocoloenctociot)},
        @indotimpresso = ${toSqlValue(manifest.indotimpresso)},
        @inveiculo = ${toSqlNumber(manifest.inveiculo, 0)},
        @cdrota = ${toSqlValue(cdrota)},
        @inoperadorapagtoctrb = ${toSqlValue(manifest.inoperadorapagtoctrb)},
        @inrespostaquesttacagreg = ${manifest.inrespostaquesttacagreg ?? 'NULL'},
        @cdmoeda = ${toSqlValue(manifest.cdmoeda)},
        @nrprotocolointerroociot = ${toSqlValue(manifest.nrprotocolointerroociot)},
        @inretimposto = ${toSqlValue(manifest.inretimposto)},
        @cdintersenior = ${toSqlValue(manifest.cdintersenior)},
        @nrcodigooperpagtociot = ${toSqlValue(manifest.nrcodigooperpagtociot)},
        @cdseqhcm = ${toSqlValue(manifest.cdseqhcm)},
        @insitcalcpedagio = ${toSqlValue(manifest.insitcalcpedagio)},
        @nrrepom = ${toSqlValue(manifest.nrrepom)},
        @vlmanifesto = ${manifest.vlmanifesto ?? 'NULL'},
        @vlcombustivel = ${manifest.vlcombustivel ?? 'NULL'},
        @vlpedagio = ${manifest.vlpedagio ?? 'NULL'},
        @vlnotacreditodebito = ${manifest.vlnotacreditodebito ?? 'NULL'},
        @vldesconto = ${manifest.vldesconto ?? 'NULL'},
        @vlcsll = ${manifest.vlcsll ?? 'NULL'},
        @vlpis = ${manifest.vlpis ?? 'NULL'},
        @vlirff = ${manifest.vlirff ?? 'NULL'},
        @vlinss = ${manifest.vlinss ?? 'NULL'},
        @vltotalmanifesto = ${manifest.vltotalmanifesto ?? 'NULL'},
        @vlabastecimento = ${manifest.vlabastecimento ?? 'NULL'},
        @vladiantamento = ${manifest.vladiantamento ?? 'NULL'},
        @vlir = ${manifest.vlir ?? 'NULL'},
        @vlsaldoapagar = ${manifest.vlsaldoapagar ?? 'NULL'},
        @vlsaldofrete = ${manifest.vlsaldofrete ?? 'NULL'},
        @vlcofins = ${manifest.vlcofins ?? 'NULL'},
        @vlsestsenat = ${manifest.vlsestsenat ?? 'NULL'},
        @vliss = ${manifest.vliss ?? 'NULL'},
        @cdtributacao = ${toSqlValue(manifest.cdtributacao)},
        @vlcsl = ${manifest.vlcsl ?? 'NULL'},
        @status = ${toSqlValue(manifest.status)},
        @error_message = ${toSqlValue(manifest.error_message)};
      SELECT @ReturnValue AS manifest_id;
    `;

    logger.debug({ sql: sql.substring(0, 500) }, 'Executando stored procedure para inserir manifesto');

    const result = await prisma.$queryRawUnsafe<Array<{ manifest_id: number }>>(sql);

    // A stored procedure retorna o ID do manifesto inserido
    const id = result && result[0]?.manifest_id ? Number(result[0].manifest_id) : 0;

    // Se a stored procedure retornou 0, tentar encontrar via fallback
    if (id === 0) {
      const fallbackId = await findManifestIdByUniqueKeys(manifest);
      if (fallbackId === 0) {
        logger.error(
          {
            nrciot: manifest.nrciot,
            cdempresa: manifest.cdempresa,
            cdcartafrete: manifest.cdcartafrete,
            result,
          },
          'Stored procedure retornou 0 e nenhum manifesto existente foi encontrado'
        );
        throw new Error('Falha ao inserir manifesto: retorno inválido da stored procedure');
      }
      // Se encontrou via fallback, verificar se foi criado recentemente
      // Se o ID encontrado é maior que maxIdBefore, foi inserção nova (a stored procedure pode ter criado mas retornado 0)
      const wasNewInsertViaFallback = fallbackId > maxIdBefore;
      if (wasNewInsertViaFallback) {
        logger.info(
          { manifestId: fallbackId, maxIdBefore },
          'Stored procedure retornou 0, mas registro foi criado recentemente (ID > maxIdBefore), considerando como inserção nova'
        );
      } else {
        logger.warn(
          { manifestId: fallbackId, maxIdBefore },
          'Stored procedure retornou 0 e registro já existia (ID <= maxIdBefore)'
        );
      }
      return { id: fallbackId, wasNewInsert: wasNewInsertViaFallback };
    }

    // Se a stored procedure retornou um ID > 0, assumir que foi inserido
    // A única forma de ter certeza de que já existia é quando retorna 0 e encontramos via fallback com ID <= maxIdBefore
    // Se retornou um ID > 0, mesmo que não seja maior que maxIdBefore, pode ser uma inserção nova
    // (pode haver condições de corrida ou a stored procedure pode ter comportamento específico)
    const wasNewInsert = id > 0;

    if (wasNewInsert) {
      // FORÇAR processed = 0 para registros inseridos pelo backend (sem condição)
      // A stored procedure pode estar setando processed = 1 por padrão
      // O worker processará depois e mudará para processed = 1
      try {
        await prisma.$executeRawUnsafe(`
          UPDATE dbo.manifests
          SET processed = 0
          WHERE Id = ${id};
        `);
        logger.debug({ manifestId: id }, 'Campo processed = 0 FORÇADO para registro inserido pelo backend (aguardando processamento pelo worker)');
      } catch (updateError: any) {
        // Não falhar se o campo processed não existir ou houver erro
        logger.warn({ manifestId: id, error: updateError.message }, 'Erro ao setar processed = 0 (pode ser campo inexistente)');
      }
      logger.info({ manifestId: id, maxIdBefore }, 'Manifesto inserido com sucesso (processed = 0, aguardando worker)');
    }

    return { id, wasNewInsert };
  } catch (error: any) {
    logger.error({ error: error.message, stack: error.stack }, 'Erro ao inserir manifesto');
    throw error;
  }
}

/**
 * Insere Parcelas do Manifesto
 */
/**
 * Valida se uma parcela tem idparcela válido (não vazio e não nulo)
 */
function isValidParcela(parcela: ManifestParcelas): boolean {
  if (!parcela) return false;
  const idparcela = parcela.idparcela;
  // Verificar se idparcela existe e não é vazio
  if (!idparcela) return false;
  if (typeof idparcela === 'string' && idparcela.trim() === '') return false;
  return true;
}

export async function inserirManifestoParcelas(parcela: ManifestParcelas): Promise<void> {
  // Validar se a parcela tem idparcela válido antes de tentar inserir
  if (!isValidParcela(parcela)) {
    throw new Error(`Parcela inválida: idparcela está vazio ou nulo. Parcela ID: ${parcela.ID || 'N/A'}`);
  }

  try {
    // Tratar strings vazias como NULL para idparcela e dsstatus
    const idparcela = parcela.idparcela && typeof parcela.idparcela === 'string' && parcela.idparcela.trim() !== '' ? parcela.idparcela : null;
    const dsstatus = parcela.dsstatus && typeof parcela.dsstatus === 'string' && parcela.dsstatus.trim() !== '' ? parcela.dsstatus : null;
    
    const sql = `
      EXEC dbo.P_CONTAS_PAGAR_CIOT_MANIFESTO_PARCELAS_ESL_INCLUIR
        @manifest_id = ${parcela.manifest_id ?? 'NULL'},
        @external_id = ${toSqlValue(parcela.ID)},
        @idparcela = ${idparcela ? toSqlValue(idparcela) : 'NULL'},
        @nrciotsistema = ${toSqlValue(parcela.nrciotsistema)},
        @nrciot = ${toSqlValue(parcela.nrciot)},
        @dstipo = ${toSqlValue(parcela.dstipo)},
        @dsstatus = ${dsstatus ? toSqlValue(dsstatus) : 'NULL'},
        @cdfavorecido = ${toSqlValue(parcela.cdfavorecido)},
        @cdcartafrete = ${toSqlValue(parcela.cdcartafrete)},
        @cdevento = ${toSqlValue(parcela.cdevento)},
        @dtpagto = ${parseDate(parcela.dtpagto)},
        @indesconto = ${toSqlValue(parcela.indesconto)},
        @vlbasecalculo = ${parcela.vlbasecalculo ?? 'NULL'},
        @dtrecebimento = ${parseDate(parcela.dtrecebimento)},
        @vloriginal = ${parcela.vloriginal ?? 'NULL'},
        @dtinclusao = ${parseDate(parcela.dtinclusao)},
        @hrinclusao = ${toSqlValue(parcela.hrinclusao)},
        @dsusuarioinc = ${toSqlValue(parcela.dsusuarioinc)},
        @dtreferenciacalculo = ${parseDate(parcela.dtreferenciacalculo)},
        @dsobservacao = ${toSqlValue(parcela.dsobservacao)},
        @vlprovisionado = ${parcela.vlprovisionado ?? 'NULL'},
        @dtvencimento = ${parseDate(parcela.dtvencimento)};
    `;

    logger.debug({ 
      manifestId: parcela.manifest_id, 
      parcelaId: parcela.ID, 
      idparcela: idparcela,
      dsstatus: dsstatus,
      sql: sql.substring(0, 500) 
    }, 'Executando stored procedure para inserir parcela');

    await prisma.$executeRawUnsafe(sql);

    logger.info({ 
      manifestId: parcela.manifest_id,
      parcelaId: parcela.ID, 
      idparcela: idparcela,
      dsstatus: dsstatus
    }, 'Parcela inserida com sucesso na tabela manifest_parcelas');
  } catch (error: any) {
    logger.error({ 
      error: error.message, 
      stack: error.stack,
      manifestId: parcela.manifest_id,
      parcelaId: parcela.ID, 
      idparcela: parcela.idparcela 
    }, 'Erro ao inserir parcela na tabela manifest_parcelas');
    throw error;
  }
}

/**
 * Insere Faturamento do Manifesto
 */
export async function inserirManifestoFaturamento(faturamento: ManifestFaturamento): Promise<void> {
  try {
    const sql = `
      EXEC dbo.P_CONTAS_PAGAR_CIOT_MANIFESTO_FATURAMNETO_ESL_INCLUIR
        @manifest_id = ${faturamento.manifest_id ?? 'NULL'},
        @external_id = ${toSqlValue(faturamento.ID)},
        @cdempresa = ${toSqlValue(faturamento.cdempresa)},
        @cdcartafrete = ${toSqlValue(faturamento.cdcartafrete)},
        @cdempresaFV = ${toSqlValue(faturamento.cdempresaFV)},
        @nrficha = ${toSqlValue(faturamento.nrficha)};
    `;

    logger.debug({ sql: sql.substring(0, 500) }, 'Executando stored procedure para inserir faturamento');

    await prisma.$executeRawUnsafe(sql);

    logger.debug({ faturamentoId: faturamento.ID }, 'Faturamento inserido com sucesso');
  } catch (error: any) {
    logger.error({ error: error.message, faturamentoId: faturamento.ID }, 'Erro ao inserir faturamento');
    throw error;
  }
}

/**
 * Cancela ContasPagarCIOT
 */
export async function cancelarContasPagarCIOT(
  nrciot: string,
  obscancelado: string | null | undefined,
  dsUsuarioCan: string | null | undefined
): Promise<void> {
  try {
    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_CP_CANCELAMENTO_CIOT_ALTERAR
        @Nrciot = ${toSqlValue(nrciot)},
        @Obscancelado = ${toSqlValue(obscancelado)},
        @DsUsuarioCan = ${toSqlValue((dsUsuarioCan || '').substring(0, 10))};
    `;

    logger.debug({ sql: sql.substring(0, 500) }, 'Executando stored procedure para cancelar CIOT');

    await prisma.$executeRawUnsafe(sql);

    logger.info({ nrciot }, 'CIOT cancelado com sucesso');
  } catch (error: any) {
    logger.error({ error: error.message, nrciot }, 'Erro ao cancelar CIOT');
    throw error;
  }
}

/**
 * Processa Inserção de ContasPagarCIOT completo
 */
export async function inserirContasPagarCIOT(
  data: ContasPagarCIOT,
  eventId?: string | null
): Promise<{ status: boolean; mensagem: string; manifestId?: number | null; created?: boolean }> {
  try {
    // Se cancelado = 1, apenas cancelar
    if (data.cancelado === 1) {
      await cancelarContasPagarCIOT(
        data.Manifest.nrciot,
        data.Obscancelado,
        data.DsUsuarioCan
      );
      return {
        status: true,
        mensagem: 'Registro cancelado com sucesso!',
        manifestId: null,
        created: false, // Cancelamento é uma atualização
      };
    }

    // Se cancelado = 0, inserir manifesto
    if (data.cancelado === 0 || data.cancelado === null || data.cancelado === undefined) {
      if (!data.Manifest) {
        return {
          status: false,
          mensagem: 'Manifesto não incluído, favor verificar lançamento!',
          manifestId: null,
        };
      }

      // Usar uma transação curta APENAS para a verificação de duplicação com lock
      // Isso garante que o lock seja mantido durante a verificação e previne condições de corrida
      // As inserções serão feitas fora da transação (stored procedures fazem commit automático)
      const checkResult = await prisma.$transaction(async (tx) => {
        // PRIORIDADE 1: Se external_id foi fornecido, verificar PRIMEIRO por ele (mais específico)
        if (data.Manifest.id) {
          const checkByExternalIdSql = `
            SELECT TOP 1 Id, external_id
            FROM dbo.manifests WITH (UPDLOCK, ROWLOCK, HOLDLOCK)
            WHERE external_id = ${toSqlValue(data.Manifest.id)}
            ORDER BY Id DESC;
          `;
          
          logger.debug(
            {
              externalId: data.Manifest.id,
            },
            'Verificando duplicação por external_id (prioridade 1)'
          );
          
          const existingByExternalId = await tx.$queryRawUnsafe<Array<{ Id: number; external_id: string | number | null }>>(checkByExternalIdSql);
          
          if (existingByExternalId && existingByExternalId.length > 0 && existingByExternalId[0]) {
            logger.debug(
              {
                foundId: existingByExternalId[0].Id,
                externalId: data.Manifest.id,
              },
              'Registro encontrado por external_id'
            );
            return existingByExternalId;
          }
        }
        
        // PRIORIDADE 2: Se não encontrou por external_id, verificar por nrciot + cdempresa + cdcartafrete
        const checkByKeysSql = `
          SELECT TOP 1 Id, external_id
          FROM dbo.manifests WITH (UPDLOCK, ROWLOCK, HOLDLOCK)
          WHERE nrciot = ${toSqlValue(data.Manifest.nrciot)}
            AND cdempresa = ${toSqlValue(data.Manifest.cdempresa)}
            AND cdcartafrete = ${toSqlValue(data.Manifest.cdcartafrete)}
          ORDER BY Id DESC;
        `;
        
        logger.debug(
          {
            nrciot: data.Manifest.nrciot,
            cdempresa: data.Manifest.cdempresa,
            cdcartafrete: data.Manifest.cdcartafrete,
          },
          'Verificando duplicação por chaves únicas (prioridade 2)'
        );
        
        const existingByKeys = await tx.$queryRawUnsafe<Array<{ Id: number; external_id: string | number | null }>>(checkByKeysSql);
        
        return existingByKeys;
      }, {
        timeout: 5000, // 5 segundos é suficiente para a verificação
        maxWait: 5000,
      });
      
      if (checkResult && checkResult.length > 0 && checkResult[0]) {
        const existingId = checkResult[0].Id;
        const existingExternalId = checkResult[0].external_id;
        const requestedExternalId = data.Manifest.id;
        
        // Se o external_id está diferente, é um problema - o registro foi encontrado mas com external_id errado
        // Isso pode acontecer se a stored procedure setou o external_id incorretamente em uma inserção anterior
        if (requestedExternalId && existingExternalId && String(existingExternalId) !== String(requestedExternalId)) {
          logger.warn(
            {
              existingId,
              existingExternalId,
              requestedExternalId,
              nrciot: data.Manifest.nrciot,
            },
            'ATENÇÃO: Registro encontrado mas external_id está diferente! Será corrigido no UPDATE.'
          );
        }
        
        logger.info(
          {
            existingId,
            existingExternalId,
            requestedExternalId,
            nrciot: data.Manifest.nrciot,
            cdempresa: data.Manifest.cdempresa,
            cdcartafrete: data.Manifest.cdcartafrete,
          },
          'Manifesto já existe na tabela destino, realizando UPDATE'
        );
        
        // Atualizar o manifesto existente (isso vai corrigir o external_id se estiver errado)
        await atualizarManifesto(data.Manifest, existingId);
        
        // Garantir que processed = 0 após UPDATE para que o worker processe novamente
        // O worker processará e mudará para processed = 1
        try {
          await prisma.$executeRawUnsafe(`
            UPDATE dbo.manifests
            SET processed = 0
            WHERE Id = ${existingId} AND (processed IS NULL OR processed != 0);
          `);
          logger.debug({ manifestId: existingId }, 'Campo processed = 0 setado após UPDATE (aguardando reprocessamento pelo worker)');
        } catch (updateError: any) {
          logger.warn({ manifestId: existingId, error: updateError.message }, 'Erro ao setar processed = 0 após UPDATE');
        }
        
        // Atualizar parcelas na tabela intermediária manifest_parcelas (NÃO nas tabelas da Senior)
        // Primeiro, deletar parcelas existentes para reinserir com os novos dados
        try {
          // Deletar parcelas existentes
          const deleteParcelasSql = `DELETE FROM dbo.manifest_parcelas WHERE manifest_id = ${existingId};`;
          await prisma.$executeRawUnsafe(deleteParcelasSql);
          logger.debug({ manifestId: existingId }, 'Parcelas antigas deletadas da tabela intermediária');
          
          // Deletar faturamento existente
          const deleteFaturamentoSql = `DELETE FROM dbo.manifest_faturamento WHERE manifest_id = ${existingId};`;
          await prisma.$executeRawUnsafe(deleteFaturamentoSql);
          logger.debug({ manifestId: existingId }, 'Faturamento antigo deletado da tabela intermediária');
          
          // Inserir novas parcelas se existirem
          if (data.Manifest.parcelas && data.Manifest.parcelas.length > 0) {
            logger.info({ manifestId: existingId, parcelasCount: data.Manifest.parcelas.length }, 'Inserindo parcelas atualizadas na tabela intermediária');
            let validParcelasCount = 0;
            for (let i = 0; i < data.Manifest.parcelas.length; i++) {
              const parcela = data.Manifest.parcelas[i];
              if (!parcela) {
                logger.warn({ manifestId: existingId, parcelaIndex: i + 1 }, 'Parcela é undefined, pulando');
                continue;
              }
              
              // Validar se a parcela tem idparcela válido antes de tentar inserir
              if (!isValidParcela(parcela)) {
                logger.warn({
                  manifestId: existingId,
                  parcelaIndex: i + 1,
                  parcelaId: parcela.ID,
                  idparcela: parcela.idparcela,
                  dstipo: parcela.dstipo
                }, 'Parcela com idparcela vazio ou nulo será ignorada - não será inserida');
                continue; // Pular esta parcela e continuar com as próximas
              }
              
              parcela.manifest_id = existingId;
              try {
                await inserirManifestoParcelas(parcela);
                validParcelasCount++;
              } catch (parcelaError: any) {
                logger.error(
                  {
                    error: parcelaError.message,
                    manifestId: existingId,
                    parcelaIndex: i + 1,
                    parcelaId: parcela.ID,
                  },
                  'Erro ao inserir parcela atualizada na tabela intermediária'
                );
                // Continuar com as outras parcelas mesmo se esta falhar
              }
            }
            logger.info({ manifestId: existingId, validParcelasCount, totalParcelas: data.Manifest.parcelas.length }, 'Parcelas atualizadas inseridas na tabela intermediária com sucesso');
          }
          
          // Inserir novo faturamento se existir
          if (data.Manifest.dadosFaturamento) {
            logger.info({ manifestId: existingId }, 'Inserindo faturamento atualizado na tabela intermediária');
            data.Manifest.dadosFaturamento.manifest_id = existingId;
            await inserirManifestoFaturamento(data.Manifest.dadosFaturamento);
            logger.info({ manifestId: existingId }, 'Faturamento atualizado inserido na tabela intermediária com sucesso');
          }
        } catch (updateError: any) {
          logger.error(
            {
              error: updateError.message,
              stack: updateError.stack,
              manifestId: existingId,
            },
            'Erro ao atualizar parcelas/faturamento na tabela intermediária'
          );
          // Não lançar erro, pois o manifesto já foi atualizado
        }
        
        logger.info(
          { manifestId: existingId },
          'Manifesto atualizado. Parcelas e faturamento serão processados pelo worker (processed = 0)'
        );
        
        return {
          status: true,
          mensagem: 'Registro atualizado com sucesso!',
          manifestId: existingId,
          created: false, // Indica que foi atualizado (não criado)
        };
      }
      
      logger.info(
        {
          nrciot: data.Manifest.nrciot,
          cdempresa: data.Manifest.cdempresa,
          cdcartafrete: data.Manifest.cdcartafrete,
        },
        'Nenhum registro encontrado, inserindo manifesto (fora da transação)'
      );
      
      // Capturar MAX(Id) ANTES de inserir para verificar se foi realmente uma inserção nova
      const maxIdBeforeSql = `
        SELECT ISNULL(MAX(Id), 0) AS MaxId
        FROM dbo.manifests WITH (NOLOCK);
      `;
      const maxIdBeforeResult = await prisma.$queryRawUnsafe<Array<{ MaxId: number }>>(maxIdBeforeSql);
      const maxIdBefore = maxIdBeforeResult && maxIdBeforeResult[0] ? maxIdBeforeResult[0].MaxId : 0;
      
      logger.debug(
        {
          maxIdBefore,
          nrciot: data.Manifest.nrciot,
        },
        'MAX(Id) capturado antes de inserir'
      );
      
      // Inserir manifesto e obter ID
      // NOTA: A stored procedure faz commit automático, então o manifesto será inserido imediatamente
      const insertResult = await inserirManifesto(data.Manifest);
      let manifestId = insertResult.id;
      let wasNewInsert = insertResult.wasNewInsert;
      
      // Verificar novamente se o ID retornado é realmente maior que maxIdBefore
      // Isso garante que foi uma inserção nova mesmo em condições de corrida
      if (manifestId > 0 && manifestId <= maxIdBefore) {
        logger.warn(
          {
            manifestId,
            maxIdBefore,
            nrciot: data.Manifest.nrciot,
          },
          'ID retornado não é maior que maxIdBefore, pode ser um registro existente. Verificando novamente...'
        );
        // Verificar se realmente existe um registro com esse ID e as mesmas chaves
        const verifyIdSql = `
          SELECT TOP 1 Id, external_id
          FROM dbo.manifests WITH (NOLOCK)
          WHERE Id = ${manifestId}
            AND nrciot = ${toSqlValue(data.Manifest.nrciot)}
            AND cdempresa = ${toSqlValue(data.Manifest.cdempresa)}
            AND cdcartafrete = ${toSqlValue(data.Manifest.cdcartafrete)}
        `;
        const verifyIdResult = await prisma.$queryRawUnsafe<Array<{ Id: number; external_id: string | number | null }>>(verifyIdSql);
        
        if (verifyIdResult && verifyIdResult.length > 0 && verifyIdResult[0]) {
          // O registro existe, mas pode ter sido criado por outra requisição simultânea
          // Verificar se foi criado recentemente comparando com maxIdBefore
          if (manifestId <= maxIdBefore) {
            logger.warn(
              {
                manifestId,
                maxIdBefore,
                nrciot: data.Manifest.nrciot,
              },
              'Registro existe mas ID <= maxIdBefore, considerando como já existente'
            );
            wasNewInsert = false;
          }
        }
      }
      
      logger.info(
        {
          manifestId,
          wasNewInsert,
          maxIdBefore,
          nrciot: data.Manifest.nrciot,
        },
        'Manifesto inserido pela stored procedure (commit automático feito)'
      );

      // Verificar novamente após inserção para garantir que não foi duplicado
      // Esta verificação é crítica para detectar duplicações em condições de corrida
      // Verificar por external_id primeiro (se fornecido), depois por chaves únicas
      let verifyAfterInsertSql = '';
      if (data.Manifest.id) {
        verifyAfterInsertSql = `
          SELECT Id, external_id
          FROM dbo.manifests WITH (NOLOCK)
          WHERE external_id = ${toSqlValue(data.Manifest.id)}
             OR (nrciot = ${toSqlValue(data.Manifest.nrciot)}
                 AND cdempresa = ${toSqlValue(data.Manifest.cdempresa)}
                 AND cdcartafrete = ${toSqlValue(data.Manifest.cdcartafrete)})
          ORDER BY Id ASC;
        `;
      } else {
        verifyAfterInsertSql = `
          SELECT Id, external_id
          FROM dbo.manifests WITH (NOLOCK)
          WHERE nrciot = ${toSqlValue(data.Manifest.nrciot)}
            AND cdempresa = ${toSqlValue(data.Manifest.cdempresa)}
            AND cdcartafrete = ${toSqlValue(data.Manifest.cdcartafrete)}
          ORDER BY Id ASC;
        `;
      }
      const verifyAfterInsert = await prisma.$queryRawUnsafe<Array<{ Id: number; external_id: string | number | null }>>(verifyAfterInsertSql);
      
      if (verifyAfterInsert && verifyAfterInsert.length > 0) {
        // Se há mais de um registro, houve duplicação
        if (verifyAfterInsert.length > 1) {
          logger.error(
            {
              insertedId: manifestId,
              allIds: verifyAfterInsert.map(r => r.Id),
              allExternalIds: verifyAfterInsert.map(r => r.external_id),
              requestedExternalId: data.Manifest.id,
              nrciot: data.Manifest.nrciot,
              maxIdBefore,
            },
            'Duplicação detectada após inserção! Múltiplos registros encontrados com as mesmas chaves'
          );
          
          // Priorizar o registro com external_id correto (se fornecido)
          let correctRecord = verifyAfterInsert.find(r => 
            data.Manifest.id && String(r.external_id) === String(data.Manifest.id)
          );
          
          // Se não encontrou por external_id, usar o mais antigo (menor ID)
          if (!correctRecord) {
            correctRecord = verifyAfterInsert.sort((a, b) => a.Id - b.Id)[0];
          }
          
          const correctId = correctRecord?.Id || manifestId;
          const duplicateIds = verifyAfterInsert
            .filter(r => r.Id !== correctId)
            .map(r => r.Id);
          
          // Se o ID inserido não é o correto, deletar o registro duplicado e não inserir parcelas
          if (manifestId !== correctId) {
            logger.warn(
              {
                insertedId: manifestId,
                correctId: correctId,
                correctExternalId: correctRecord?.external_id,
                duplicateIds: duplicateIds,
                maxIdBefore,
              },
              'Registro inserido é duplicado, será deletado. Mantendo apenas o registro correto.'
            );
            
            // Deletar o registro duplicado que acabamos de inserir
            try {
              const deleteSql = `DELETE FROM dbo.manifests WHERE Id = ${manifestId};`;
              await prisma.$executeRawUnsafe(deleteSql);
              logger.info({ deletedId: manifestId }, 'Registro duplicado deletado com sucesso');
            } catch (deleteError: any) {
              logger.error(
                { error: deleteError.message, deletedId: manifestId },
                'Erro ao deletar registro duplicado'
              );
            }
            
            return {
              status: true,
              mensagem: 'Registro já existe na tabela destino (duplicação detectada e removida)',
              manifestId: correctId,
            };
          } else {
            // Se o ID inserido é o correto, deletar os outros duplicados
            logger.warn(
              {
                keptId: manifestId,
                duplicateIds: duplicateIds,
                maxIdBefore,
              },
              'Múltiplos registros encontrados. Mantendo o correto e deletando duplicados.'
            );
            
            // Deletar os registros duplicados (mais recentes)
            for (const dupId of duplicateIds) {
              try {
                const deleteSql = `DELETE FROM dbo.manifests WHERE Id = ${dupId};`;
                await prisma.$executeRawUnsafe(deleteSql);
                logger.info({ deletedId: dupId }, 'Registro duplicado deletado com sucesso');
              } catch (deleteError: any) {
                logger.error(
                  { error: deleteError.message, deletedId: dupId },
                  'Erro ao deletar registro duplicado'
                );
              }
            }
          }
        } else {
          // Apenas um registro encontrado, verificar se o ID corresponde
          const verifiedId = verifyAfterInsert[0]?.Id;
          if (verifiedId && verifiedId !== manifestId) {
            logger.warn(
              {
                insertedId: manifestId,
                verifiedId: verifiedId,
                nrciot: data.Manifest.nrciot,
                maxIdBefore,
              },
              'ID verificado após inserção difere do ID retornado pela stored procedure. Usando ID verificado.'
            );
            manifestId = verifiedId;
            // Se o ID verificado é <= maxIdBefore, pode ser um registro existente
            if (verifiedId <= maxIdBefore) {
              logger.warn(
                {
                  verifiedId,
                  maxIdBefore,
                  nrciot: data.Manifest.nrciot,
                },
                'ID verificado <= maxIdBefore, pode ser um registro existente. Verificando...'
              );
              // Verificar se o registro foi criado recentemente (dentro dos últimos segundos)
              // Se não, considerar como já existente
              wasNewInsert = false;
            }
          }
        }
      }

      logger.info(
          { manifestId, wasNewInsert, nrciot: data.Manifest.nrciot, hasParcelas: !!data.Manifest.parcelas?.length },
          'Resultado da inserção do manifesto'
        );

        // Mesmo se o registro já existia, devemos inserir/atualizar parcelas e faturamento
        // pois podem ter mudado no payload
        if (!wasNewInsert) {
          logger.info(
            { manifestId, nrciot: data.Manifest.nrciot },
            'Manifesto já existia, mas parcelas e faturamento serão inseridos/atualizados'
          );
        }

        // Rastreamento de tabelas inseridas para o evento
        const tabelasInseridas: string[] = [];
        const tabelasFalhadas: Array<{ tabela: string; erro: string }> = [];
        
        // Tabela manifests já foi inserida com sucesso
        tabelasInseridas.push('dbo.manifests');
        
        // Inserir parcelas na tabela intermediária manifest_parcelas (NÃO nas tabelas da Senior)
        // O worker processará essas parcelas nas tabelas da Senior depois
        // Primeiro, deletar parcelas existentes para evitar duplicação
        if (data.Manifest.parcelas && data.Manifest.parcelas.length > 0) {
          logger.info({ manifestId, parcelasCount: data.Manifest.parcelas.length }, 'Inserindo parcelas na tabela intermediária manifest_parcelas');
          try {
            // Deletar parcelas existentes primeiro para evitar duplicação
            const deleteParcelasSql = `DELETE FROM dbo.manifest_parcelas WHERE manifest_id = ${manifestId};`;
            await prisma.$executeRawUnsafe(deleteParcelasSql);
            logger.debug({ manifestId }, 'Parcelas antigas deletadas da tabela intermediária');
            
            const errors: Array<{ index: number; error: string }> = [];
            let successCount = 0;
            
            for (let i = 0; i < data.Manifest.parcelas.length; i++) {
              const parcela = data.Manifest.parcelas[i];
              if (!parcela) {
                logger.warn({ manifestId, parcelaIndex: i + 1 }, 'Parcela é undefined, pulando');
                continue;
              }
              
              // Validar se a parcela tem idparcela válido antes de tentar inserir
              if (!isValidParcela(parcela)) {
                logger.warn({
                  manifestId,
                  parcelaIndex: i + 1,
                  parcelaId: parcela.ID,
                  idparcela: parcela.idparcela,
                  dstipo: parcela.dstipo
                }, 'Parcela com idparcela vazio ou nulo será ignorada - não será inserida');
                continue; // Pular esta parcela e continuar com as próximas
              }
              
              try {
                parcela.manifest_id = manifestId;
                
                logger.debug({ 
                  manifestId, 
                  parcelaIndex: i + 1, 
                  totalParcelas: data.Manifest.parcelas.length,
                  parcelaId: parcela.ID,
                  idparcela: parcela.idparcela,
                  dsstatus: parcela.dsstatus,
                  dstipo: parcela.dstipo
                }, 'Inserindo parcela na tabela intermediária');
                
                await inserirManifestoParcelas(parcela);
                
                successCount++;
                logger.debug({ manifestId, parcelaIndex: i + 1, parcelaId: parcela.ID }, 'Parcela inserida na tabela intermediária com sucesso');
              } catch (parcelaError: any) {
                logger.error(
                  {
                    error: parcelaError.message,
                    stack: parcelaError.stack,
                    manifestId,
                    parcelaIndex: i + 1,
                    parcelaId: parcela.ID,
                  },
                  `Erro ao inserir parcela ${i + 1} na tabela intermediária - continuando com as outras`
                );
                errors.push({ index: i + 1, error: parcelaError.message });
                // Continuar com a próxima parcela mesmo se esta falhar
              }
            }
            
            if (errors.length > 0) {
              logger.warn(
                {
                  manifestId,
                  totalParcelas: data.Manifest.parcelas.length,
                  successCount,
                  errorCount: errors.length,
                  errors,
                },
                'Algumas parcelas falharam ao serem inseridas, mas as demais foram processadas'
              );
            }
            
            if (successCount === 0) {
              tabelasFalhadas.push({ tabela: 'dbo.manifest_parcelas', erro: `Todas as ${data.Manifest.parcelas.length} parcelas falharam ao serem inseridas` });
              throw new Error(`Todas as ${data.Manifest.parcelas.length} parcelas falharam ao serem inseridas`);
            }
            
            // Registrar sucesso/parcial da tabela de parcelas
            if (errors.length > 0) {
              tabelasFalhadas.push({ 
                tabela: 'dbo.manifest_parcelas', 
                erro: `${errors.length} de ${data.Manifest.parcelas.length} parcelas falharam` 
              });
            } else {
              tabelasInseridas.push('dbo.manifest_parcelas');
            }
            
            logger.info({ manifestId, successCount, totalParcelas: data.Manifest.parcelas.length }, 'Parcelas inseridas na tabela intermediária');
          } catch (parcelaError: any) {
            logger.error(
              {
                error: parcelaError.message,
                stack: parcelaError.stack,
                manifestId,
                parcelasCount: data.Manifest.parcelas.length,
              },
              'Erro crítico ao inserir parcelas na tabela intermediária'
            );
            // Lançar erro apenas se foi um erro crítico (não de parcela individual)
            const nrciot = data?.Manifest?.nrciot || 'N/A';
            throw new Error(`Falha crítica ao inserir parcelas do CIOT ${nrciot}: ${parcelaError.message}. O manifesto pode ter sido salvo, mas as parcelas não foram registradas.`);
          }
        } else {
          logger.warn({ manifestId }, 'Nenhuma parcela encontrada no payload para inserir na tabela intermediária');
        }

        // Inserir faturamento na tabela intermediária manifest_faturamento (NÃO nas tabelas da Senior)
        logger.debug({ 
          manifestId, 
          hasDadosFaturamento: !!data.Manifest.dadosFaturamento,
          dadosFaturamento: data.Manifest.dadosFaturamento 
        }, 'Verificando dadosFaturamento antes de inserir');
        
        if (data.Manifest.dadosFaturamento) {
          logger.info({ manifestId, dadosFaturamento: data.Manifest.dadosFaturamento }, 'Inserindo faturamento na tabela intermediária manifest_faturamento');
          try {
            data.Manifest.dadosFaturamento.manifest_id = manifestId;
            await inserirManifestoFaturamento(data.Manifest.dadosFaturamento);
            tabelasInseridas.push('dbo.manifest_faturamento');
            logger.info({ manifestId }, 'Faturamento inserido na tabela intermediária com sucesso');
          } catch (faturamentoError: any) {
            tabelasFalhadas.push({ 
              tabela: 'dbo.manifest_faturamento', 
              erro: faturamentoError.message 
            });
            logger.error(
              {
                error: faturamentoError.message,
                stack: faturamentoError.stack,
                manifestId,
                dadosFaturamento: data.Manifest.dadosFaturamento,
              },
              'Erro ao inserir faturamento na tabela intermediária'
            );
            // Não falhar completamente, apenas logar o erro
          }
        } else {
          logger.warn({ manifestId, nrciot: data.Manifest.nrciot }, 'dadosFaturamento não está presente no payload - não será inserido na tabela manifest_faturamento');
        }

        logger.info(
          { manifestId, parcelasCount: data.Manifest.parcelas?.length || 0, hasFaturamento: !!data.Manifest.dadosFaturamento },
          'Manifesto e dados relacionados inseridos. Worker processará nas tabelas da Senior (processed = 0)'
        );

        // Atualizar evento WebhookEvent com manifestId correto e informações detalhadas das tabelas
        if (eventId) {
          const finalEventId = generateCiotEventId(manifestId, data.Manifest.nrciot);
          const temFalhas = tabelasFalhadas.length > 0;
          
          let mensagemErro: string | null = null;
          if (temFalhas) {
            const tabelasOk = tabelasInseridas.length > 0 
              ? `Tabelas inseridas com sucesso: ${tabelasInseridas.join(', ')}. `
              : '';
            const tabelasErro = `Tabelas com erro: ${tabelasFalhadas.map(t => `${t.tabela} (${t.erro})`).join(', ')}.`;
            mensagemErro = tabelasOk + tabelasErro;
          }
          
          const metadata: any = {
            manifestId: manifestId,
            nrciot: data.Manifest.nrciot,
            parcelasCount: data.Manifest.parcelas?.length || 0,
            etapa: 'backend_inserido',
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
            finalEventId,
            '/api/CIOT/InserirContasPagarCIOT',
            'processed',
            mensagemErro,
            metadata
          );
        }

      return {
        status: true,
        mensagem: 'Registro criado com sucesso!',
        manifestId: manifestId,
        created: true, // Indica que foi criado (não atualizado)
      };
    }

    return {
      status: false,
      mensagem: 'Valor de cancelado inválido',
      manifestId: null,
    };
  } catch (error: any) {
    const errorMessage = error?.message || '';
    const errorCode = error?.code || error?.meta?.code || null;
    const errorName = error?.name || 'UnknownError';
    
    // Log detalhado do erro
    logger.error(
      {
        error: errorMessage,
        errorName,
        errorCode,
        stack: error.stack,
        nrciot: data?.Manifest?.nrciot,
        cdempresa: data?.Manifest?.cdempresa,
        cdcartafrete: data?.Manifest?.cdcartafrete,
        external_id: data?.Manifest?.id,
        hasParcelas: !!(data?.Manifest?.parcelas && data.Manifest.parcelas.length > 0),
        parcelasCount: data?.Manifest?.parcelas?.length || 0,
        hasFaturamento: !!data?.Manifest?.dadosFaturamento,
        meta: error?.meta,
      },
      'Erro ao inserir ContasPagarCIOT'
    );
    
    // Se o erro é relacionado a parcelas ou faturamento, o manifesto pode já ter sido inserido
    // pela stored procedure (que faz commit automático)
    
    // Tentar identificar quais tabelas foram inseridas antes do erro
    const tabelasInseridas: string[] = [];
    const tabelasFalhadas: Array<{ tabela: string; erro: string }> = [];
    
    // Verificar se o manifesto foi inserido (pode ter sido inserido pela stored procedure antes do erro)
    if (errorMessage.includes('parcelas') || errorMessage.includes('faturamento')) {
      tabelasInseridas.push('dbo.manifests'); // Provavelmente foi inserido
    } else {
      tabelasFalhadas.push({ 
        tabela: 'dbo.manifests', 
        erro: errorMessage || 'Erro desconhecido durante inserção do manifesto' 
      });
    }
    
    // Construir mensagem de erro mais detalhada
    let mensagemErroDetalhada = errorMessage || 'Erro desconhecido ao inserir ContasPagarCIOT';
    
    if (errorCode) {
      mensagemErroDetalhada += ` (Código: ${errorCode})`;
    }
    
    // Atualizar evento WebhookEvent com erro detalhado
    if (eventId) {
      const metadata: any = {
        nrciot: data?.Manifest?.nrciot || null,
        erro: mensagemErroDetalhada,
        etapa: 'backend_falha',
        errorCode,
        errorName,
      };
      
      if (tabelasInseridas.length > 0) {
        metadata.tabelasInseridas = tabelasInseridas;
      }
      
      if (tabelasFalhadas.length > 0) {
        metadata.tabelasFalhadas = tabelasFalhadas;
        const tabelasOk = tabelasInseridas.length > 0 
          ? `Tabelas inseridas: ${tabelasInseridas.join(', ')}. `
          : '';
        const tabelasErro = `Tabelas com erro: ${tabelasFalhadas.map(t => `${t.tabela} (${t.erro})`).join(', ')}.`;
        mensagemErroDetalhada = mensagemErroDetalhada + (tabelasOk || tabelasErro ? `. ${tabelasOk}${tabelasErro}` : '');
        metadata.erro = mensagemErroDetalhada;
      }
      
      if (tabelasInseridas.length > 0 || tabelasFalhadas.length > 0) {
        metadata.resumo = {
          totalTabelas: tabelasInseridas.length + tabelasFalhadas.length,
          sucesso: tabelasInseridas.length,
          falhas: tabelasFalhadas.length,
        };
      }
      
      await createOrUpdateWebhookEvent(
        eventId,
        '/api/CIOT/InserirContasPagarCIOT',
        'failed',
        mensagemErroDetalhada,
        metadata
      );
    }
    
    if (errorMessage.includes('parcelas') || errorMessage.includes('faturamento')) {
      const nrciot = data?.Manifest?.nrciot || 'desconhecido';
      const tipo = errorMessage.includes('parcelas') ? 'parcelas' : 'dados de faturamento';
      return {
        status: false,
        mensagem: `CIOT ${nrciot}: Manifesto registrado com sucesso, mas falha ao salvar ${tipo}. Corrija os dados de ${tipo} e reenvie a requisição para atualizar.`,
        manifestId: null,
      };
    }
    
    // Construir mensagem mais detalhada e amigável
    let mensagemRetorno = 'Erro ao processar CIOT';
    
    // Adicionar informações sobre o CIOT e manifestor
    const nrciot = data?.Manifest?.nrciot;
    if (nrciot) {
      mensagemRetorno += ` (CIOT ${nrciot})`;
    }
    
    // Identificar tipo de erro e fornecer mensagem específica
    if (errorMessage) {
      // Erro de truncamento (campo muito grande)
      if (errorCode === '2628' || errorMessage.includes('truncated')) {
        const truncatedMatch = errorMessage.match(/column '(\w+)'.*value: '([^']+)'/i);
        if (truncatedMatch) {
          const coluna = truncatedMatch[1];
          const valorTruncado = truncatedMatch[2];
          mensagemRetorno += `: Campo "${coluna}" excede tamanho permitido. Valor tentado: "${valorTruncado}..."`;
        } else {
          mensagemRetorno += ': Dados excedem tamanho permitido de um ou mais campos.';
        }
        mensagemRetorno += ' Verifique os dados enviados e ajuste os valores.';
      }
      // Erro de chave duplicada
      else if (errorCode === '2627' || errorMessage.includes('duplicate key') || errorMessage.includes('UNIQUE KEY constraint')) {
        const tableMatch = errorMessage.match(/in object '([^']+)'/i);
        const keyMatch = errorMessage.match(/key value is \(([^)]+)\)/i);
        if (tableMatch && keyMatch) {
          mensagemRetorno += `: Registro duplicado na tabela "${tableMatch[1]}" com chave ${keyMatch[1]}.`;
        } else {
          mensagemRetorno += ': Registro já existe no banco de dados.';
        }
        mensagemRetorno += ' Use o mesmo payload para atualizar ou verifique se já foi processado.';
      }
      // Erro de deadlock
      else if (errorCode === '1205' || errorMessage.includes('deadlock')) {
        mensagemRetorno += ': Sistema temporariamente ocupado processando outras requisições.';
        mensagemRetorno += ' Aguarde alguns segundos e tente novamente.';
      }
      // Erro de timeout de transação
      else if (errorCode === 'P2028' || errorMessage.includes('Transaction already closed') || errorMessage.includes('timeout')) {
        mensagemRetorno += ': Tempo limite de processamento excedido.';
        mensagemRetorno += ' Isso pode ocorrer com muitas parcelas ou dados volumosos. Tente novamente.';
      }
      // Erro genérico com código
      else {
        mensagemRetorno += `: ${errorMessage}`;
        if (errorCode) {
          mensagemRetorno += ` (Código: ${errorCode})`;
        }
      }
    } else {
      mensagemRetorno += ': Erro desconhecido durante processamento.';
    }
    
    // Adicionar dica sobre logs apenas se não for erro conhecido
    if (!errorCode || !['2628', '2627', '1205', 'P2028'].includes(errorCode)) {
      mensagemRetorno += ' Consulte os logs do sistema para mais detalhes técnicos.';
    }
    
    return {
      status: false,
      mensagem: mensagemRetorno,
      manifestId: null,
    };
  }
}

