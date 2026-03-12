import type { Request, Response } from 'express';
import { PrismaClient } from '@prisma/client';
import { pessoaSchema } from '../schemas/pessoa';
import { inserirPessoa } from '../services/pessoaService';
import { logger } from '../utils/logger';
import { createOrUpdateWebhookEvent } from '../utils/webhookEvent';

const prisma = new PrismaClient();

/**
 * Gera um eventId para Pessoa baseado no CodPessoaEsl
 */
function generatePessoaEventId(codPessoaEsl?: string | null, codPessoa?: number | null): string {
  if (codPessoa) {
    return `pessoa-${codPessoa}`;
  }
  if (codPessoaEsl) {
    return `pessoa-${codPessoaEsl}-${Date.now()}`;
  }
  return `pessoa-${Date.now()}`;
}

/**
 * Controller para inserir Pessoa
 * POST /api/Pessoa/InserirPessoa?token=...
 */
/**
 * Normaliza campos do payload de camelCase para PascalCase
 */
function normalizePessoaPayload(body: any): any {
  const normalized: any = { ...body };

  // Normalizar campos principais
  if (body.codPessoaEsl !== undefined && body.CodPessoaEsl === undefined) {
    normalized.CodPessoaEsl = body.codPessoaEsl;
  }
  if (body.tipoPessoa !== undefined && body.TipoPessoa === undefined) {
    normalized.TipoPessoa = normalizeTipoPessoa(body.tipoPessoa);
  }
  if (body.contatoList !== undefined && body.ContatoList === undefined) {
    normalized.ContatoList = Array.isArray(body.contatoList)
      ? body.contatoList.map((c: any) => normalizeContato(c))
      : body.contatoList;
  }
  if (body.enderecoList !== undefined && body.EnderecoList === undefined) {
    normalized.EnderecoList = Array.isArray(body.enderecoList)
      ? body.enderecoList.map((e: any) => normalizeEndereco(e))
      : body.enderecoList;
  }
  if (body.dadosBancarioList !== undefined && body.DadosBancarioList === undefined) {
    normalized.DadosBancarioList = Array.isArray(body.dadosBancarioList)
      ? body.dadosBancarioList.map((d: any) => normalizeDadosBancario(d))
      : body.dadosBancarioList;
  }
  if (body.chavesPixList !== undefined && body.ChavesPixList === undefined) {
    normalized.ChavesPixList = Array.isArray(body.chavesPixList)
      ? body.chavesPixList.map((c: any) => normalizeChavePix(c))
      : body.chavesPixList;
  }
  if (body.pessoaPersonagemList !== undefined && body.PessoaPersonagemList === undefined) {
    normalized.PessoaPersonagemList = Array.isArray(body.pessoaPersonagemList)
      ? body.pessoaPersonagemList.map((p: any) => normalizePessoaPersonagem(p))
      : body.pessoaPersonagemList;
  }
  if (body.pessoaMotorista !== undefined && body.PessoaMotorista === undefined) {
    normalized.PessoaMotorista = normalizePessoaMotorista(body.pessoaMotorista);
  }
  if (body.pessoaFuncionario !== undefined && body.PessoaFuncionario === undefined) {
    normalized.PessoaFuncionario = normalizePessoaFuncionario(body.pessoaFuncionario);
  }

  // Normalizar outros campos principais (preservar ambos para compatibilidade)
  const fieldMappings: Record<string, string> = {
    codPessoa: 'CodPessoa',
    nomeRazaoSocial: 'NomeRazaoSocial',
    nomeFantasia: 'NomeFantasia',
    cpf: 'Cpf',
    cnpj: 'Cnpj',
    rg: 'RG',
    cidadeExpedicaoRG: 'CidadeExpedicaoRG',
    orgaoExpedidorRG: 'OrgaoExpedidorRG',
    dataEmissaoRG: 'DataEmissaoRG',
    estadoCivil: 'EstadoCivil',
    dataNascimento: 'DataNascimento',
    cidadeNascimento: 'CidadeNascimento',
    inscricaoMunicipal: 'InscricaoMunicipal',
    inscricaoEstadual: 'InscricaoEstadual',
    documentoExterior: 'DocumentoExterior',
    numeroPISPASEP: 'NumeroPISPASEP',
    rntrc: 'RNTRC',
    dataValidadeRNTRC: 'DataValidadeRNTRC',
    nomePai: 'NomePai',
    nomeMae: 'NomeMae',
    contrinuinte: 'Contrinuinte',
    site: 'Site',
    codigoCNAE: 'CodigoCNAE',
    regimeFiscal: 'RegimeFiscal',
    codigoSuframa: 'CodigoSuframa',
    observacao: 'Observacao',
    dataCadastro: 'DataCadastro',
    codUsuarioCadastro: 'CodUsuarioCadastro',
    usuarioCadastro: 'UsuarioCadastro',
    ativo: 'Ativo',
    codNaturezaOperacao: 'CodNaturezaOperacao',
    descNaturezaOperacao: 'DescNaturezaOperacao',
    codFilialResponsavel: 'CodFilialResponsavel',
    descFilialResponsavel: 'DescFilialResponsavel',
    dataFundacao: 'DataFundacao',
    cdFuncionario: 'cdFuncionario',
  };

  for (const [camelCase, pascalCase] of Object.entries(fieldMappings)) {
    if (body[camelCase] !== undefined && body[pascalCase] === undefined) {
      normalized[pascalCase] = body[camelCase];
    }
  }

  // Preservar campos que já estão em PascalCase
  for (const key of Object.keys(body)) {
    if (key && key.length > 0) {
      const firstChar = key.charAt(0);
      if (firstChar === firstChar.toUpperCase() && !(key in normalized)) {
        (normalized as Record<string, any>)[key] = body[key];
      }
    }
  }

  return normalized;
}

function normalizeTipoPessoa(tipo: any): any {
  if (!tipo) return tipo;
  return {
    CodTipoPessoa: tipo.CodTipoPessoa ?? tipo.codTipoPessoa,
    Descricao: tipo.Descricao ?? tipo.descricao,
  };
}

function normalizeContato(contato: any): any {
  if (!contato) return contato;
  return {
    CodContato: contato.CodContato ?? contato.codContato,
    CodTipoContato: contato.CodTipoContato ?? contato.codTipoContato,
    Nome: contato.Nome ?? contato.nome,
    Cpf: contato.Cpf ?? contato.cpf,
    Ddd: contato.Ddd ?? contato.ddd,
    Telefone: contato.Telefone ?? contato.telefone,
    Celular: contato.Celular ?? contato.celular,
    Departamento: contato.Departamento ?? contato.departamento,
    Email: contato.Email ?? contato.email,
    Site: contato.Site ?? contato.site,
    Observacao: contato.Observacao ?? contato.observacao,
  };
}

function normalizeEndereco(endereco: any): any {
  if (!endereco) return endereco;
  return {
    CodEndereco: endereco.CodEndereco ?? endereco.codEndereco,
    CodTipoEndereco:
      endereco.CodTipoEndereco ??
      endereco.tipoEndereco?.codTipoEndereco ??
      endereco.tipoEndereco?.CodTipoEndereco,
    Cep: endereco.Cep ?? endereco.cep,
    Logradouro: endereco.Logradouro ?? endereco.logradouro,
    Numero: endereco.Numero ?? endereco.numero,
    Complemento: endereco.Complemento ?? endereco.complemento,
    Bairro: endereco.Bairro ?? endereco.bairro,
    Cidade: endereco.Cidade ?? endereco.cidade,
    Estado: endereco.Estado ?? endereco.estado,
  };
}

function normalizeDadosBancario(dados: any): any {
  if (!dados) return dados;
  return {
    CodDadosBancario: dados.CodDadosBancario ?? dados.codDadosBancario,
    Banco:
      (dados.Banco ?? dados.banco)
        ? {
            CodBanco: dados.banco.CodBanco ?? dados.banco.codBanco,
            Codigo: dados.banco.Codigo ?? dados.banco.codigo,
            Nome: dados.banco.Nome ?? dados.banco.nome,
          }
        : undefined,
    TipoConta:
      (dados.TipoConta ?? dados.tipoConta)
        ? {
            CodTipoConta: dados.tipoConta.CodTipoConta ?? dados.tipoConta.codTipoConta,
            Descricao: dados.tipoConta.Descricao ?? dados.tipoConta.descricao,
          }
        : undefined,
    Titularidade:
      (dados.Titularidade ?? dados.titularidade)
        ? {
            CodTitularidade:
              dados.titularidade.CodTitularidade ?? dados.titularidade.codTitularidade,
            Descricao: dados.titularidade.Descricao ?? dados.titularidade.descricao,
          }
        : undefined,
    NumAgencia: dados.NumAgencia ?? dados.numAgencia,
    NumConta: dados.NumConta ?? dados.numConta,
    NomeTitular: dados.NomeTitular ?? dados.nomeTitular,
    CpfCnpj: dados.CpfCnpj ?? dados.cpfCnpj,
  };
}

function normalizeChavePix(chave: any): any {
  if (!chave) return chave;
  return {
    CodChavesPix: chave.CodChavesPix ?? chave.codChavesPix,
    TipoChave:
      (chave.TipoChave ?? chave.tipoChave)
        ? {
            CodTipoChave: chave.tipoChave.CodTipoChave ?? chave.tipoChave.codTipoChave,
            Descricao: chave.tipoChave.Descricao ?? chave.tipoChave.descricao,
          }
        : undefined,
    ChavePix: chave.ChavePix ?? chave.chavePix,
    NomeTitular: chave.NomeTitular ?? chave.nomeTitular,
  };
}

function normalizePessoaPersonagem(personagem: any): any {
  if (!personagem) return personagem;
  return {
    CodPessoaPersonagem: personagem.CodPessoaPersonagem ?? personagem.codPessoaPersonagem,
    Descricao: personagem.Descricao ?? personagem.descricao,
  };
}

function normalizePessoaMotorista(motorista: any): any {
  if (!motorista) return motorista;
  return {
    CodPessoaMotorista: motorista.CodPessoaMotorista ?? motorista.codPessoaMotorista,
    CodPessoa: motorista.CodPessoa ?? motorista.codPessoa,
    CodPessoaEsl: motorista.CodPessoaEsl ?? motorista.codPessoaEsl,
    NumeroCNH: motorista.NumeroCNH ?? motorista.numeroCNH,
    NumeroRegistroCNH: motorista.NumeroRegistroCNH ?? motorista.numeroRegistroCNH,
    DataPrimeiraCNH: motorista.DataPrimeiraCNH ?? motorista.dataPrimeiraCNH,
    DataEmissaoCNH: motorista.DataEmissaoCNH ?? motorista.dataEmissaoCNH,
    DataVencimentoCNH: motorista.DataVencimentoCNH ?? motorista.dataVencimentoCNH,
    TipoCNH: motorista.TipoCNH ?? motorista.tipoCNH,
    CidadeCNH: motorista.CidadeCNH ?? motorista.cidadeCNH,
    OrgaoEmissor: motorista.OrgaoEmissor ?? motorista.orgaoEmissor,
    CodSegurancaCNH: motorista.CodSegurancaCNH ?? motorista.codSegurancaCNH,
    NumeroRenach: motorista.NumeroRenach ?? motorista.numeroRenach,
    DataCadastro: motorista.DataCadastro ?? motorista.dataCadastro,
    CodUsuarioCadastro: motorista.CodUsuarioCadastro ?? motorista.codUsuarioCadastro,
    UsuarioCadastro: motorista.UsuarioCadastro ?? motorista.usuarioCadastro,
  };
}

function normalizePessoaFuncionario(funcionario: any): any {
  if (!funcionario) return funcionario;
  return {
    CodPessoaFuncionario: funcionario.CodPessoaFuncionario ?? funcionario.codPessoaFuncionario,
    CdFuncionario: funcionario.CdFuncionario ?? funcionario.cdFuncionario,
    CodPessoa: funcionario.CodPessoa ?? funcionario.codPessoa,
    CodPessoaEsl: funcionario.CodPessoaEsl ?? funcionario.codPessoaEsl,
    DsNumCtps: funcionario.DsNumCtps ?? funcionario.dsNumCtps,
    DsSerieCtps: funcionario.DsSerieCtps ?? funcionario.dsSerieCtps,
    DsUfCtps: funcionario.DsUfCtps ?? funcionario.dsUfCtps,
    DtAdmissao: funcionario.DtAdmissao ?? funcionario.dtAdmissao,
    VlSalarioAtual: funcionario.VlSalarioAtual ?? funcionario.vlSalarioAtual,
    DsCorPele: funcionario.DsCorPele ?? funcionario.dsCorPele,
    DtEmissaoCTPS: funcionario.DtEmissaoCTPS ?? funcionario.dtEmissaoCTPS,
    VlSalarioInicial: funcionario.VlSalarioInicial ?? funcionario.vlSalarioInicial,
  };
}

export async function inserirPessoaController(req: Request, res: Response) {
  let eventId: string | null = null;
  const source = '/api/Pessoa/InserirPessoa';

  try {
    // Log do body original antes da normalização
    logger.info(
      {
        bodyKeys: Object.keys(req.body || {}),
        temDadosBancarioListOriginal: !!(
          req.body?.DadosBancarioList || req.body?.dadosBancarioList
        ),
        dadosBancarioListOriginal: req.body?.DadosBancarioList || req.body?.dadosBancarioList,
        dadosBancarioListLength:
          (req.body?.DadosBancarioList || req.body?.dadosBancarioList)?.length || 0,
      },
      'Body recebido antes da normalização',
    );

    // Normalizar payload de camelCase para PascalCase
    req.body = normalizePessoaPayload(req.body);

    // Log após normalização
    logger.info(
      {
        temDadosBancarioListNormalizado: !!req.body.DadosBancarioList,
        dadosBancarioListNormalizado: req.body.DadosBancarioList,
        dadosBancarioListLengthNormalizado: req.body.DadosBancarioList?.length || 0,
      },
      'Body após normalização',
    );

    // Gerar eventId baseado nos dados disponíveis
    const codPessoaEsl = req.body?.CodPessoaEsl;
    eventId = generatePessoaEventId(codPessoaEsl);

    // Validar schema (fazer primeiro, antes de qualquer operação de banco)
    const data = pessoaSchema.parse(req.body);

    logger.info(
      { codPessoaEsl: data.CodPessoaEsl, cpf: data.Cpf, cnpj: data.Cnpj },
      'Recebida requisição para inserir Pessoa',
    );

    // Criar/atualizar eventos em background (não bloqueia)
    createOrUpdateWebhookEvent(eventId, source, 'pending', null, {
      codPessoaEsl: codPessoaEsl || null,
      etapa: 'validacao',
    }).catch((err: any) =>
      logger.warn({ error: err?.message }, 'Erro ao criar evento pending (não crítico)'),
    );

    createOrUpdateWebhookEvent(eventId, source, 'processing', null, {
      codPessoaEsl: data.CodPessoaEsl || null,
      etapa: 'processamento',
    }).catch((err: any) =>
      logger.warn({ error: err?.message }, 'Erro ao criar evento processing (não crítico)'),
    );

    // Processar inserção/alteração
    const resultado: {
      status: boolean;
      mensagem: string;
      codPessoa?: number;
      tabelasInseridas?: string[];
      tabelasFalhadas?: Array<{ tabela: string; erro: string }>;
      created?: boolean; // Indica se foi criado (true) ou atualizado (false)
    } = await inserirPessoa(data);

    if (resultado.status) {
      // Retornar resposta IMEDIATAMENTE após processar pessoa
      // Operações de atualização de eventos serão feitas em background
      const responseData = {
        Status: resultado.status,
        Mensagem: resultado.mensagem,
      };

      // Preparar dados para atualização de eventos em background
      const finalEventId = resultado.codPessoa ? `pessoa-${resultado.codPessoa}` : eventId;

      const temFalhas = resultado.tabelasFalhadas && resultado.tabelasFalhadas.length > 0;
      const tabelasInseridas = resultado.tabelasInseridas || [];
      const tabelasFalhadas = resultado.tabelasFalhadas || [];

      const metadata: any = {
        codPessoa: resultado.codPessoa || null,
        codPessoaEsl: data.CodPessoaEsl || null,
        etapa: 'backend_concluido',
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
        const tabelasOk =
          tabelasInseridas.length > 0
            ? `Tabelas inseridas com sucesso: ${tabelasInseridas.join(', ')}. `
            : '';
        const tabelasErro = `Tabelas com erro: ${tabelasFalhadas.map((t: { tabela: string; erro: string }) => `${t.tabela} (${t.erro})`).join(', ')}.`;
        mensagemErro = tabelasOk + tabelasErro;
      }

      // Determinar código HTTP: 201 Created para novos registros, 200 OK para atualizações
      const httpStatus = resultado.created === true ? 201 : 200;

      // Enviar resposta imediatamente
      res.status(httpStatus).json(responseData);

      // Atualizar eventos em background (não bloqueia a resposta)
      (async () => {
        try {
          // Se o eventId mudou, migrar o evento antigo para o novo
          if (finalEventId !== eventId && resultado.codPessoa) {
            try {
              const oldEvent = await prisma.webhookEvent.findUnique({ where: { id: eventId } });
              if (oldEvent) {
                await prisma.webhookEvent.upsert({
                  where: { id: finalEventId },
                  create: {
                    id: finalEventId,
                    source: oldEvent.source,
                    receivedAt: oldEvent.receivedAt,
                    status: 'processed',
                    errorMessage: mensagemErro,
                    retryCount: oldEvent.retryCount || 0,
                    metadata: JSON.stringify(metadata).substring(0, 2000),
                    processedAt: new Date(),
                    integrationStatus: temFalhas ? 'partial' : 'integrated',
                    seniorId: resultado.codPessoa ? String(resultado.codPessoa) : null,
                    tipoIntegracao: 'Web API',
                  },
                  update: {
                    status: 'processed',
                    errorMessage: mensagemErro,
                    metadata: JSON.stringify(metadata).substring(0, 2000),
                    processedAt: new Date(),
                    integrationStatus: temFalhas ? 'partial' : 'integrated',
                    seniorId: resultado.codPessoa ? String(resultado.codPessoa) : null,
                  },
                });
                await prisma.webhookEvent.delete({ where: { id: eventId } }).catch(() => {});
                logger.debug(
                  { oldEventId: eventId, newEventId: finalEventId, codPessoa: resultado.codPessoa },
                  'Evento migrado para eventId com codPessoa',
                );
              }
            } catch (migrateError: any) {
              logger.warn(
                { error: migrateError?.message, oldEventId: eventId, newEventId: finalEventId },
                'Erro ao migrar evento, usando eventId original',
              );
            }
          }

          // Atualizar evento como processado
          await createOrUpdateWebhookEvent(
            finalEventId,
            source,
            'processed',
            mensagemErro,
            metadata,
          );
        } catch (bgError: any) {
          logger.error(
            { error: bgError?.message, eventId: finalEventId, codPessoaEsl: data.CodPessoaEsl },
            'Erro ao atualizar eventos em background (não crítico)',
          );
        }
      })();

      return;
    } else {
      // Atualizar evento como falha
      await createOrUpdateWebhookEvent(eventId, source, 'failed', resultado.mensagem, {
        codPessoaEsl: data.CodPessoaEsl || null,
        etapa: 'backend_falha',
        mensagem: resultado.mensagem,
      });

      return res.status(400).json({
        Status: resultado.status,
        Mensagem: resultado.mensagem,
      });
    }
  } catch (error: any) {
    // Erro de validação Zod
    if (error.name === 'ZodError') {
      logger.warn({ errors: error.errors }, 'Erro de validação no schema Pessoa');

      if (eventId) {
        await createOrUpdateWebhookEvent(
          eventId,
          source,
          'failed',
          'Dados inválidos - Erro de validação',
          {
            codPessoaEsl: req.body?.CodPessoaEsl || req.body?.codPessoaEsl || null,
            etapa: 'validacao_falha',
            erros: error.errors,
          },
        );
      }

      return res.status(400).json({
        Status: false,
        Mensagem: 'Dados inválidos',
        Erros: error.errors,
      });
    }

    // Detectar tipo de erro específico do Prisma
    let mensagemErro: string;
    let tabela: string | null = null;

    if (error.message?.includes('does not exist in the current database')) {
      // Extrair nome da tabela/modelo do erro
      const match =
        error.message.match(/column `(\w+)` does not exist.*model `(\w+)`/i) ||
        error.message.match(/The column `(\w+)` does not exist/i) ||
        error.message.match(/model `(\w+)`/i);

      if (match) {
        tabela = match[2] || match[1] || 'desconhecida';
        const coluna = match[1] || 'id';
        mensagemErro = `Erro de esquema do banco de dados: A coluna '${coluna}' não existe na tabela/modelo '${tabela}'. Verifique se o schema do Prisma está sincronizado com a estrutura real do banco de dados.`;
      } else {
        mensagemErro = `Erro de esquema do banco de dados: Coluna não encontrada. Verifique se o schema do Prisma está sincronizado com a estrutura real do banco de dados.`;
      }
    } else if (error.code === 'P2021' || error.message?.includes('does not exist')) {
      tabela = error.meta?.target?.[0] || 'desconhecida';
      mensagemErro = `Tabela não encontrada: A tabela '${tabela}' não existe no banco de dados.`;
    } else {
      mensagemErro = error.message || 'Erro desconhecido';
    }

    logger.error(
      {
        error: error.message,
        errorCode: error.code,
        errorName: error.name,
        stack: error.stack,
        tabela,
        codPessoaEsl: req.body?.CodPessoaEsl || req.body?.codPessoaEsl || null,
      },
      'Erro ao processar Pessoa',
    );

    if (eventId) {
      await createOrUpdateWebhookEvent(eventId, source, 'failed', mensagemErro, {
        codPessoaEsl: req.body?.CodPessoaEsl || req.body?.codPessoaEsl || null,
        etapa: 'erro_interno',
        erro: mensagemErro,
        errorCode: error.code,
        errorName: error.name,
        tabela: tabela || null,
      });
    }

    return res.status(500).json({
      Status: false,
      Mensagem: mensagemErro,
      ...(process.env.NODE_ENV === 'development' && { detalhes: error.message, tabela }),
    });
  }
}
