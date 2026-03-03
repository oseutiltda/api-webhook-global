import { PrismaClient } from '@prisma/client';
import { logger } from '../utils/logger';
import { env } from '../config/env';
import { buildBypassMetadata, isPostgresSafeMode } from '../utils/integrationMode';
import type { Pessoa } from '../schemas/pessoa';

const prisma = new PrismaClient();
const IS_POSTGRES = env.DATABASE_URL.startsWith('postgresql://');

// Map para rastrear pessoas que estão sendo processadas (lock de processamento)
// Evita processamento duplicado quando múltiplas requisições chegam simultaneamente
// Lock baseado em CodPessoaEsl (string) para funcionar antes de obter codPessoa
const pessoasEmProcessamentoEsl = new Map<
  string,
  Promise<{
    status: boolean;
    mensagem: string;
    codPessoa?: number;
    tabelasInseridas?: string[];
    tabelasFalhadas?: Array<{ tabela: string; erro: string }>;
  }>
>();
// Lock baseado em codPessoa (number) para operações em background
const pessoasEmProcessamento = new Map<number, Promise<void>>();

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

/**
 * Normaliza CPF/CNPJ para o formato esperado pelo Senior
 * NOTA: O Senior pode esperar CPF com 14 dígitos (zeros à esquerda) em alguns campos (CdInscricao),
 * mas CPF deve ter 11 dígitos em campos específicos como @NrCPF
 * @param documento CPF ou CNPJ (com ou sem formatação)
 * @param tipoPessoa 1 = Pessoa Física (CPF), 2 = Pessoa Jurídica (CNPJ)
 * @param tamanhoPadrao Tamanho padrão para completar (14 para CdInscricao, 11 para CPF específico)
 * @returns Documento normalizado
 */
const normalizeDocumentoSenior = (
  documento: string | null | undefined,
  tipoPessoa: number,
  tamanhoPadrao: number = 14,
): string => {
  if (!documento) return '';
  // Remove formatação (pontos, hífens, barras, espaços)
  const documentoLimpo = documento.replace(/\D/g, '');

  if (tipoPessoa === 1) {
    // CPF: Para CdInscricao usa 14 dígitos (padrão do Senior), para campos específicos usa 11
    // Ex: "12345678901" → "00012345678901" (14 dígitos) ou "12345678901" (11 dígitos)
    return tamanhoPadrao === 11
      ? documentoLimpo.padStart(11, '0')
      : documentoLimpo.padStart(14, '0');
  } else {
    // CNPJ: Sempre 14 dígitos
    return documentoLimpo.padStart(14, '0');
  }
};

// Helper para converter string de data ou Date para formato SQL datetime
const parseDate = (dateInput: string | Date | null | undefined): string => {
  if (!dateInput) return 'NULL';
  try {
    const date = dateInput instanceof Date ? dateInput : new Date(dateInput);
    if (isNaN(date.getTime())) return 'NULL';
    // Formato SQL Server datetime: 'YYYY-MM-DD HH:MM:SS'
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    const seconds = String(date.getSeconds()).padStart(2, '0');
    return `'${year}-${month}-${day} ${hours}:${minutes}:${seconds}'`;
  } catch {
    return 'NULL';
  }
};

// Helper específico para data de nascimento - considera timezone UTC-3 do SQL Server
// Extrai apenas a data (sem hora) para evitar problemas de timezone
// Para data de nascimento, sempre usamos apenas YYYY-MM-DD, ignorando hora e timezone
const parseDateNascimento = (dateInput: string | Date | null | undefined): string => {
  if (!dateInput) return 'NULL';
  try {
    // Se for string ISO com T (formato ISO 8601), extrair apenas a parte da data
    if (typeof dateInput === 'string' && dateInput.includes('T')) {
      // Extrair apenas YYYY-MM-DD da string ISO (antes do T)
      const datePart = dateInput.split('T')[0];
      if (datePart) {
        const [year, month, day] = datePart.split('-');

        if (year && month && day && year.length === 4) {
          // Usar apenas a parte da data, ignorando hora e timezone
          // Isso garante que a data não será afetada pelo timezone
          return `'${year}-${month}-${day} 00:00:00'`;
        }
      }
    }

    // Se for string no formato YYYY-MM-DD (sem hora)
    if (typeof dateInput === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(dateInput.trim())) {
      return `'${dateInput.trim()} 00:00:00'`;
    }

    // Se for Date ou outra string, converter e extrair apenas a data
    const date = dateInput instanceof Date ? dateInput : new Date(dateInput);
    if (isNaN(date.getTime())) return 'NULL';

    // Para data de nascimento, sempre usar apenas a data (sem hora)
    // Como o SQL Server está em UTC-3, precisamos ajustar:
    // Se a data vem em UTC e queremos manter a data correta em UTC-3,
    // extraímos apenas a parte da data sem considerar o timezone
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');

    return `'${year}-${month}-${day} 00:00:00'`;
  } catch {
    return 'NULL';
  }
};

const shouldBypassPessoaLegacyFlow = (): boolean => {
  return isPostgresSafeMode();
};

const extractCodPessoaFromPayload = (payload: string | null): number | undefined => {
  if (!payload) return undefined;
  try {
    const parsed = JSON.parse(payload) as Record<string, unknown>;
    const candidate = parsed.CodPessoa ?? parsed.codPessoa;
    const numeric = Number(candidate);
    return Number.isFinite(numeric) && numeric > 0 ? numeric : undefined;
  } catch {
    return undefined;
  }
};

const buscarPessoaLocal = async (
  codPessoaEsl: string,
): Promise<{ existe: boolean; codPessoa?: number }> => {
  const pessoaLocal = await prisma.pessoa.findUnique({
    where: { id: codPessoaEsl },
    select: { id: true, payload: true },
  });

  if (!pessoaLocal) {
    return { existe: false };
  }

  const codPessoa = extractCodPessoaFromPayload(pessoaLocal.payload);
  return codPessoa ? { existe: true, codPessoa } : { existe: true };
};

const toPessoaPayloadText = (pessoa: Pessoa, codPessoa?: number): string => {
  const payload = {
    ...pessoa,
    codPessoa: codPessoa ?? pessoa.CodPessoa ?? null,
    atualizadoEm: new Date().toISOString(),
    origem: 'migracao_postgres',
  };
  return JSON.stringify(payload).substring(0, 4000);
};

/**
 * Verifica se a pessoa existe pelo CodPessoaEsl e retorna o CodPessoa se existir
 * Usa consulta direta à tabela para garantir que não haja duplicação
 */
export async function verificaExistenciaPessoa(
  codPessoaEsl: string,
): Promise<{ existe: boolean; codPessoa?: number }> {
  try {
    if (IS_POSTGRES) {
      const local = await buscarPessoaLocal(codPessoaEsl);
      logger.debug(
        { codPessoaEsl, ...local },
        'Verificação de Pessoa em modo PostgreSQL (consulta local)',
      );
      return local;
    }

    // Primeiro, tentar buscar diretamente na tabela para garantir precisão
    const sqlDireta = `
      SELECT TOP 1 CodPessoa
      FROM [AFS_INTEGRADOR].[dbo].[Pessoa]
      WHERE CodPessoaEsl = ${toSqlValue(codPessoaEsl)}
      ORDER BY CodPessoa DESC;
    `;

    const resultDireta = await prisma.$queryRawUnsafe<Array<{ CodPessoa: number }>>(sqlDireta);

    if (resultDireta && resultDireta.length > 0 && resultDireta[0]?.CodPessoa) {
      const codPessoa = Number(resultDireta[0].CodPessoa);
      logger.debug({ codPessoaEsl, codPessoa }, 'Pessoa encontrada na tabela pelo CodPessoaEsl');
      return { existe: true, codPessoa };
    }

    // Fallback: usar stored procedure se a consulta direta não retornou nada
    const sql = `
      EXEC dbo.P_VERIFICA_EXISTE_PESSOA_ESL
        @CodPessoaEsl = ${toSqlValue(codPessoaEsl)};
    `;

    const result = await prisma.$queryRawUnsafe<Array<any>>(sql);
    if (result && result.length > 0) {
      // Se a stored procedure retornou algo, buscar o CodPessoa
      const codPessoa = await obterCodigoPessoa(codPessoaEsl);
      if (codPessoa > 0) {
        return { existe: true, codPessoa };
      }
      return { existe: true };
    }

    return { existe: false };
  } catch (error: any) {
    logger.error({ error: error.message, codPessoaEsl }, 'Erro ao verificar existência da pessoa');
    return { existe: false };
  }
}

/**
 * Obtém o código da pessoa pelo CodPessoaEsl
 */
/**
 * Busca pessoas pendentes de integração com Senior (FlgIntegradoSenior = 0)
 * Baseado no código C# original: ObterPessoaParaIntegracao() -> P_INTEGRACAO_PESSOA_LISTAR
 */
async function obterPessoasParaIntegracao(): Promise<Array<{ CodPessoa: number }>> {
  try {
    const sql = `EXEC dbo.P_INTEGRACAO_PESSOA_LISTAR`;
    const result = await prisma.$queryRawUnsafe<Array<{ CodPessoa: number }>>(sql);
    return result || [];
  } catch (error: any) {
    logger.error({ error: error.message }, 'Erro ao buscar pessoas para integração');
    return [];
  }
}

/**
 * Marca pessoa como integrada com Senior (FlgIntegradoSenior = 1)
 * Baseado no código C# original: AlterarIntegracaoPessoa() -> P_DADOS_PESSOA_ESL_ALTERAR_INTEGRACAO
 */
async function alterarIntegracaoPessoa(codPessoa: number): Promise<void> {
  try {
    const sql = `
      EXEC dbo.P_DADOS_PESSOA_ESL_ALTERAR_INTEGRACAO
        @CodPessoa = ${codPessoa};
    `;
    await prisma.$executeRawUnsafe(sql);
    logger.info({ codPessoa }, 'Pessoa marcada como integrada com Senior');
  } catch (error: any) {
    logger.error({ error: error.message, codPessoa }, 'Erro ao marcar pessoa como integrada');
    throw error;
  }
}

/**
 * Interface para dados completos da pessoa para integração Senior
 */
interface PessoaIntegracaoData {
  // Dados principais
  CodPessoa: number;
  CodPessoaEsl: string;
  CodTipoPessoa: number;
  CodNaturezaOperacao: number | null;
  DescNaturezaOperacao: string | null;
  CodFilialResponsavel: number | null;
  DescFilialResponsavel: string | null;
  NomeRazaoSocial: string;
  NomeFantasia: string | null;
  DataFundacao: string | null;
  Cpf: string | null;
  Cnpj: string | null;
  RG: string | null;
  CidadeExpedicaoRG: string | null;
  OrgaoExpedidorRG: string | null;
  DataEmissaoRG: string | null;
  Sexo: boolean;
  EstadoCivil: string | null;
  DataNascimento: string | null;
  CidadeNascimento: string | null;
  InscricaoMunicipal: string | null;
  InscricaoEstadual: string | null;
  DocumentoExterior: string | null;
  NumeroPISPASEP: string | null;
  RNTRC: string | null;
  DataValidadeRNTRC: string | null;
  NomePai: string | null;
  NomeMae: string | null;
  Contrinuinte: boolean;
  Site: string | null;
  CodigoCNAE: string | null;
  RegimeFiscal: string | null;
  CodigoSuframa: string | null;
  Observacao: string | null;
  DataCadastro: string | null;
  CodUsuarioCadastro: number | null;
  UsuarioCadastro: string | null;
  Ativo: boolean;
  // Relacionados
  Personagens: Array<{ CodPersonagem: number; DescPersonagem: string }>;
  Contatos: Array<any>;
  Enderecos: Array<any>;
  DadosBancarios: Array<any>;
  ChavesPix: Array<any>;
  Motorista: any | null;
  Funcionario: any | null;
  cdFuncionario: number | null;
}

/**
 * Busca dados completos da pessoa para integração Senior
 * Baseado no código C# original: ObterPessoaParaIntegracao() -> P_INTEGRACAO_PESSOA_LISTAR
 */
async function obterDadosCompletosPessoa(codPessoa: number): Promise<PessoaIntegracaoData | null> {
  try {
    logger.debug({ codPessoa }, 'Buscando dados completos da pessoa usando stored procedures');

    // Usar stored procedure P_INTEGRACAO_PESSOA_LISTAR e filtrar por CodPessoa
    // Baseado no código C# original que usa essa stored procedure
    const sqlPessoa = `EXEC dbo.P_INTEGRACAO_PESSOA_LISTAR`;

    logger.debug({ codPessoa }, 'Executando P_INTEGRACAO_PESSOA_LISTAR');

    const todasPessoas = await prisma.$queryRawUnsafe<
      Array<{
        CodPessoa: number;
        CodPessoaEsl: string;
        CodTipoPessoa: number;
        CodNaturezaOperacao: number | null;
        DescNaturezaOperacao: string | null;
        CodFilialResponsavel: number | null;
        DescFilialResponsavel: string | null;
        NomeRazaoSocial: string;
        NomeFantasia: string | null;
        DataFundacao: Date | string | null;
        Cpf: string | null;
        Cnpj: string | null;
        RG: string | null;
        CidadeExpedicaoRG: string | null;
        OrgaoExpedidorRG: string | null;
        DataEmissaoRG: Date | string | null;
        Sexo: boolean;
        EstadoCivil: string | null;
        DataNascimento: Date | string | null;
        CidadeNascimento: string | null;
        InscricaoMunicipal: string | null;
        InscricaoEstadual: string | null;
        DocumentoExterior: string | null;
        PISPASEP: string | null;
        RNTRC: string | null;
        ValidadeRNTRC: Date | string | null;
        CodCNAE: string | null;
        CodSuframa: string | null;
        NomePai: string | null;
        NomeMae: string | null;
        Contrinuinte: boolean;
        Site: string | null;
        RegimeFiscal: string | null;
        Observacao: string | null;
        DataCadastro: Date | string | null;
        CodUsuarioCadastro: number | null;
        UsuarioCadastro: string | null;
        Ativo: boolean;
      }>
    >(sqlPessoa);

    // Filtrar por CodPessoa
    const pessoaData = todasPessoas?.filter((p) => p.CodPessoa === codPessoa) || [];

    if (!pessoaData || pessoaData.length === 0) {
      return null;
    }

    const pessoa = pessoaData[0];
    if (!pessoa) {
      return null;
    }

    // Converter datas para string se necessário
    const formatDate = (date: Date | string | null | undefined): string | null => {
      if (!date) return null;
      if (typeof date === 'string') return date;
      if (date instanceof Date) {
        if (isNaN(date.getTime())) return null;
        const dateStr = date.toISOString().split('T')[0];
        return dateStr || null;
      }
      return null;
    };

    // Buscar personagens usando stored procedure
    logger.debug({ codPessoa }, 'Buscando personagens da pessoa');
    let personagens: Array<{ CodPersonagem: number; DescPersonagem: string }> = [];
    try {
      const sqlPersonagens = `EXEC dbo.P_INTEGRACAO_PESSOA_PERSONAGENS_LISTAR @CodPessoa = ${codPessoa}`;
      personagens =
        (await prisma.$queryRawUnsafe<
          Array<{
            CodPersonagem: number;
            DescPersonagem: string;
          }>
        >(sqlPersonagens)) || [];
    } catch (error: any) {
      logger.warn(
        { error: error.message, codPessoa },
        'Erro ao buscar personagens, tentando query direta',
      );
      // Fallback para query direta
      personagens =
        (await prisma.$queryRawUnsafe<
          Array<{
            CodPersonagem: number;
            DescPersonagem: string;
          }>
        >(`
        SELECT 
          pp.CodPersonagem,
          per.Descricao as DescPersonagem
        FROM [AFS_INTEGRADOR].[dbo].[PessoaPersonagem] pp
        INNER JOIN [AFS_INTEGRADOR].[dbo].[Personagem] per ON pp.CodPersonagem = per.CodPersonagem
        WHERE pp.CodPessoa = ${codPessoa}
      `)) || [];
    }
    logger.debug({ codPessoa, totalPersonagens: personagens?.length || 0 }, 'Personagens buscados');

    // Buscar contatos usando stored procedure
    let contatos: Array<any> = [];
    try {
      const sqlContatos = `EXEC dbo.P_INTEGRACAO_PESSOA_CONTATO_LISTAR @CodPessoa = ${codPessoa}`;
      contatos = (await prisma.$queryRawUnsafe<Array<any>>(sqlContatos)) || [];
    } catch (error: any) {
      logger.warn(
        { error: error.message, codPessoa },
        'Erro ao buscar contatos, tentando query direta',
      );
      // Fallback para query direta
      contatos =
        (await prisma.$queryRawUnsafe<Array<any>>(`
        SELECT 
          c.CodContato,
          c.CodPessoa,
          c.CodTipoContato,
          c.Nome,
          c.Cpf,
          c.DDD,
          c.Telefone,
          c.Celular,
          c.Departamento,
          c.Email,
          c.Observacao
        FROM [AFS_INTEGRADOR].[dbo].[Contato] c
        WHERE c.CodPessoa = ${codPessoa}
      `)) || [];
    }

    // Buscar endereços usando stored procedure
    let enderecos: Array<any> = [];
    try {
      const sqlEnderecos = `EXEC dbo.P_INTEGRACAO_PESSOA_ENDERECO_LISTAR @CodPessoa = ${codPessoa}`;
      enderecos = (await prisma.$queryRawUnsafe<Array<any>>(sqlEnderecos)) || [];
    } catch (error: any) {
      logger.warn(
        { error: error.message, codPessoa },
        'Erro ao buscar endereços, tentando query direta',
      );
      // Fallback para query direta
      enderecos =
        (await prisma.$queryRawUnsafe<Array<any>>(`
        SELECT 
          e.CodEndereco,
          e.CodPessoa,
          e.CodTipoEndereco,
          e.Cep,
          e.Logradouro,
          e.Numero,
          e.Complemento,
          e.Bairro,
          e.Cidade,
          e.Estado
        FROM [AFS_INTEGRADOR].[dbo].[Endereco] e
        WHERE e.CodPessoa = ${codPessoa}
      `)) || [];
    }

    // Buscar dados bancários usando stored procedure
    let dadosBancarios: Array<any> = [];
    try {
      const sqlDadosBancarios = `EXEC dbo.P_INTEGRACAO_PESSOA_DADOS_BANCARIO_LISTAR @CodPessoa = ${codPessoa}`;
      dadosBancarios = (await prisma.$queryRawUnsafe<Array<any>>(sqlDadosBancarios)) || [];
    } catch (error: any) {
      logger.warn(
        { error: error.message, codPessoa },
        'Erro ao buscar dados bancários, tentando query direta',
      );
      // Fallback para query direta
      dadosBancarios =
        (await prisma.$queryRawUnsafe<Array<any>>(`
        SELECT 
          db.CodDadosBancario,
          db.CodPessoa,
          db.CodBanco,
          db.Codigo,
          db.CodTipoConta,
          db.CodTitularidade,
          db.NumAgencia,
          db.NumConta,
          db.NomeTitular,
          db.CpfCnpj,
          b.Codigo as BancoCodigo,
          b.Nome as BancoNome
        FROM [AFS_INTEGRADOR].[dbo].[DadosBancario] db
        LEFT JOIN [AFS_INTEGRADOR].[dbo].[Banco] b ON db.CodBanco = b.CodBanco
        WHERE db.CodPessoa = ${codPessoa}
      `)) || [];
    }

    // Buscar chaves PIX usando stored procedure
    let chavesPix: Array<any> = [];
    try {
      const sqlChavesPix = `EXEC dbo.P_INTEGRACAO_PESSOA_CHAVE_PIX_LISTAR @CodPessoa = ${codPessoa}`;
      chavesPix = (await prisma.$queryRawUnsafe<Array<any>>(sqlChavesPix)) || [];
    } catch (error: any) {
      logger.warn(
        { error: error.message, codPessoa },
        'Erro ao buscar chaves PIX, tentando query direta',
      );
      // Fallback para query direta
      chavesPix =
        (await prisma.$queryRawUnsafe<Array<any>>(`
        SELECT 
          cp.CodChavesPix,
          cp.CodPessoa,
          cp.CodTipoChave,
          cp.ChavePix,
          cp.NomeTitular,
          tc.Descricao as TipoChaveDescricao
        FROM [AFS_INTEGRADOR].[dbo].[ChavesPix] cp
        LEFT JOIN [AFS_INTEGRADOR].[dbo].[TipoChave] tc ON cp.CodTipoChave = tc.CodTipoChave
        WHERE cp.CodPessoa = ${codPessoa}
      `)) || [];
    }

    // Buscar motorista usando stored procedure
    let motorista: any | null = null;
    try {
      const sqlMotorista = `EXEC dbo.P_INTEGRACAO_PESSOA_MOTORISTA_OBTER @CodPessoa = ${codPessoa}`;
      const motoristaResult = await prisma.$queryRawUnsafe<Array<any>>(sqlMotorista);
      motorista = motoristaResult && motoristaResult.length > 0 ? motoristaResult[0] : null;
    } catch (error: any) {
      logger.warn(
        { error: error.message, codPessoa },
        'Erro ao buscar motorista, tentando query direta',
      );
      // Fallback para query direta
      const motoristaResult =
        (await prisma.$queryRawUnsafe<Array<any>>(`
        SELECT TOP 1
          pm.CodPessoaMotorista,
          pm.CodPessoa,
          pm.NumeroCNH,
          pm.NumeroRegistro,
          pm.DataPrimeiraCNH as DataPrimeiraCNH,
          pm.DataEmissaoCNH as DataEmissaoCNH,
          pm.DataValidadeCNH as DataVencimentoCNH,
          pm.TipoCNH,
          pm.CidadeCNH,
          pm.CodSegurancaCNH,
          pm.NumeroRenach
        FROM [AFS_INTEGRADOR].[dbo].[PessoaMotorista] pm
        WHERE pm.CodPessoa = ${codPessoa}
      `)) || [];
      motorista = motoristaResult.length > 0 ? motoristaResult[0] : null;
    }

    // Buscar funcionário usando stored procedure
    let funcionario: any | null = null;
    try {
      const sqlFuncionario = `EXEC dbo.P_INTEGRACAO_PESSOA_FUNCIONARIO_OBTER @CodPessoa = ${codPessoa}`;
      const funcionarioResult = await prisma.$queryRawUnsafe<Array<any>>(sqlFuncionario);
      funcionario = funcionarioResult && funcionarioResult.length > 0 ? funcionarioResult[0] : null;
    } catch (error: any) {
      logger.warn(
        { error: error.message, codPessoa },
        'Erro ao buscar funcionário, tentando query direta',
      );
      // Fallback para query direta
      const funcionarioResult =
        (await prisma.$queryRawUnsafe<Array<any>>(`
        SELECT TOP 1
          pf.CodPessoaFuncionario,
          pf.CodPessoa,
          pf.CdFuncionario,
          pf.DsNumCtps,
          pf.DsSerieCtps,
          pf.DsUfCtps,
          pf.DtAdmissao,
          pf.VlSalarioAtual,
          pf.DsCorPele,
          pf.DtEmissaoCTPS,
          pf.VlSalarioInicial
        FROM [AFS_INTEGRADOR].[dbo].[PessoaFuncionario] pf
        WHERE pf.CodPessoa = ${codPessoa}
      `)) || [];
      funcionario = funcionarioResult.length > 0 ? funcionarioResult[0] : null;
    }

    return {
      CodPessoa: pessoa.CodPessoa,
      CodPessoaEsl: pessoa.CodPessoaEsl,
      CodTipoPessoa: pessoa.CodTipoPessoa,
      CodNaturezaOperacao: pessoa.CodNaturezaOperacao,
      DescNaturezaOperacao: pessoa.DescNaturezaOperacao,
      CodFilialResponsavel: pessoa.CodFilialResponsavel,
      DescFilialResponsavel: pessoa.DescFilialResponsavel,
      NomeRazaoSocial: pessoa.NomeRazaoSocial,
      NomeFantasia: pessoa.NomeFantasia,
      DataFundacao: formatDate(pessoa.DataFundacao),
      Cpf: pessoa.Cpf,
      Cnpj: pessoa.Cnpj,
      RG: pessoa.RG,
      CidadeExpedicaoRG: pessoa.CidadeExpedicaoRG,
      OrgaoExpedidorRG: pessoa.OrgaoExpedidorRG,
      DataEmissaoRG: formatDate(pessoa.DataEmissaoRG),
      Sexo: pessoa.Sexo,
      EstadoCivil: pessoa.EstadoCivil,
      DataNascimento: formatDate(pessoa.DataNascimento),
      CidadeNascimento: pessoa.CidadeNascimento,
      InscricaoMunicipal: pessoa.InscricaoMunicipal,
      InscricaoEstadual: pessoa.InscricaoEstadual,
      DocumentoExterior: pessoa.DocumentoExterior,
      NumeroPISPASEP: pessoa.PISPASEP || null,
      RNTRC: pessoa.RNTRC || null,
      DataValidadeRNTRC: formatDate(pessoa.ValidadeRNTRC),
      NomePai: pessoa.NomePai,
      NomeMae: pessoa.NomeMae,
      Contrinuinte: pessoa.Contrinuinte,
      Site: pessoa.Site,
      CodigoCNAE: pessoa.CodCNAE || null,
      RegimeFiscal: pessoa.RegimeFiscal || null,
      CodigoSuframa: pessoa.CodSuframa || null,
      Observacao: pessoa.Observacao,
      DataCadastro: formatDate(pessoa.DataCadastro) ?? null,
      CodUsuarioCadastro: pessoa.CodUsuarioCadastro,
      UsuarioCadastro: pessoa.UsuarioCadastro,
      Ativo: pessoa.Ativo,
      Personagens: personagens || [],
      Contatos: contatos || [],
      Enderecos: enderecos || [],
      DadosBancarios: dadosBancarios || [],
      ChavesPix: chavesPix || [],
      Motorista: motorista,
      Funcionario: funcionario,
      cdFuncionario: funcionario && funcionario.CdFuncionario ? funcionario.CdFuncionario : null,
    };
  } catch (error: any) {
    logger.error({ error: error.message, codPessoa }, 'Erro ao buscar dados completos da pessoa');
    throw error;
  }
}

/**
 * Retorna código da empresa baseado no CNPJ
 * Baseado no código C# original: RetornaCodEmpresa()
 */
async function retornaCodEmpresa(descFilialResponsavel: string | null): Promise<number> {
  try {
    if (!descFilialResponsavel) return 300; // Default

    const sql = `
      EXEC dbo.P_EMPRESA_SENIOR_POR_CNPJ_LISTAR
        @Cnpj = ${toSqlValue(descFilialResponsavel)};
    `;

    const result = await prisma.$queryRawUnsafe<Array<{ codEmpresa: number }>>(sql);
    if (result && result.length > 0 && result[0]?.codEmpresa) {
      return result[0].codEmpresa;
    }
    return 300; // Default
  } catch (error: any) {
    logger.warn(
      { error: error.message, descFilialResponsavel },
      'Erro ao buscar código da empresa, usando default 300',
    );
    return 300;
  }
}

/**
 * Retorna próximo código de contato
 * Baseado no código C# original: RetornaProximoContato()
 */
async function retornaProximoContato(cdInscricao: string): Promise<number> {
  try {
    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_RETORNA_PROXIMO_CONTATO
        @CdInscricao = ${toSqlValue(cdInscricao)};
    `;

    const result = await prisma.$queryRawUnsafe<Array<{ CdContato: number }>>(sql);
    if (result && result.length > 0 && result[0]?.CdContato) {
      return result[0].CdContato;
    }
    return 0;
  } catch (error: any) {
    logger.warn({ error: error.message, cdInscricao }, 'Erro ao buscar próximo contato');
    return 0;
  }
}

/**
 * Retorna próximo código PIX
 * Baseado no código C# original: RetornaProximoCdPix()
 */
async function retornaProximoCdPix(): Promise<number> {
  try {
    const sql = `EXEC dbo.P_NR_SEQ_CONTROLE_MAX_PIX_OBTER`;
    const result = await prisma.$queryRawUnsafe<Array<{ CdPIX: number | null }>>(sql);
    if (
      result &&
      result.length > 0 &&
      result[0]?.CdPIX !== null &&
      result[0]?.CdPIX !== undefined
    ) {
      return result[0].CdPIX + 1; // Incrementar o próximo
    }
    return 1; // Primeiro código
  } catch (error: any) {
    logger.warn(
      { error: error.message },
      'Erro ao buscar próximo código PIX, usando 1 como padrão',
    );
    return 1;
  }
}

/**
 * Processa a integração de uma pessoa específica com Senior
 * Baseado no código C# original: InserirDadosPessoaSISCLI()
 * Esta função decide qual tipo de inserção fazer baseado nos personagens da pessoa
 * e chama as stored procedures apropriadas para inserir nas tabelas da Senior
 */
async function inserirDadosPessoaSISCLI(codPessoa: number): Promise<void> {
  try {
    // Buscar dados completos da pessoa
    const pessoaData = await obterDadosCompletosPessoa(codPessoa);

    if (!pessoaData) {
      logger.warn({ codPessoa }, 'Pessoa não encontrada para integração');
      return;
    }

    // Determinar tipo de integração baseado nos personagens
    const ehMotorista = pessoaData.Personagens.some((p) => p.DescPersonagem === 'Motorista');
    const ehAgregado = pessoaData.Personagens.some((p) => p.DescPersonagem === 'Agregado');
    const ehFuncionario = pessoaData.Personagens.some(
      (p) => p.DescPersonagem === 'Funcionario' || p.DescPersonagem === 'Funcionário',
    );

    logger.info(
      {
        codPessoa,
        ehMotorista,
        ehAgregado,
        ehFuncionario,
        personagens: pessoaData.Personagens.map((p) => p.DescPersonagem),
      },
      'Determinando tipo de integração Senior',
    );

    // Lógica baseada no código C# original:
    if (ehMotorista && ehAgregado) {
      // Motorista e Agregado - entra no freteiro
      await insereMotoristaAgregado(pessoaData);
    } else if (ehMotorista && ehFuncionario) {
      // Motorista e Funcionario - entra no funcionario
      await insereMotoristaFuncionario(pessoaData);
    } else if (ehMotorista) {
      // Motorista sozinho (autônomo) - entra no freteiro
      logger.info({ codPessoa }, 'Motorista autônomo detectado, inserindo como freteiro');
      await insereMotoristaAgregado(pessoaData);
    } else if (ehAgregado) {
      // Agregado sozinho - também precisa inserir GTCFUNDP
      logger.info({ codPessoa }, 'Agregado detectado, inserindo como freteiro com GTCFUNDP');
      await insereMotoristaAgregado(pessoaData);
    } else if (
      pessoaData.Personagens.some(
        (p) =>
          p.DescPersonagem === 'Agente' ||
          p.DescPersonagem === 'Cliente' ||
          p.DescPersonagem === 'Destinatário' ||
          p.DescPersonagem === 'Destinatario' ||
          p.DescPersonagem === 'Expedidor do frete' ||
          p.DescPersonagem === 'Filial' ||
          p.DescPersonagem === 'Fornecedor' ||
          p.DescPersonagem === 'Recebedor do frete' ||
          p.DescPersonagem === 'Remetente' ||
          p.DescPersonagem === 'Seguradora' ||
          p.DescPersonagem === 'Transportadora' ||
          p.DescPersonagem === 'Vendedor',
      )
    ) {
      // Qualquer outro que não for motorista, agregado ou motorista + funcionario, caem no cliente fornecedor
      await insereClienteFornecedor(pessoaData);
    } else {
      // Se não tem personagens definidos, tratar como cliente/fornecedor
      logger.warn({ codPessoa }, 'Nenhum personagem definido, tratando como cliente/fornecedor');
      await insereClienteFornecedor(pessoaData);
    }

    logger.info({ codPessoa }, 'Integração Senior concluída com sucesso');
  } catch (error: any) {
    logger.error({ error: error.message, codPessoa }, 'Erro ao inserir dados da pessoa na Senior');
    throw error;
  }
}

/**
 * Insere Cliente/Fornecedor na Senior
 * Baseado no código C# original: InsereClienteFornecedor()
 * Chama: SISCLI, SISPIX, SISCONTA, SISCLITL
 */
async function insereClienteFornecedor(pessoa: PessoaIntegracaoData): Promise<void> {
  try {
    logger.info({ codPessoa: pessoa.CodPessoa }, 'Iniciando inserção Cliente/Fornecedor na Senior');

    // REGRA CRIADA DEPOIS PARA CLIENTE EXTERIOR
    // Se endereço for Exterior, ajustar CNPJ
    if (pessoa.Enderecos && pessoa.Enderecos.length > 0) {
      const primeiroEndereco = pessoa.Enderecos[0];
      if (primeiroEndereco.Cidade === 'Exterior' && primeiroEndereco.Estado === 'EX') {
        if (pessoa.CodTipoPessoa === 2) {
          // PJ - usar CodPessoaEsl como CNPJ
          pessoa.Cnpj = pessoa.CodPessoaEsl.padStart(14, '0');
        }
      }
    }

    await insereSISCLI(pessoa);
    logger.info({ codPessoa: pessoa.CodPessoa }, 'SISCLI inserido com sucesso');

    if (pessoa.ChavesPix && pessoa.ChavesPix.length > 0) {
      await insereSISPIX(pessoa);
      logger.info({ codPessoa: pessoa.CodPessoa }, 'SISPIX inserido com sucesso');
    }

    await insereSISCONTA(pessoa);
    logger.info({ codPessoa: pessoa.CodPessoa }, 'SISCONTA inserido com sucesso');

    await insereSISCLITL(pessoa);
    logger.info({ codPessoa: pessoa.CodPessoa }, 'SISCLITL inserido com sucesso');

    // Para Pessoa Jurídica (CNPJ), também inserir na GTCFRETE
    // Baseado na orientação: "Na tabela GTCFRETE não gravou o cadastro inteiro da pessoa CNPJ"
    if (pessoa.CodTipoPessoa === 2) {
      try {
        await insereGTCFRETE(pessoa);
        logger.info(
          { codPessoa: pessoa.CodPessoa },
          'GTCFRETE inserido com sucesso para Cliente/Fornecedor CNPJ',
        );
      } catch (error: any) {
        // Log mas não interrompe o fluxo se GTCFRETE falhar
        logger.warn(
          { error: error.message, codPessoa: pessoa.CodPessoa },
          'Erro ao inserir GTCFRETE para Cliente/Fornecedor CNPJ (continuando)',
        );
      }
    }

    logger.info(
      { codPessoa: pessoa.CodPessoa },
      'Cliente/Fornecedor inserido na Senior com sucesso',
    );
  } catch (error: any) {
    logger.error(
      { error: error.message, codPessoa: pessoa.CodPessoa },
      'Erro ao inserir Cliente/Fornecedor na Senior',
    );
    throw error;
  }
}

/**
 * Insere Motorista/Agregado na Senior
 * Baseado no código C# original: InsereMotoristaAgregado()
 * Chama: SISCLI, SISPIX, SISCONTA, SISCLIFV, GTCFRETE, GTCFUNDP (se funcionario), GTCFTRCT
 */
async function insereMotoristaAgregado(pessoa: PessoaIntegracaoData): Promise<void> {
  try {
    logger.info({ codPessoa: pessoa.CodPessoa }, 'Iniciando inserção Motorista/Agregado na Senior');

    await insereSISCLI(pessoa);
    logger.info({ codPessoa: pessoa.CodPessoa }, 'SISCLI inserido com sucesso');

    if (pessoa.ChavesPix && pessoa.ChavesPix.length > 0) {
      await insereSISPIX(pessoa);
      logger.info({ codPessoa: pessoa.CodPessoa }, 'SISPIX inserido com sucesso');
    }

    await insereSISCONTA(pessoa);
    logger.info({ codPessoa: pessoa.CodPessoa }, 'SISCONTA inserido com sucesso');

    const inseriuDadosBancarios = await insereSISCLIFV(pessoa);
    if (inseriuDadosBancarios) {
      logger.info({ codPessoa: pessoa.CodPessoa }, 'SISCLIFV inserido com sucesso');
    }

    // Tentar inserir GTCFRETE - pode não inserir se faltar CPF/CNPJ, mas não interrompe o fluxo
    try {
      await insereGTCFRETE(pessoa);
      logger.info({ codPessoa: pessoa.CodPessoa }, 'GTCFRETE inserido com sucesso');
    } catch (error: any) {
      // Log mas não interrompe o fluxo se GTCFRETE falhar
      logger.warn(
        { error: error.message, codPessoa: pessoa.CodPessoa },
        'Erro ao inserir GTCFRETE em insereMotoristaAgregado (continuando)',
      );
    }

    // InsereGTCFUNDP para Motorista e Agregado (personagem 2 e 9)
    // Mesmo que não seja funcionário, precisa inserir na GTCFUNDP
    try {
      await insereGTCFUNDP(pessoa);
      logger.info(
        { codPessoa: pessoa.CodPessoa },
        'GTCFUNDP inserido com sucesso após insereMotoristaAgregado',
      );
    } catch (error: any) {
      // Não interromper o fluxo se GTCFUNDP falhar
      // Se for erro de chave duplicada, já foi tratado em insereGTCFUNDP como aviso
      // Outros erros são logados aqui, mas não interrompem o fluxo
      if (error?.code !== 'P2010' || error?.meta?.code !== '2627') {
        logger.error(
          {
            error: error.message,
            codPessoa: pessoa.CodPessoa,
            stack: error.stack,
            code: error.code,
            meta: error.meta,
          },
          'Erro ao inserir GTCFUNDP em insereMotoristaAgregado - continuando fluxo',
        );
      }
      // Erro de chave duplicada já foi tratado em insereGTCFUNDP, apenas continuar
    }

    // InsereGTCFTRCT só para Pessoa Jurídica
    if (pessoa.CodTipoPessoa === 2) {
      await insereGTCFTRCT(pessoa);
      logger.info({ codPessoa: pessoa.CodPessoa }, 'GTCFTRCT inserido com sucesso');
    }

    logger.info(
      { codPessoa: pessoa.CodPessoa },
      'Motorista/Agregado inserido na Senior com sucesso',
    );
  } catch (error: any) {
    logger.error(
      { error: error.message, codPessoa: pessoa.CodPessoa },
      'Erro ao inserir Motorista/Agregado na Senior',
    );
    throw error;
  }
}

/**
 * Insere Motorista/Funcionário na Senior
 * Baseado no código C# original: InsereMotoristaFuncionario()
 * Chama: SISFUN, SISPIX, SISFUNIA, SISFUNAL, SISFUNFP
 */
async function insereMotoristaFuncionario(pessoa: PessoaIntegracaoData): Promise<void> {
  try {
    logger.info(
      { codPessoa: pessoa.CodPessoa },
      'Iniciando inserção Motorista/Funcionário na Senior',
    );

    // SOMENTE Editar pessoa física
    // NAO BUSCAR CODIGO DO FUNCIONARIO, POIS O VIRGILIO MANDARÁ NO CAMPO CODIGO NA TELA DE EDICAO DA PESSOA
    await insereSISFUN(pessoa);
    logger.info({ codPessoa: pessoa.CodPessoa }, 'SISFUN inserido com sucesso');

    if (pessoa.ChavesPix && pessoa.ChavesPix.length > 0) {
      await insereSISPIX(pessoa);
      logger.info({ codPessoa: pessoa.CodPessoa }, 'SISPIX inserido com sucesso');
    }

    await insereSISFUNIA(pessoa);
    logger.info({ codPessoa: pessoa.CodPessoa }, 'SISFUNIA inserido com sucesso');

    await insereSISFUNAL(pessoa);
    logger.info({ codPessoa: pessoa.CodPessoa }, 'SISFUNAL inserido com sucesso');

    await insereSISFUNFP(pessoa);
    logger.info({ codPessoa: pessoa.CodPessoa }, 'SISFUNFP inserido com sucesso');

    logger.info(
      { codPessoa: pessoa.CodPessoa },
      'Motorista/Funcionário inserido na Senior com sucesso',
    );
  } catch (error: any) {
    logger.error(
      { error: error.message, codPessoa: pessoa.CodPessoa },
      'Erro ao inserir Motorista/Funcionário na Senior',
    );
    throw error;
  }
}

/**
 * Exclui registros existentes na tabela SISCLI antes de inserir
 * Baseado no código C# original: ExcluirSISCLI()
 */
async function excluirSISCLI(pessoa: PessoaIntegracaoData): Promise<void> {
  try {
    const cdInscricao =
      pessoa.CodTipoPessoa === 1
        ? (pessoa.Cpf || pessoa.CodPessoaEsl).padStart(14, '0')
        : (pessoa.Cnpj || pessoa.CodPessoaEsl).padStart(14, '0');

    const nrCGCCPF = pessoa.CodTipoPessoa === 1 ? pessoa.Cpf || '' : pessoa.Cnpj || '';

    if (!cdInscricao || !nrCGCCPF) {
      logger.warn(
        { codPessoa: pessoa.CodPessoa },
        'ExcluirSISCLI - CdInscricao ou NrCGCCPF vazio, abortando exclusão',
      );
      return;
    }

    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_SISCLI_EXCLUIR
        @CdInscricao = ${toSqlValue(cdInscricao)},
        @NrCGCCPF = ${toSqlValue(nrCGCCPF)};
    `;

    await prisma.$executeRawUnsafe(sql);
    logger.info(
      { codPessoa: pessoa.CodPessoa, cdInscricao },
      'SISCLI excluído com sucesso antes da inserção',
    );
  } catch (error: any) {
    // Não lançar erro se a exclusão falhar - pode ser que não exista registro para excluir
    logger.warn(
      { error: error.message, codPessoa: pessoa.CodPessoa },
      'Erro ao excluir SISCLI (pode não existir)',
    );
  }
}

/**
 * Insere na tabela SISCLI (Cliente/Fornecedor)
 * Baseado no código C# original: InsereSISCLI()
 */
async function insereSISCLI(pessoa: PessoaIntegracaoData): Promise<void> {
  // Definir cdInscricao fora do try para estar acessível no catch
  const cdInscricao =
    pessoa.CodTipoPessoa === 1
      ? (pessoa.Cpf || pessoa.CodPessoaEsl).padStart(14, '0')
      : (pessoa.Cnpj || pessoa.CodPessoaEsl).padStart(14, '0');

  try {
    const cdEmpresa = await retornaCodEmpresa(pessoa.DescFilialResponsavel);

    // Excluir registros existentes antes de inserir (padrão do código C#)
    // Em caso de processamento paralelo, pode falhar se já foi excluído - ignorar
    try {
      await excluirSISCLI(pessoa);
    } catch (excludeError: any) {
      // Ignorar erros de exclusão em condições de corrida
      logger.warn(
        { error: excludeError.message, codPessoa: pessoa.CodPessoa },
        'Erro ao excluir SISCLI (pode já ter sido excluído em processamento paralelo)',
      );
    }

    // Mapear CEP para int (remover caracteres não numéricos)
    const cepNumero =
      pessoa.Enderecos && pessoa.Enderecos.length > 0 && pessoa.Enderecos[0].Cep
        ? parseInt(pessoa.Enderecos[0].Cep.replace(/\D/g, '')) || null
        : null;

    // Mapear Suframa para numeric
    const suframaNumero = pessoa.CodigoSuframa
      ? parseFloat(pessoa.CodigoSuframa.replace(/\D/g, '')) || null
      : null;

    // Determinar InClassificacaoFiscal e NrInscricaoEstadual conforme regras:
    // Quando InInscricao = 2 (CNPJ) → InClassificacaoFiscal = 8 e NrInscricaoEstadual = 'ISENTO'
    // Quando InInscricao = 1 (CPF) → InClassificacaoFiscal = 7 e NrInscricaoEstadual = 'ISENTO'
    const inInscricao = pessoa.CodTipoPessoa === 1 ? 1 : 2;
    const inClassificacaoFiscal = inInscricao === 2 ? 8 : inInscricao === 1 ? 7 : null;
    const nrInscricaoEstadual = 'ISENTO';

    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_SISCLI_INCLUIR
        @CdInscricao = ${toSqlValue(cdInscricao)},
        @DsEntidade = ${toSqlValue(pessoa.NomeRazaoSocial)},
        @DsApelido = ${toSqlValue((pessoa.NomeFantasia || pessoa.NomeRazaoSocial).substring(0, 10))},
        @InInscricao = ${inInscricao},
        @InCadastro = ${pessoa.CodTipoPessoa === 1 ? 1 : 2},
        @DsEndereco = ${toSqlValue((pessoa.Enderecos && pessoa.Enderecos.length > 0 ? pessoa.Enderecos[0].Logradouro || '' : '').substring(0, 40))},
        @DsBairro = ${toSqlValue((pessoa.Enderecos && pessoa.Enderecos.length > 0 ? pessoa.Enderecos[0].Bairro || '' : '').substring(0, 30))},
        @NrCEP = ${cepNumero !== null ? cepNumero : 'NULL'},
        @NrInscricaoEstadual = ${toSqlValue(nrInscricaoEstadual)},
        @NrCGCCPF = ${toSqlValue((pessoa.CodTipoPessoa === 1 ? pessoa.Cpf || '' : pessoa.Cnpj || '').substring(0, 18))},
        @NrTelefone = ${toSqlValue((pessoa.Contatos && pessoa.Contatos.length > 0 ? (pessoa.Contatos[0].DDD || '') + (pessoa.Contatos[0].Telefone || '') : '').substring(0, 25))},
        @NrFax = NULL,
        @DtCadastro = ${parseDate(pessoa.DataCadastro)},
        @DtAtualizacao = ${parseDate(new Date())},
        @CdEmpresa = ${cdEmpresa},
        @NrSuframa = ${suframaNumero !== null ? suframaNumero : 'NULL'},
        @DsEMail = ${toSqlValue((pessoa.Contatos && pessoa.Contatos.length > 0 ? pessoa.Contatos[0].Email || '' : '').substring(0, 100))},
        @DsHomePage = ${toSqlValue((pessoa.Site || '').substring(0, 40))},
        @FgClienteBasico = NULL,
        @DsComentario = ${toSqlValue(pessoa.Observacao || '')},
        @NrInscricaoMunicipal = ${toSqlValue((pessoa.InscricaoMunicipal || '').substring(0, 20))},
        @DtMovimentacao = NULL,
        @NrApolice = NULL,
        @DsUSuarioInc = ${toSqlValue((pessoa.UsuarioCadastro || '').substring(0, 10))},
        @DsUSuarioAlt = NULL,
        @InClassificacaoFiscal = ${inClassificacaoFiscal !== null ? inClassificacaoFiscal : 'NULL'},
        @DSLONG = NULL,
        @DSLAT = NULL,
        @InHoraEntrega = NULL,
        @InObsEntrega = NULL,
        @InLocalDificilEnt = NULL,
        @InIntegra = 1,
        @DtFundacaoEmp = ${parseDate(pessoa.DataFundacao)},
        @InAtivo = ${pessoa.Ativo ? 0 : 1},
        @DsComplemento = ${toSqlValue((pessoa.Enderecos && pessoa.Enderecos.length > 0 ? pessoa.Enderecos[0].Complemento || '' : '').substring(0, 40))},
        @DsNumero = ${toSqlValue((pessoa.Enderecos && pessoa.Enderecos.length > 0 ? pessoa.Enderecos[0].Numero || '' : '').substring(0, 10))},
        @CdCNAE20 = ${pessoa.CodigoCNAE ? parseInt(pessoa.CodigoCNAE.replace(/\D/g, '')) || 'NULL' : 'NULL'},
        @CdPotencialCliente = NULL,
        @QtDiasAdicionaisTDE = NULL,
        @VlPercTDE = NULL,
        @VlMinimoTDE = NULL,
        @DtUltVerifEletInfCad = NULL,
        @CdFornecSenior = NULL,
        @InAgeCargaTranspInter = NULL,
        @InLocalDificilAcesso = NULL,
        @DtNascimento = ${parseDateNascimento(pessoa.DataNascimento)},
        @ININTEGRADOG7 = NULL,
        @InObrigaM3Peso = NULL,
        @InObrigaMedPeso = NULL,
        @CdIntegSenior = NULL;
    `;

    await prisma.$executeRawUnsafe(sql);
    logger.info({ codPessoa: pessoa.CodPessoa, cdInscricao }, 'SISCLI inserido com sucesso');
  } catch (error: any) {
    // Ignorar erros de chave duplicada em condições de corrida (múltiplas requisições simultâneas)
    if (
      error.message?.includes('PRIMARY KEY') ||
      error.message?.includes('duplicate key') ||
      error.message?.includes('2627')
    ) {
      logger.warn(
        { codPessoa: pessoa.CodPessoa, cdInscricao },
        'SISCLI já existe (ignorando duplicata em processamento paralelo)',
      );
      return; // Não lançar erro, apenas logar aviso
    }
    logger.error({ error: error.message, codPessoa: pessoa.CodPessoa }, 'Erro ao inserir SISCLI');
    throw error;
  }
}

/**
 * Exclui registros existentes na tabela SISPIX antes de inserir
 * Baseado no código C# original: ExcluirSISPIX()
 */
async function excluirSISPIX(pessoa: PessoaIntegracaoData): Promise<void> {
  try {
    const cdInscricao =
      pessoa.CodTipoPessoa === 1
        ? (pessoa.Cpf || pessoa.CodPessoaEsl).padStart(14, '0')
        : (pessoa.Cnpj || pessoa.CodPessoaEsl).padStart(14, '0');

    if (!cdInscricao) {
      logger.warn(
        { codPessoa: pessoa.CodPessoa },
        'ExcluirSISPIX - CdInscricao vazio, abortando exclusão',
      );
      return;
    }

    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_SISPIX_EXCLUIR
        @NrCPFCNPJ = ${toSqlValue(cdInscricao)};
    `;

    await prisma.$executeRawUnsafe(sql);
    logger.info(
      { codPessoa: pessoa.CodPessoa, cdInscricao },
      'SISPIX excluído com sucesso antes da inserção',
    );
  } catch (error: any) {
    // Não lançar erro se a exclusão falhar - pode ser que não exista registro para excluir
    logger.warn(
      { error: error.message, codPessoa: pessoa.CodPessoa },
      'Erro ao excluir SISPIX (pode não existir)',
    );
  }
}

/**
 * Insere na tabela SISPIX (Chaves PIX)
 * Baseado no código C# original: InsereSISPIX()
 */
async function insereSISPIX(pessoa: PessoaIntegracaoData): Promise<void> {
  try {
    if (!pessoa.ChavesPix || pessoa.ChavesPix.length === 0) {
      logger.info(
        { codPessoa: pessoa.CodPessoa },
        'InsereSISPIX - Sem chaves PIX para inserir, abortando',
      );
      return;
    }

    const cdInscricao =
      pessoa.CodTipoPessoa === 1
        ? (pessoa.Cpf || pessoa.CodPessoaEsl).padStart(14, '0')
        : (pessoa.Cnpj || pessoa.CodPessoaEsl).padStart(14, '0');

    // Excluir registros existentes antes de inserir (padrão do código C#)
    await excluirSISPIX(pessoa);

    // Primeiro loop: InCadastro = 1 (Cliente)
    const cdPixAux = await retornaProximoCdPix();

    for (let i = 0; i < pessoa.ChavesPix.length; i++) {
      const chavePix = pessoa.ChavesPix[i];

      // Determinar InTpChavePIX baseado no CodTipoChave
      // TipoChave == 1  Celular - bmx 1
      // TipoChave == 2  CPF - bmx 3
      // TipoChave == 3  CNPJ - bmx 3
      // TipoChave == 4  Email - bmx 2
      // TipoChave == 5  Aleatorio - bmx 4
      let inTpChavePIX = 0;
      if (chavePix.CodTipoChave === 1) {
        inTpChavePIX = 1;
      } else if (chavePix.CodTipoChave === 2 || chavePix.CodTipoChave === 3) {
        inTpChavePIX = 3;
      } else if (chavePix.CodTipoChave === 4) {
        inTpChavePIX = 2;
      } else if (chavePix.CodTipoChave === 5) {
        inTpChavePIX = 4;
      }

      // CdSequencia = i + 1 (como no código C#)
      const cdSequencia = i + 1;

      // InChavePadrao = 1 se for a primeira (i == 0), senão 0
      const inChavePadrao = i === 0 ? 1 : 0;

      // Determinar NrCPFCNPJ
      let nrCPFCNPJ: string;
      if (pessoa.CodTipoPessoa === 1) {
        if (!pessoa.Cpf) {
          continue; // Pula se não tem CPF
        }
        nrCPFCNPJ = pessoa.Cpf.padStart(14, '0');
      } else {
        if (!pessoa.Cnpj) {
          continue; // Pula se não tem CNPJ
        }
        nrCPFCNPJ = pessoa.Cnpj;
      }

      // Se tipo de chave for 1 (Celular), adicionar prefixo +55
      let dsChavePIX = chavePix.ChavePix || '';
      if (chavePix.CodTipoChave === 1 && dsChavePIX) {
        // Verificar se já não começa com +55
        if (!dsChavePIX.startsWith('+55')) {
          dsChavePIX = '+55' + dsChavePIX;
        }
      }

      const sql = `
        EXEC dbo.P_INTEGRACAO_SENIOR_SISPIX_INCLUIR
          @CdPIX = ${cdPixAux},
          @CdSequencia = ${cdSequencia},
          @InTpChavePIX = ${inTpChavePIX},
          @DsChavePIX = ${toSqlValue(dsChavePIX)},
          @InChavePadrao = ${inChavePadrao},
          @InCadastro = 1,
          @NrCPFCNPJ = ${toSqlValue(nrCPFCNPJ)};
      `;

      await prisma.$executeRawUnsafe(sql);
      logger.info(
        {
          codPessoa: pessoa.CodPessoa,
          cdPix: cdPixAux,
          cdSequencia,
          chavePix: dsChavePIX,
          tipoChave: chavePix.CodTipoChave,
        },
        'Chave PIX inserida com sucesso (InCadastro=1)',
      );
    }

    // Segundo loop: InCadastro = 2 (Fornecedor) - apenas para Pessoa Jurídica
    if (pessoa.CodTipoPessoa === 2) {
      const cdPixAux1 = await retornaProximoCdPix();

      for (let j = 0; j < pessoa.ChavesPix.length; j++) {
        const chavePix = pessoa.ChavesPix[j];

        // Determinar InTpChavePIX (mesma lógica)
        let inTpChavePIX = 0;
        if (chavePix.CodTipoChave === 1) {
          inTpChavePIX = 1;
        } else if (chavePix.CodTipoChave === 2 || chavePix.CodTipoChave === 3) {
          inTpChavePIX = 3;
        } else if (chavePix.CodTipoChave === 4) {
          inTpChavePIX = 2;
        } else if (chavePix.CodTipoChave === 5) {
          inTpChavePIX = 4;
        }

        const cdSequencia = j + 1;
        const inChavePadrao = j === 0 ? 1 : 0;
        const nrCPFCNPJ = pessoa.Cnpj || '';

        // Se tipo de chave for 1 (Celular), adicionar prefixo +55
        let dsChavePIX = chavePix.ChavePix || '';
        if (chavePix.CodTipoChave === 1 && dsChavePIX) {
          // Verificar se já não começa com +55
          if (!dsChavePIX.startsWith('+55')) {
            dsChavePIX = '+55' + dsChavePIX;
          }
        }

        const sql = `
          EXEC dbo.P_INTEGRACAO_SENIOR_SISPIX_INCLUIR
            @CdPIX = ${cdPixAux1},
            @CdSequencia = ${cdSequencia},
            @InTpChavePIX = ${inTpChavePIX},
            @DsChavePIX = ${toSqlValue(dsChavePIX)},
            @InChavePadrao = ${inChavePadrao},
            @InCadastro = 2,
            @NrCPFCNPJ = ${toSqlValue(nrCPFCNPJ)};
        `;

        await prisma.$executeRawUnsafe(sql);
        logger.info(
          {
            codPessoa: pessoa.CodPessoa,
            cdPix: cdPixAux1,
            cdSequencia,
            chavePix: dsChavePIX,
            tipoChave: chavePix.CodTipoChave,
          },
          'Chave PIX inserida com sucesso (InCadastro=2)',
        );
      }
    }
  } catch (error: any) {
    logger.error({ error: error.message, codPessoa: pessoa.CodPessoa }, 'Erro ao inserir SISPIX');
    throw error;
  }
}

/**
 * Exclui registros existentes na tabela SISCONTA antes de inserir
 * Baseado no código C# original: ExcluirSISCONTA()
 */
async function excluirSISCONTA(pessoa: PessoaIntegracaoData): Promise<void> {
  try {
    const cdInscricao =
      pessoa.CodTipoPessoa === 1
        ? (pessoa.Cpf || pessoa.CodPessoaEsl).padStart(14, '0')
        : (pessoa.Cnpj || pessoa.CodPessoaEsl).padStart(14, '0');

    if (!cdInscricao) {
      logger.warn(
        { codPessoa: pessoa.CodPessoa },
        'ExcluirSISCONTA - CdInscricao vazio, abortando exclusão',
      );
      return;
    }

    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_SISCONTA_EXCLUIR
        @CdInscricao = ${toSqlValue(cdInscricao)};
    `;

    await prisma.$executeRawUnsafe(sql);
    logger.info(
      { codPessoa: pessoa.CodPessoa, cdInscricao },
      'SISCONTA excluído com sucesso antes da inserção',
    );
  } catch (error: any) {
    // Não lançar erro se a exclusão falhar - pode ser que não exista registro para excluir
    logger.warn(
      { error: error.message, codPessoa: pessoa.CodPessoa },
      'Erro ao excluir SISCONTA (pode não existir)',
    );
  }
}

/**
 * Insere na tabela SISCONTA (Contatos)
 * Baseado no código C# original: InsereSISCONTA()
 */
async function insereSISCONTA(pessoa: PessoaIntegracaoData): Promise<void> {
  try {
    if (!pessoa.Contatos || pessoa.Contatos.length === 0) {
      return;
    }

    const cdEmpresa = await retornaCodEmpresa(pessoa.DescFilialResponsavel);
    const cdInscricao =
      pessoa.CodTipoPessoa === 1
        ? (pessoa.Cpf || pessoa.CodPessoaEsl).padStart(14, '0')
        : (pessoa.Cnpj || pessoa.CodPessoaEsl).padStart(14, '0');

    // Excluir registros existentes antes de inserir (padrão do código C#)
    await excluirSISCONTA(pessoa);

    for (const contato of pessoa.Contatos) {
      const cdContato = await retornaProximoContato(cdInscricao);

      const sql = `
        EXEC dbo.P_INTEGRACAO_SENIOR_SISCONTA_INCLUIR
          @CdEmpresa = ${cdEmpresa},
          @CdInscricao = ${toSqlValue(cdInscricao)},
          @CdContato = ${cdContato},
          @CdTipoContato = ${contato.CodTipoContato || 0},
          @DsNome = ${toSqlValue(contato.Nome || '')},
          @DsCpf = ${toSqlValue(contato.Cpf || '')},
          @DsDDD = ${toSqlValue(contato.DDD || '')},
          @DsTelefone = ${toSqlValue(contato.Telefone || '')},
          @DsCelular = ${toSqlValue(contato.Celular || '')},
          @DsDepartamento = ${toSqlValue(contato.Departamento || '')},
          @DsEmail = ${toSqlValue(contato.Email || '')},
          @DsObservacao = ${toSqlValue(contato.Observacao || '')};
      `;

      await prisma.$executeRawUnsafe(sql);
      logger.info(
        { codPessoa: pessoa.CodPessoa, cdContato, nome: contato.Nome },
        'Contato inserido com sucesso',
      );
    }
  } catch (error: any) {
    logger.error({ error: error.message, codPessoa: pessoa.CodPessoa }, 'Erro ao inserir SISCONTA');
    throw error;
  }
}

/**
 * Exclui registros existentes na tabela SISCLITL antes de inserir
 * Baseado no código C# original: EcluirSISCLITL()
 */
async function excluirSISCLITL(pessoa: PessoaIntegracaoData): Promise<void> {
  try {
    const cdInscricao =
      pessoa.CodTipoPessoa === 1
        ? (pessoa.Cpf || pessoa.CodPessoaEsl).padStart(14, '0')
        : (pessoa.Cnpj || pessoa.CodPessoaEsl).padStart(14, '0');

    if (!cdInscricao) {
      logger.warn(
        { codPessoa: pessoa.CodPessoa },
        'ExcluirSISCLITL - CdInscricao vazio, abortando exclusão',
      );
      return;
    }

    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_SISCLITL_EXCLUIR
        @CdInscricao = ${toSqlValue(cdInscricao)};
    `;

    await prisma.$executeRawUnsafe(sql);
    logger.info(
      { codPessoa: pessoa.CodPessoa, cdInscricao },
      'SISCLITL excluído com sucesso antes da inserção',
    );
  } catch (error: any) {
    // Não lançar erro se a exclusão falhar - pode ser que não exista registro para excluir
    logger.warn(
      { error: error.message, codPessoa: pessoa.CodPessoa },
      'Erro ao excluir SISCLITL (pode não existir)',
    );
  }
}

/**
 * Insere na tabela SISCLITL (Telefones)
 * Baseado no código C# original: InsereSISCLITL()
 */
async function insereSISCLITL(pessoa: PessoaIntegracaoData): Promise<void> {
  try {
    if (!pessoa.Contatos || pessoa.Contatos.length === 0) {
      return;
    }

    const cdEmpresa = await retornaCodEmpresa(pessoa.DescFilialResponsavel);
    const cdInscricao =
      pessoa.CodTipoPessoa === 1
        ? (pessoa.Cpf || pessoa.CodPessoaEsl).padStart(14, '0')
        : (pessoa.Cnpj || pessoa.CodPessoaEsl).padStart(14, '0');

    // Excluir registros existentes antes de inserir (padrão do código C#)
    await excluirSISCLITL(pessoa);

    for (const contato of pessoa.Contatos) {
      if (contato.Telefone || contato.Celular) {
        const sql = `
          EXEC dbo.P_INTEGRACAO_SENIOR_SISCLITL_INCLUIR
            @CdEmpresa = ${cdEmpresa},
            @CdInscricao = ${toSqlValue(cdInscricao)},
            @CdTipoTelefone = ${contato.CodTipoContato || 0},
            @DsDDD = ${toSqlValue(contato.DDD || '')},
            @DsTelefone = ${toSqlValue(contato.Telefone || contato.Celular || '')},
            @DsRamal = ${toSqlValue('')},
            @DsNomeContato = ${toSqlValue(contato.Nome || '')};
        `;

        await prisma.$executeRawUnsafe(sql);
        logger.info(
          { codPessoa: pessoa.CodPessoa, telefone: contato.Telefone || contato.Celular },
          'Telefone inserido com sucesso',
        );
      }
    }
  } catch (error: any) {
    logger.error({ error: error.message, codPessoa: pessoa.CodPessoa }, 'Erro ao inserir SISCLITL');
    throw error;
  }
}

/**
 * Exclui registros existentes na tabela SISCLIFV antes de inserir
 * Baseado no código C# original: ExcluirSISCLIFV()
 */
async function excluirSISCLIFV(pessoa: PessoaIntegracaoData): Promise<void> {
  try {
    // Só excluir se houver dados bancários
    if (!pessoa.DadosBancarios || pessoa.DadosBancarios.length === 0) {
      return;
    }

    const cdInscricao =
      pessoa.CodTipoPessoa === 1
        ? (pessoa.Cpf || pessoa.CodPessoaEsl).padStart(14, '0')
        : (pessoa.Cnpj || pessoa.CodPessoaEsl).padStart(14, '0');

    if (!cdInscricao) {
      logger.warn(
        { codPessoa: pessoa.CodPessoa },
        'ExcluirSISCLIFV - CdInscricao vazio, abortando exclusão',
      );
      return;
    }

    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_SISCLIFV_EXCLUIR
        @CdInscricao = ${toSqlValue(cdInscricao)};
    `;

    await prisma.$executeRawUnsafe(sql);
    logger.info(
      { codPessoa: pessoa.CodPessoa, cdInscricao },
      'SISCLIFV excluído com sucesso antes da inserção',
    );
  } catch (error: any) {
    // Não lançar erro se a exclusão falhar - pode ser que não exista registro para excluir
    logger.warn(
      { error: error.message, codPessoa: pessoa.CodPessoa },
      'Erro ao excluir SISCLIFV (pode não existir)',
    );
  }
}

/**
 * Insere na tabela SISCLIFV (Favorecido - Dados Bancários)
 * Baseado no código C# original: InsereSISCLIFV()
 * @returns true se inseriu dados bancários, false caso contrário
 */
async function insereSISCLIFV(pessoa: PessoaIntegracaoData): Promise<boolean> {
  try {
    // Retorna se não houver dados bancários para inserir
    if (!pessoa.DadosBancarios || pessoa.DadosBancarios.length === 0) {
      logger.info(
        { codPessoa: pessoa.CodPessoa },
        'InsereSISCLIFV - Sem dados bancários para inserir, abortando',
      );
      return false;
    }

    const cdInscricao =
      pessoa.CodTipoPessoa === 1
        ? (pessoa.Cpf || pessoa.CodPessoaEsl).padStart(14, '0')
        : (pessoa.Cnpj || pessoa.CodPessoaEsl).padStart(14, '0');

    // Excluir registros existentes antes de inserir (padrão do código C#)
    await excluirSISCLIFV(pessoa);

    // Iterar sobre os dados bancários (como no código C#)
    for (let i = 0; i < pessoa.DadosBancarios.length; i++) {
      const dadosBancario = pessoa.DadosBancarios[i];

      // Determinar InInscrFavorecido (1 para CPF, 0 para CNPJ)
      const inInscrFavorecido = pessoa.CodTipoPessoa === 1 ? 1 : 0;

      // Determinar NrCNPJCPFFavorecido
      const nrCNPJCPFFavorecido = pessoa.CodTipoPessoa === 1 ? pessoa.Cpf || '' : pessoa.Cnpj || '';

      // Determinar InTipoContaBancaria
      // Código C#: CodTipoConta == 3 -> 0, CodTipoConta == 2 -> 1
      // Verificar se existe CodTipoConta ou TipoConta.CodTipoConta
      let inTipoContaBancaria = 0;
      const codTipoConta = dadosBancario.CodTipoConta || dadosBancario.TipoConta?.CodTipoConta;
      if (codTipoConta === 3) {
        inTipoContaBancaria = 0;
      } else if (codTipoConta === 2) {
        inTipoContaBancaria = 1;
      }

      // Usar o índice do loop como CdSequencia (como no código C#: SISCLIFV.CdSequencia = i)
      const cdSequencia = i;

      // Obter CdBanco - usar o mesmo valor que foi salvo na tabela DadosBancario
      // IMPORTANTE: CodBanco da tabela DadosBancario contém o código FEBRABAN (ex: 33 para Santander)
      // Prioridade: CodBanco da tabela DadosBancario > Codigo (FEBRABAN) > banco.codigo do JSON
      // NÃO usar BancoCodigo (vem do JOIN com tabela Banco e pode ter valor diferente)
      let cdBanco = dadosBancario.CodBanco;
      if (!cdBanco || cdBanco === 0) {
        cdBanco = dadosBancario.Codigo; // Código FEBRABAN
      }
      if (!cdBanco || cdBanco === 0) {
        cdBanco = dadosBancario.Banco?.Codigo || dadosBancario.banco?.codigo;
      }
      if (!cdBanco || cdBanco === 0) {
        logger.warn(
          {
            codPessoa: pessoa.CodPessoa,
            dadosBancario: {
              CodBanco: dadosBancario.CodBanco,
              Codigo: dadosBancario.Codigo,
              BancoCodigo: dadosBancario.BancoCodigo,
            },
          },
          'SISCLIFV - Código de banco não encontrado, usando 0',
        );
        cdBanco = 0;
      }

      // Log de debug para verificar valores
      logger.debug(
        {
          codPessoa: pessoa.CodPessoa,
          cdSequencia,
          codBancoTabela: dadosBancario.CodBanco,
          codigoFEBRABAN: dadosBancario.Codigo,
          bancoCodigo: dadosBancario.Banco?.Codigo || dadosBancario.banco?.codigo,
          bancoCodigoTabela: dadosBancario.BancoCodigo,
          cdBancoFinal: cdBanco,
        },
        'Valores de banco para SISCLIFV',
      );

      // Obter NumAgencia e NumConta (campos vêm da query como NumAgencia e NumConta)
      const cdAgencia = dadosBancario.NumAgencia || '';
      const nrContaCorrente = dadosBancario.NumConta || '';

      const sql = `
        EXEC dbo.P_INTEGRACAO_SENIOR_SISCLIFV_INCLUIR
          @CdInscricao = ${toSqlValue(cdInscricao)},
          @CdSequencia = ${cdSequencia},
          @DsFavorecido = ${toSqlValue(pessoa.NomeRazaoSocial || '')},
          @InInscrFavorecido = ${inInscrFavorecido},
          @NrCNPJCPFFavorecido = ${toSqlValue(nrCNPJCPFFavorecido)},
          @CdBanco = ${cdBanco},
          @CdAgencia = ${toSqlValue(cdAgencia)},
          @NrContaCorrente = ${toSqlValue(nrContaCorrente)},
          @InTipoContaBancaria = ${inTipoContaBancaria},
          @InFavorecidoPadrao = 1;
      `;

      await prisma.$executeRawUnsafe(sql);
      logger.info(
        {
          codPessoa: pessoa.CodPessoa,
          cdSequencia,
          cdBanco,
          codBancoTabela: dadosBancario.CodBanco,
          codigoFEBRABAN: dadosBancario.Codigo,
          numAgencia: cdAgencia,
          numConta: nrContaCorrente,
        },
        'SISCLIFV inserido com sucesso',
      );
    }

    return true; // Retorna true indicando que inseriu dados bancários
  } catch (error: any) {
    logger.error({ error: error.message, codPessoa: pessoa.CodPessoa }, 'Erro ao inserir SISCLIFV');
    throw error;
  }
}

/**
 * Exclui registros existentes na tabela GTCFRETE antes de inserir
 * Baseado no código C# original: ExcluirGTCFRETE()
 */
async function excluirGTCFRETE(pessoa: PessoaIntegracaoData): Promise<void> {
  try {
    const cdfreteiro =
      pessoa.CodTipoPessoa === 1
        ? (pessoa.Cpf || pessoa.CodPessoaEsl).padStart(14, '0')
        : (pessoa.Cnpj || pessoa.CodPessoaEsl).padStart(14, '0');

    if (!cdfreteiro) {
      logger.warn(
        { codPessoa: pessoa.CodPessoa },
        'ExcluirGTCFRETE - CDFRETEIRO vazio, abortando exclusão',
      );
      return;
    }

    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_GTCFRETE_EXCLUIR
        @CDFRETEIRO = ${toSqlValue(cdfreteiro)};
    `;

    await prisma.$executeRawUnsafe(sql);
    logger.info(
      { codPessoa: pessoa.CodPessoa, cdfreteiro },
      'GTCFRETE excluído com sucesso antes da inserção',
    );
  } catch (error: any) {
    // Não lançar erro se a exclusão falhar - pode ser que não exista registro para excluir
    logger.warn(
      { error: error.message, codPessoa: pessoa.CodPessoa },
      'Erro ao excluir GTCFRETE (pode não existir)',
    );
  }
}

/**
 * Insere na tabela GTCFRETE (Freteiro)
 * Baseado no código C# original: InsereGTCFRETE()
 */
async function insereGTCFRETE(pessoa: PessoaIntegracaoData): Promise<void> {
  try {
    // Validação: Para Pessoa Física, verificar se tem pelo menos CPF (obrigatório para GTCFRETE)
    // Agregados podem não ter dados completos de motorista, mas ainda devem ser inseridos na GTCFRETE
    if (pessoa.CodTipoPessoa === 1 && !pessoa.Cpf && !pessoa.CodPessoaEsl) {
      logger.info(
        { codPessoa: pessoa.CodPessoa },
        'InsereGTCFRETE - Pessoa Física sem CPF ou CodPessoaEsl, abortando inserção',
      );
      return;
    }

    // Excluir registros existentes antes de inserir (padrão do código C#)
    await excluirGTCFRETE(pessoa);

    // Determinar CDFRETEIRO (primeiro parâmetro obrigatório)
    let cdfreteiro: string;
    if (pessoa.CodTipoPessoa === 1) {
      if (!pessoa.Cpf) {
        logger.info(
          { codPessoa: pessoa.CodPessoa },
          'InsereGTCFRETE - CPF vazio, abortando inserção',
        );
        return;
      }
      cdfreteiro = pessoa.Cpf.padStart(14, '0');
    } else {
      if (!pessoa.Cnpj) {
        logger.info(
          { codPessoa: pessoa.CodPessoa },
          'InsereGTCFRETE - CNPJ vazio, abortando inserção',
        );
        return;
      }
      // CNPJ deve ter 14 caracteres (sem formatação)
      cdfreteiro = pessoa.Cnpj.replace(/\D/g, '').padStart(14, '0');
    }

    // Mapear CEP para int (remover caracteres não numéricos)
    const nrCep =
      pessoa.Enderecos && pessoa.Enderecos.length > 0 && pessoa.Enderecos[0].Cep
        ? parseInt(pessoa.Enderecos[0].Cep.replace(/\D/g, '')) || null
        : null;

    // Mapear dados bancários - usar campo Codigo (código FEBRABAN) da tabela ou banco.codigo do JSON
    // Prioridade: Codigo da tabela > banco.codigo do JSON > CodBanco > BancoCodigo
    const cdBanco =
      pessoa.DadosBancarios && pessoa.DadosBancarios.length > 0
        ? pessoa.DadosBancarios[0].Codigo ||
          pessoa.DadosBancarios[0].Banco?.Codigo ||
          pessoa.DadosBancarios[0].banco?.codigo ||
          pessoa.DadosBancarios[0].CodBanco ||
          pessoa.DadosBancarios[0].BancoCodigo ||
          null
        : null;
    const cdAgencia =
      pessoa.DadosBancarios &&
      pessoa.DadosBancarios.length > 0 &&
      pessoa.DadosBancarios[0].NumAgencia
        ? parseInt(pessoa.DadosBancarios[0].NumAgencia.replace(/\D/g, '')) || null
        : null;
    const dsContaCorrente =
      pessoa.DadosBancarios && pessoa.DadosBancarios.length > 0
        ? pessoa.DadosBancarios[0].NumConta || ''
        : '';
    const dsFavorecido =
      pessoa.DadosBancarios && pessoa.DadosBancarios.length > 0
        ? pessoa.DadosBancarios[0].NomeTitular || pessoa.NomeRazaoSocial || ''
        : pessoa.NomeRazaoSocial || '';
    const nrCNPJCPFFavorecido =
      pessoa.DadosBancarios && pessoa.DadosBancarios.length > 0
        ? pessoa.DadosBancarios[0].CpfCnpj || ''
        : '';

    // Mapear telefone
    const nrTelefone =
      pessoa.Contatos && pessoa.Contatos.length > 0 ? pessoa.Contatos[0].Telefone || '' : '';

    // Determinar InFreteiro conforme regra:
    // Quando InInscricao = 1 (CPF) → InFreteiro = 0
    // Caso contrário → InFreteiro = 1
    const inInscricao = pessoa.CodTipoPessoa === 1 ? 1 : 0;
    const inFreteiro = inInscricao === 1 ? 0 : 1;

    // Mapear NRANTT (RNTC) - pode estar em Motorista.NumeroANTT ou pessoa.RNTRC
    const nrAntt = pessoa.Motorista?.NumeroANTT?.toString() || pessoa.RNTRC || '';

    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_GTCFRETE_INCLUIR
        @CDFRETEIRO = ${toSqlValue(cdfreteiro)},
        @DSNOME = ${toSqlValue((pessoa.NomeRazaoSocial || '').substring(0, 40))},
        @DSAPELIDO = ${toSqlValue((pessoa.NomeFantasia || pessoa.NomeRazaoSocial || '').substring(0, 10))},
        @ININSCRICAO = ${inInscricao},
        @INFRETEIRO = ${inFreteiro},
        @NRCEP = ${nrCep !== null ? nrCep : 'NULL'},
        @DSENDERECO = ${toSqlValue((pessoa.Enderecos && pessoa.Enderecos.length > 0 ? pessoa.Enderecos[0].Logradouro || '' : '').substring(0, 40))},
        @INJURIDICAFISICA = ${pessoa.CodTipoPessoa === 1 ? 1 : 0},
        @NRINSCRICAOESTADUAL = ${toSqlValue((pessoa.InscricaoEstadual || '').substring(0, 18))},
        @NRINSCRICAODNER = NULL,
        @DSCATEGORIA = ${toSqlValue((pessoa.Motorista?.TipoCNH || '').substring(0, 5))},
        @DTVENCIMENTOCNH = ${parseDate(pessoa.Motorista?.DataVencimentoCNH)},
        @DTEMISSAOCNH = ${parseDate(pessoa.Motorista?.DataEmissaoCNH)},
        @NRCEPCNH = NULL,
        @DSORGAOEMISSORCNH = ${toSqlValue((pessoa.Motorista?.OrgaoEmissor || '').substring(0, 6))},
        @NRCNPJCPF = ${toSqlValue((pessoa.CodTipoPessoa === 1 ? pessoa.Cpf || '' : pessoa.Cnpj || '').substring(0, 18))},
        @CDPROPRIETARIO = NULL,
        @NRTELEFONE = ${toSqlValue(nrTelefone.substring(0, 20))},
        @NRFAX = NULL,
        @NRDEPENDENTES = 0,
        @DTULTIMOSERVICO = NULL,
        @VLIAPAS = NULL,
        @CDCONCEITO = NULL,
        @DTCADASTRO = ${parseDate(pessoa.DataCadastro)},
        @CDBANCO = ${cdBanco !== null ? cdBanco : 'NULL'},
        @CDAGENCIA = ${cdAgencia !== null ? cdAgencia : 'NULL'},
        @DSCONTACORRENTE = ${toSqlValue(dsContaCorrente.substring(0, 20))},
        @CDCONTACORRENTE = NULL,
        @DSNOMEPAI = ${toSqlValue((pessoa.NomePai || '').substring(0, 40))},
        @DSNOMEMAE = ${toSqlValue((pessoa.NomeMae || '').substring(0, 40))},
        @DTNASCIMENTO = ${parseDateNascimento(pessoa.DataNascimento)},
        @NRCEPNASCIMENTO = NULL,
        @CDESTADOCIVIL = 1,
        @DSNOMECONJUGE = NULL,
        @DSCORPELE = NULL,
        @DSCABELO = NULL,
        @DSOLHOS = NULL,
        @NRALTURA = NULL,
        @QTPESO = NULL,
        @DSSINAISFISICOS = NULL,
        @DSREFERENCIA = NULL,
        @CDTABELACOMISSAO = NULL,
        @FGVITIMAROUBO = 0,
        @QTVITIMAROUBO = 0,
        @FGACIDENTE = 0,
        @QTACIDENTE = 0,
        @FGTRANSPEMPRESA = 0,
        @QTTRANSPEMPRESA = 0,
        @NRPROTOCOLOGRIS = NULL,
        @DTVENCIMENTOGRIS = NULL,
        @FGAGREGADO = 0,
        @CDTABELA = NULL,
        @VLPERCADTO = NULL,
        @CDCONDICAOVENCTO = NULL,
        @DTEMISSAORG = ${parseDate(pessoa.DataEmissaoRG)},
        @NRCEPRG = 1,
        @VLALIQLIMITEADTO = NULL,
        @DTEMISSAOPRONTUARIO = NULL,
        @DTVENCIMENTOPRONTUARIO = NULL,
        @NRPRONTUARIO = NULL,
        @NRCNH = ${toSqlValue((pessoa.Motorista?.NumeroCNH || '').substring(0, 20))},
        @DTATUALIZACAO = ${parseDate(new Date())},
        @DTLIBERACAOGRIS = NULL,
        @CDTABELACONVENIO = NULL,
        @NRPIS = ${toSqlValue((pessoa.NumeroPISPASEP || '').substring(0, 11))},
        @CDFILIAL = ${(await retornaCodEmpresa(pessoa.DescFilialResponsavel)) || 300},
        @DSFAVORECIDO = ${toSqlValue(dsFavorecido.substring(0, 40))},
        @ININATIVO = ${pessoa.Ativo ? 0 : 1},
        @DSORGAOEMISSORRG = ${toSqlValue((pessoa.OrgaoExpedidorRG || '').substring(0, 6))},
        @NRPRONTUARIOCNH = NULL,
        @DTPRIMHABILITACAO = ${parseDate(pessoa.Motorista?.DataPrimeiraCNH)},
        @INSEXO = ${pessoa.Sexo ? 0 : 1},
        @INEMPRESAGRUPO = NULL,
        @CDEMPRESAGRUPO = NULL,
        @NRANTT = ${toSqlValue(nrAntt.substring(0, 20))},
        @ININSCRICAOFAVORECIDO = ${pessoa.CodTipoPessoa === 1 ? 1 : 0},
        @NRCNPJCPFFAVORECIDO = ${toSqlValue(nrCNPJCPFFavorecido.substring(0, 14))},
        @NRMOPP = NULL,
        @DTEMISSAOMOPP = NULL,
        @DTVENCIMENTOMOPP = NULL,
        @DSORGAOEMISSORMOPP = NULL,
        @NRCEPMOPP = NULL,
        @CDCONTRATANTE = NULL,
        @CDFRETEIROFOLHA = NULL,
        @DSUSUARIOINCLUSAO = ${toSqlValue((pessoa.UsuarioCadastro || '').substring(0, 10))},
        @DTINCLUSAO = ${parseDate(pessoa.DataCadastro)},
        @HRINCLUSAO = NULL,
        @DSEMAIL = ${toSqlValue((pessoa.Contatos && pessoa.Contatos.length > 0 ? pessoa.Contatos[0].Email || '' : '').substring(0, 100))},
        @INENVIAEMAILCONHEC = 0,
        @DSNUMERO = ${toSqlValue((pessoa.Enderecos && pessoa.Enderecos.length > 0 ? pessoa.Enderecos[0].Numero || '0' : '0').substring(0, 10))},
        @DSCOMPLEMENTO = ${toSqlValue((pessoa.Enderecos && pessoa.Enderecos.length > 0 ? pessoa.Enderecos[0].Complemento || '' : '').substring(0, 40))},
        @CDCBO2002 = NULL,
        @CDSEQUENCIALCBO2002 = NULL,
        @NrInscrMunicipal = ${toSqlValue((pessoa.InscricaoMunicipal || '').substring(0, 50))},
        @InCalcAdtoCartaFrete = NULL,
        @NrCelularParticular = ${toSqlValue((nrTelefone || '').substring(0, 25))},
        @NrCelularEmpresa = NULL,
        @InTpPagtoAdtoCIOT = 0,
        @InTpPagtoSaldoCIOT = 0,
        @InTpEfetivaAdtoCIOT = 0,
        @InTpEfetivaSaldoCIOT = 0,
        @InTpConfirmAdtoCIOT = 0,
        @InTpConfirmSaldoCIOT = 0,
        @InTipoContaBancaria = NULL,
        @InConfEspecPagtoCIOT = 0,
        @InMeioPagtoCFe = 0,
        @InTpContaBancaria = NULL,
        @InGeraMovFinPedagioCFe = 0,
        @CdMovFinDescPedagio = NULL,
        @CdOperadoraPedagio = NULL,
        @InTpLogradouro = ${toSqlValue('R')},
        @DsBairro = ${toSqlValue((pessoa.Enderecos && pessoa.Enderecos.length > 0 ? pessoa.Enderecos[0].Bairro || '' : '').substring(0, 30))},
        @CdModContratoCTRBRPA = NULL,
        @CdRepLegal = NULL,
        @NumCadHCM = NULL,
        @ININTEGSENIOR = NULL,
        @CdIntegSenior = NULL,
        @DSOBSERVACAO = ${toSqlValue(pessoa.Observacao || '')},
        @NrFormNrCNH = NULL,
        @NrSegNrCNH = ${toSqlValue((pessoa.Motorista?.CodSegurancaCNH || '').substring(0, 20))},
        @NrRenach = ${toSqlValue((pessoa.Motorista?.NumeroRenach || '').substring(0, 20))},
        @DsImgCNH = NULL,
        @ImgCNH = NULL;
    `;

    logger.debug(
      {
        codPessoa: pessoa.CodPessoa,
        cdfreteiro,
        sql: sql.replace(/\s+/g, ' ').trim().substring(0, 500), // Limitar tamanho do log
      },
      'Executando stored procedure GTCFRETE',
    );

    await prisma.$executeRawUnsafe(sql);

    logger.info(
      {
        codPessoa: pessoa.CodPessoa,
        cdfreteiro,
        codTipoPessoa: pessoa.CodTipoPessoa,
        temMotorista: !!pessoa.Motorista,
        mensagem: 'Stored procedure P_INTEGRACAO_SENIOR_GTCFRETE_INCLUIR executada com sucesso',
      },
      'GTCFRETE inserido com sucesso',
    );
  } catch (error: any) {
    logger.error({ error: error.message, codPessoa: pessoa.CodPessoa }, 'Erro ao inserir GTCFRETE');
    throw error;
  }
}

/**
 * Insere na tabela GTCFUNDP (Fundador)
 * Baseado no código C# original: InsereGTCFUNDP()
 */
async function insereGTCFUNDP(pessoa: PessoaIntegracaoData): Promise<void> {
  // Definir nrCPF no início para estar disponível no catch
  let nrCPF: string = '';

  try {
    logger.info(
      {
        codPessoa: pessoa.CodPessoa,
        cdFuncionario: pessoa.cdFuncionario,
        temCdFuncionario: !!(pessoa.cdFuncionario && pessoa.cdFuncionario > 0),
      },
      'Iniciando inserção GTCFUNDP',
    );

    const cdInscricao = normalizeDocumentoSenior(
      pessoa.CodTipoPessoa === 1
        ? pessoa.Cpf || pessoa.CodPessoaEsl
        : pessoa.Cnpj || pessoa.CodPessoaEsl,
      pessoa.CodTipoPessoa,
      14, // cdInscricao usa 14 dígitos (CPF preenchido com zeros à esquerda)
    );

    // @NrCPF: Apenas CPF com 14 dígitos (zeros à esquerda), NUNCA CNPJ - OBRIGATÓRIO
    // Exemplo: "12345678901" → "00012345678901"
    // Se for pessoa jurídica, não tem CPF - não deve chamar esta função
    if (pessoa.CodTipoPessoa !== 1) {
      logger.warn(
        { codPessoa: pessoa.CodPessoa },
        'GTCFUNDP - Pessoa jurídica não tem CPF, abortando inserção',
      );
      return;
    }

    nrCPF = normalizeDocumentoSenior(
      pessoa.Cpf || pessoa.CodPessoaEsl || '',
      pessoa.CodTipoPessoa,
      14,
    );

    if (!nrCPF || nrCPF.trim() === '') {
      logger.warn({ codPessoa: pessoa.CodPessoa }, 'GTCFUNDP - CPF vazio, abortando inserção');
      return;
    }

    // @CdFuncionario: opcional, passar NULL se não houver
    const cdFuncionario =
      pessoa.cdFuncionario && pessoa.cdFuncionario > 0 ? pessoa.cdFuncionario : 'NULL';

    // Campos opcionais disponíveis
    const dsNome = pessoa.NomeRazaoSocial
      ? toSqlValue((pessoa.NomeRazaoSocial || '').substring(0, 40))
      : 'NULL';
    const dsApelido =
      pessoa.NomeFantasia || pessoa.NomeRazaoSocial
        ? toSqlValue((pessoa.NomeFantasia || pessoa.NomeRazaoSocial || '').substring(0, 10))
        : 'NULL';

    // Parâmetros da stored procedure: @NrCPF (obrigatório), @CdFuncionario (opcional), @DsNome (opcional), @DsApelido (opcional)
    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_GTCFUNDP_INCLUIR
        @NrCPF = ${toSqlValue(nrCPF)},
        @CdFuncionario = ${cdFuncionario},
        @DsNome = ${dsNome},
        @DsApelido = ${dsApelido};
    `;

    logger.debug(
      {
        codPessoa: pessoa.CodPessoa,
        sql: sql.replace(/\s+/g, ' ').trim(),
      },
      'Executando stored procedure GTCFUNDP',
    );

    await prisma.$executeRawUnsafe(sql);

    logger.info(
      {
        codPessoa: pessoa.CodPessoa,
        cdInscricao,
        cdFuncionario: cdFuncionario,
        nrCPF,
        temCdFuncionario: !!(pessoa.cdFuncionario && pessoa.cdFuncionario > 0),
      },
      'GTCFUNDP inserido com sucesso',
    );
  } catch (error: any) {
    // Se for erro de chave duplicada (2627), tentar atualizar o registro existente
    if (error?.code === 'P2010' && error?.meta?.code === '2627') {
      logger.info(
        {
          codPessoa: pessoa.CodPessoa,
          nrCPF,
          mensagem: 'Registro já existe na tabela GTCFunDp - tentando atualizar',
        },
        'GTCFUNDP já existe na base (chave duplicada) - tentando atualizar registro',
      );

      // Tentar atualizar o registro existente
      try {
        const cdFuncionario =
          pessoa.cdFuncionario && pessoa.cdFuncionario > 0 ? pessoa.cdFuncionario : null;
        const dsNome = pessoa.NomeRazaoSocial
          ? (pessoa.NomeRazaoSocial || '').substring(0, 40)
          : null;
        const dsApelido =
          pessoa.NomeFantasia || pessoa.NomeRazaoSocial
            ? (pessoa.NomeFantasia || pessoa.NomeRazaoSocial || '').substring(0, 10)
            : null;

        // Verificar se existe stored procedure de atualização
        // Se não existir, fazer UPDATE direto na tabela
        const seniorDb = env.SENIOR_DATABASE || 'SOFTRAN_BRASILMAXI';
        const updateFields: string[] = [];

        if (cdFuncionario !== null) {
          updateFields.push(`CdFuncionario = ${cdFuncionario}`);
        }
        if (dsNome) {
          updateFields.push(`DsNome = ${toSqlValue(dsNome)}`);
        }
        if (dsApelido) {
          updateFields.push(`DsApelido = ${toSqlValue(dsApelido)}`);
        }
        updateFields.push(`DtAlteracao = GETDATE()`);

        if (updateFields.length === 0) {
          logger.warn(
            { codPessoa: pessoa.CodPessoa, nrCPF },
            'GTCFUNDP já existe - nenhum campo para atualizar',
          );
          return;
        }

        const updateSql = `
          UPDATE [${seniorDb}].dbo.GTCFunDp
          SET ${updateFields.join(', ')}
          WHERE NrCPF = ${toSqlValue(nrCPF)};
        `;

        await prisma.$executeRawUnsafe(updateSql);

        logger.info(
          {
            codPessoa: pessoa.CodPessoa,
            nrCPF,
            cdFuncionario: cdFuncionario,
            atualizado: true,
          },
          'GTCFUNDP atualizado com sucesso (registro já existia)',
        );

        return;
      } catch (updateError: any) {
        // Se falhar ao atualizar, apenas logar como aviso (não crítico)
        logger.warn(
          {
            codPessoa: pessoa.CodPessoa,
            nrCPF,
            erroAtualizacao: updateError.message,
            mensagem: 'Não foi possível atualizar GTCFunDp, mas registro já existe',
          },
          'GTCFUNDP já existe - falha ao atualizar (não crítico)',
        );
        // Não relançar o erro - registro já existe, não é crítico
        return;
      }
    }

    logger.error(
      {
        error: error.message,
        codPessoa: pessoa.CodPessoa,
        stack: error.stack,
        code: error.code,
        meta: error.meta,
      },
      'Erro ao inserir GTCFUNDP',
    );
    throw error;
  }
}

/**
 * Insere na tabela GTCFTRCT (Transportadora)
 * Baseado no código C# original: InsereGTCFTRCT()
 */
async function insereGTCFTRCT(pessoa: PessoaIntegracaoData): Promise<void> {
  try {
    if (pessoa.CodTipoPessoa !== 2) {
      // Só para Pessoa Jurídica
      return;
    }

    const cdEmpresa = await retornaCodEmpresa(pessoa.DescFilialResponsavel);
    const cdInscricao = (pessoa.Cnpj || pessoa.CodPessoaEsl).padStart(14, '0');

    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_GTCFTRCT_INCLUIR
        @CdEmpresa = ${cdEmpresa},
        @CdInscricao = ${toSqlValue(cdInscricao)},
        @DsRntrc = ${toSqlValue(pessoa.RNTRC || '')},
        @DtValidadeRntrc = ${parseDate(pessoa.DataValidadeRNTRC)};
    `;

    await prisma.$executeRawUnsafe(sql);
    logger.info({ codPessoa: pessoa.CodPessoa }, 'GTCFTRCT inserido com sucesso');
  } catch (error: any) {
    logger.error({ error: error.message, codPessoa: pessoa.CodPessoa }, 'Erro ao inserir GTCFTRCT');
    throw error;
  }
}

/**
 * Insere na tabela SISFUN (Funcionário)
 * Baseado no código C# original: InsereSISFUN()
 */
async function insereSISFUN(pessoa: PessoaIntegracaoData): Promise<void> {
  try {
    if (!pessoa.Funcionario) {
      logger.warn({ codPessoa: pessoa.CodPessoa }, 'Funcionário não encontrado, pulando SISFUN');
      return;
    }

    const cdEmpresa = await retornaCodEmpresa(pessoa.DescFilialResponsavel);
    const cdInscricao = (pessoa.Cpf || pessoa.CodPessoaEsl).padStart(14, '0');

    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_SISFUN_INCLUIR
        @CdEmpresa = ${cdEmpresa},
        @CdInscricao = ${toSqlValue(cdInscricao)},
        @CdFuncionario = ${pessoa.Funcionario.CdFuncionario || 0},
        @DsNumCtps = ${toSqlValue(pessoa.Funcionario.DsNumCtps || '')},
        @DsSerieCtps = ${toSqlValue(pessoa.Funcionario.DsSerieCtps || '')},
        @DsUfCtps = ${toSqlValue(pessoa.Funcionario.DsUfCtps || '')},
        @DtAdmissao = ${parseDate(pessoa.Funcionario.DtAdmissao)},
        @VlSalarioAtual = ${pessoa.Funcionario.VlSalarioAtual || 0},
        @DsCorPele = ${toSqlValue(pessoa.Funcionario.DsCorPele || '')},
        @DtEmissaoCtps = ${parseDate(pessoa.Funcionario.DtEmissaoCTPS)},
        @VlSalarioInicial = ${pessoa.Funcionario.VlSalarioInicial || 0};
    `;

    await prisma.$executeRawUnsafe(sql);
    logger.info(
      { codPessoa: pessoa.CodPessoa, cdFuncionario: pessoa.Funcionario.CdFuncionario },
      'SISFUN inserido com sucesso',
    );
  } catch (error: any) {
    logger.error({ error: error.message, codPessoa: pessoa.CodPessoa }, 'Erro ao inserir SISFUN');
    throw error;
  }
}

/**
 * Insere na tabela SISFUNIA (Informações Adicionais do Funcionário)
 * Baseado no código C# original: InsereSISFUNIA()
 */
async function insereSISFUNIA(pessoa: PessoaIntegracaoData): Promise<void> {
  try {
    if (!pessoa.Funcionario) {
      return;
    }

    const cdEmpresa = await retornaCodEmpresa(pessoa.DescFilialResponsavel);
    const cdInscricao = (pessoa.Cpf || pessoa.CodPessoaEsl).padStart(14, '0');

    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_SISFUNIA_INCLUIR
        @CdEmpresa = ${cdEmpresa},
        @CdInscricao = ${toSqlValue(cdInscricao)},
        @CdFuncionario = ${pessoa.Funcionario.CdFuncionario || 0};
    `;

    await prisma.$executeRawUnsafe(sql);
    logger.info({ codPessoa: pessoa.CodPessoa }, 'SISFUNIA inserido com sucesso');
  } catch (error: any) {
    logger.error({ error: error.message, codPessoa: pessoa.CodPessoa }, 'Erro ao inserir SISFUNIA');
    throw error;
  }
}

/**
 * Insere na tabela SISFUNAL (Alterações do Funcionário)
 * Baseado no código C# original: InsereSISFUNAL()
 */
async function insereSISFUNAL(pessoa: PessoaIntegracaoData): Promise<void> {
  try {
    if (!pessoa.Funcionario) {
      return;
    }

    const cdEmpresa = await retornaCodEmpresa(pessoa.DescFilialResponsavel);
    const cdInscricao = (pessoa.Cpf || pessoa.CodPessoaEsl).padStart(14, '0');

    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_SISFUNAL_INCLUIR
        @CdEmpresa = ${cdEmpresa},
        @CdInscricao = ${toSqlValue(cdInscricao)},
        @CdFuncionario = ${pessoa.Funcionario.CdFuncionario || 0},
        @DtAdmissao = ${parseDate(pessoa.Funcionario.DtAdmissao)},
        @VlSalarioAtual = ${pessoa.Funcionario.VlSalarioAtual || 0};
    `;

    await prisma.$executeRawUnsafe(sql);
    logger.info({ codPessoa: pessoa.CodPessoa }, 'SISFUNAL inserido com sucesso');
  } catch (error: any) {
    logger.error({ error: error.message, codPessoa: pessoa.CodPessoa }, 'Erro ao inserir SISFUNAL');
    throw error;
  }
}

/**
 * Insere na tabela SISFUNFP (Folha de Pagamento do Funcionário)
 * Baseado no código C# original: InsereSISFUNFP()
 */
async function insereSISFUNFP(pessoa: PessoaIntegracaoData): Promise<void> {
  try {
    if (!pessoa.Funcionario) {
      return;
    }

    const cdEmpresa = await retornaCodEmpresa(pessoa.DescFilialResponsavel);
    const cdInscricao = (pessoa.Cpf || pessoa.CodPessoaEsl).padStart(14, '0');

    const sql = `
      EXEC dbo.P_INTEGRACAO_SENIOR_SISFUNFP_INCLUIR
        @CdEmpresa = ${cdEmpresa},
        @CdInscricao = ${toSqlValue(cdInscricao)},
        @CdFuncionario = ${pessoa.Funcionario.CdFuncionario || 0},
        @VlSalarioAtual = ${pessoa.Funcionario.VlSalarioAtual || 0};
    `;

    await prisma.$executeRawUnsafe(sql);
    logger.info({ codPessoa: pessoa.CodPessoa }, 'SISFUNFP inserido com sucesso');
  } catch (error: any) {
    logger.error({ error: error.message, codPessoa: pessoa.CodPessoa }, 'Erro ao inserir SISFUNFP');
    throw error;
  }
}

/**
 * Processa a carga de pessoas para integração com Senior
 * Baseado no código C# original: CargaPessoaSenior()
 * Busca pessoas com FlgIntegradoSenior = 0 e as integra com Senior
 */
async function cargaPessoaSenior(): Promise<void> {
  try {
    logger.info('Iniciando carga de pessoas para integração com Senior');

    const pessoas = await obterPessoasParaIntegracao();

    if (!pessoas || pessoas.length === 0) {
      logger.info('Nenhuma pessoa pendente de integração com Senior');
      return;
    }

    logger.info({ total: pessoas.length }, `Processando ${pessoas.length} pessoas para integração`);

    for (let i = 0; i < pessoas.length; i++) {
      const pessoa = pessoas[i];
      if (!pessoa || !pessoa.CodPessoa) {
        logger.warn({ index: i }, 'Pessoa inválida no array, pulando');
        continue;
      }

      try {
        logger.info(
          {
            codPessoa: pessoa.CodPessoa,
            progresso: `${i + 1}/${pessoas.length}`,
          },
          `Processando pessoa ${i + 1}/${pessoas.length} para integração Senior`,
        );

        // Inserir dados da pessoa na Senior
        await inserirDadosPessoaSISCLI(pessoa.CodPessoa);

        // Marcar como integrada
        await alterarIntegracaoPessoa(pessoa.CodPessoa);

        logger.info({ codPessoa: pessoa.CodPessoa }, 'Pessoa integrada com Senior com sucesso');
      } catch (error: any) {
        logger.error(
          {
            error: error.message,
            stack: error.stack,
            codPessoa: pessoa.CodPessoa,
          },
          `Erro ao processar pessoa ${i + 1}/${pessoas.length} - continuando com próxima`,
        );
        // Continua processando as próximas pessoas mesmo se houver erro
      }
    }

    logger.info({ total: pessoas.length }, 'Carga de pessoas para integração Senior concluída');
  } catch (error: any) {
    logger.error(
      { error: error.message, stack: error.stack },
      'Erro ao executar carga de pessoas para integração Senior',
    );
  }
}

export async function obterCodigoPessoa(codPessoaEsl: string): Promise<number> {
  try {
    if (IS_POSTGRES) {
      const local = await buscarPessoaLocal(codPessoaEsl);
      return local.codPessoa ?? 0;
    }

    const sql = `
      EXEC dbo.P_OBTER_COD_PESSOA_ESL
        @CodPessoaEsl = ${toSqlValue(codPessoaEsl)};
    `;

    const result = await prisma.$queryRawUnsafe<Array<{ CodPessoa: number }>>(sql);
    if (result && result.length > 0 && result[0]?.CodPessoa) {
      return Number(result[0].CodPessoa);
    }
    return 0;
  } catch (error: any) {
    logger.error({ error: error.message, codPessoaEsl }, 'Erro ao obter código da pessoa');
    return 0;
  }
}

/**
 * Insere dados da pessoa principal
 */
export async function inserirDadosPessoa(pessoa: Pessoa): Promise<number> {
  try {
    logger.info(
      {
        codPessoaEsl: pessoa.CodPessoaEsl,
        nomeRazaoSocial: pessoa.NomeRazaoSocial,
        cpf: pessoa.Cpf,
        cnpj: pessoa.Cnpj,
      },
      'Chamando stored procedure P_DADOS_PESSOA_ESL_INCLUIR para inserir pessoa (deve inserir no AFS_INTEGRADO e Senior)',
    );

    const sql = `
      DECLARE @ReturnValue INT;
      EXEC @ReturnValue = dbo.P_DADOS_PESSOA_ESL_INCLUIR
        @CodPessoaEsl = ${toSqlValue(pessoa.CodPessoaEsl)},
        @CodTipoPessoa = ${pessoa.TipoPessoa.CodTipoPessoa},
        @CodNaturezaOperacao = ${pessoa.CodNaturezaOperacao ?? 'NULL'},
        @DescNaturezaOperacao = ${toSqlValue(pessoa.DescNaturezaOperacao)},
        @CodFilialResponsavel = ${pessoa.CodFilialResponsavel ? parseInt(pessoa.CodFilialResponsavel) || 0 : 0},
        @DescFilialResponsavel = ${toSqlValue(pessoa.DescFilialResponsavel)},
        @NomeRazaoSocial = ${toSqlValue(pessoa.NomeRazaoSocial)},
        @NomeFantasia = ${toSqlValue(pessoa.NomeFantasia)},
        @DataFundacao = ${parseDate(pessoa.DataFundacao)},
        @Cpf = ${toSqlValue(pessoa.Cpf)},
        @Cnpj = ${toSqlValue(pessoa.Cnpj)},
        @RG = ${toSqlValue(pessoa.RG)},
        @CidadeExpedicaoRG = ${toSqlValue(pessoa.CidadeExpedicaoRG)},
        @OrgaoExpedidorRG = ${toSqlValue(pessoa.OrgaoExpedidorRG)},
        @DataEmissaoRG = ${parseDate(pessoa.DataEmissaoRG)},
        @Sexo = ${pessoa.Sexo ? '1' : '0'},
        @EstadoCivil = ${toSqlValue(pessoa.EstadoCivil)},
        @DataNascimento = ${parseDateNascimento(pessoa.DataNascimento)},
        @CidadeNascimento = ${toSqlValue(pessoa.CidadeNascimento)},
        @InscricaoMunicipal = ${toSqlValue(pessoa.InscricaoMunicipal)},
        @InscricaoEstadual = ${toSqlValue(pessoa.InscricaoEstadual)},
        @DocumentoExterior = ${toSqlValue(pessoa.DocumentoExterior)},
        @PISPASEP = ${toSqlValue(pessoa.NumeroPISPASEP)},
        @RNTRC = ${toSqlValue(pessoa.RNTRC)},
        @ValidadeRNTRC = ${parseDate(pessoa.DataValidadeRNTRC)},
        @NomePai = ${toSqlValue(pessoa.NomePai)},
        @NomeMae = ${toSqlValue(pessoa.NomeMae)},
        @Contrinuinte = ${pessoa.Contrinuinte ? '1' : '0'},
        @Site = ${toSqlValue(pessoa.Site)},
        @CodCNAE = ${toSqlValue(pessoa.CodigoCNAE)},
        @RegimeFiscal = ${toSqlValue(pessoa.RegimeFiscal)},
        @CodSuframa = ${toSqlValue(pessoa.CodigoSuframa)},
        @Observacao = ${toSqlValue(pessoa.Observacao)},
        @DataCadastro = ${parseDate(pessoa.DataCadastro)},
        @CodUsuarioCadastro = ${pessoa.CodUsuarioCadastro ?? 0},
        @UsuarioCadastro = ${toSqlValue(pessoa.UsuarioCadastro)},
        @Ativo = ${pessoa.Ativo ? '1' : '0'},
        @FlgIntegradoSenior = 0;
      SELECT @ReturnValue AS CodPessoa;
    `;

    const result = await prisma.$queryRawUnsafe<Array<{ CodPessoa: number }>>(sql);
    const codPessoa = result && result[0]?.CodPessoa ? Number(result[0].CodPessoa) : 0;

    if (codPessoa > 0) {
      logger.info(
        {
          codPessoa,
          codPessoaEsl: pessoa.CodPessoaEsl,
          mensagem:
            'Stored procedure retornou CodPessoa. Verificar se pessoa foi inserida nas tabelas da Senior.',
        },
        'Pessoa inserida no AFS_INTEGRADO - verificar integração Senior',
      );

      // Verificar se a pessoa foi realmente inserida na tabela
      try {
        const pessoaVerificada = await prisma.$queryRawUnsafe<
          Array<{
            CodPessoa: number;
            FlgIntegradoSenior: number;
            NomeRazaoSocial: string;
          }>
        >(`
          SELECT CodPessoa, FlgIntegradoSenior, NomeRazaoSocial
          FROM [AFS_INTEGRADOR].[dbo].[Pessoa]
          WHERE CodPessoa = ${codPessoa}
        `);

        if (pessoaVerificada && pessoaVerificada.length > 0 && pessoaVerificada[0]) {
          logger.info(
            {
              codPessoa,
              flgIntegradoSenior: pessoaVerificada[0].FlgIntegradoSenior,
              nomeRazaoSocial: pessoaVerificada[0].NomeRazaoSocial,
              mensagem:
                pessoaVerificada[0].FlgIntegradoSenior === 1
                  ? 'FlgIntegradoSenior = 1 - Pessoa marcada como integrada na Senior'
                  : 'FlgIntegradoSenior = 0 - Pessoa NÃO foi integrada na Senior (verificar stored procedure)',
            },
            'Status da integração Senior verificado',
          );
        }
      } catch (verifyError: any) {
        logger.warn(
          {
            error: verifyError.message,
            codPessoa,
          },
          'Erro ao verificar status de integração Senior após inserção',
        );
      }
    } else {
      logger.warn(
        { codPessoaEsl: pessoa.CodPessoaEsl },
        'Stored procedure retornou 0 ao inserir pessoa',
      );
    }

    return codPessoa;
  } catch (error: any) {
    // Se o erro for relacionado à integração Senior, logar mas não falhar
    // A stored procedure pode ter inserido no AFS_INTEGRADO mesmo que a integração Senior tenha falhado
    if (
      error.message &&
      (error.message.includes(env.SENIOR_DATABASE) ||
        error.message.includes('SOFTRAN_BRASILMAXI') ||
        error.message.includes('SOFTRAN_BRASILMAXI') ||
        error.message.includes('Invalid object name') ||
        error.message.includes('senior') ||
        error.message.includes('Senior'))
    ) {
      logger.warn(
        {
          error: error.message,
          codPessoaEsl: pessoa.CodPessoaEsl,
          mensagem:
            'Erro na integração com Senior ERP, mas pessoa pode ter sido inserida no AFS_INTEGRADO',
        },
        'Aviso: Integração Senior ERP falhou, mas dados podem ter sido salvos na ESL',
      );

      // Tentar buscar o CodPessoa na tabela para verificar se foi inserido
      try {
        const codPessoa = await obterCodigoPessoa(pessoa.CodPessoaEsl);
        if (codPessoa > 0) {
          logger.info(
            {
              codPessoa,
              codPessoaEsl: pessoa.CodPessoaEsl,
              mensagem:
                'Pessoa encontrada na tabela AFS_INTEGRADO apesar do erro na integração Senior',
            },
            'Pessoa inserida no AFS_INTEGRADO, mas integração Senior falhou',
          );
          return codPessoa;
        }
      } catch (lookupError: any) {
        logger.error(
          {
            error: lookupError.message,
            codPessoaEsl: pessoa.CodPessoaEsl,
          },
          'Erro ao verificar se pessoa foi inserida após falha na integração Senior',
        );
      }

      // Se não encontrou, lançar o erro original
      throw error;
    }

    // Para outros erros, lançar normalmente
    logger.error(
      { error: error.message, codPessoaEsl: pessoa.CodPessoaEsl },
      'Erro ao inserir dados da pessoa',
    );
    throw error;
  }
}

/**
 * Altera dados da pessoa
 */
export async function alterarDadosPessoa(pessoa: Pessoa): Promise<void> {
  try {
    logger.info(
      {
        codPessoa: pessoa.CodPessoa,
        codPessoaEsl: pessoa.CodPessoaEsl,
        nomeRazaoSocial: pessoa.NomeRazaoSocial,
      },
      'Chamando stored procedure P_DADOS_PESSOA_ESL_ALTERAR para alterar pessoa (deve atualizar no AFS_INTEGRADO e Senior)',
    );

    const sql = `
      EXEC dbo.P_DADOS_PESSOA_ESL_ALTERAR
        @CodPessoa = ${pessoa.CodPessoa ?? 0},
        @CodPessoaEsl = ${toSqlValue(pessoa.CodPessoaEsl)},
        @CodTipoPessoa = ${pessoa.TipoPessoa.CodTipoPessoa},
        @CodNaturezaOperacao = ${pessoa.CodNaturezaOperacao ?? 'NULL'},
        @DescNaturezaOperacao = ${toSqlValue(pessoa.DescNaturezaOperacao)},
        @CodFilialResponsavel = ${pessoa.CodFilialResponsavel ? parseInt(pessoa.CodFilialResponsavel) || 0 : 0},
        @DescFilialResponsavel = ${toSqlValue(pessoa.DescFilialResponsavel)},
        @NomeRazaoSocial = ${toSqlValue(pessoa.NomeRazaoSocial)},
        @NomeFantasia = ${toSqlValue(pessoa.NomeFantasia)},
        @DataFundacao = ${parseDate(pessoa.DataFundacao)},
        @Cpf = ${toSqlValue(pessoa.Cpf)},
        @Cnpj = ${toSqlValue(pessoa.Cnpj)},
        @RG = ${toSqlValue(pessoa.RG)},
        @CidadeExpedicaoRG = ${toSqlValue(pessoa.CidadeExpedicaoRG)},
        @OrgaoExpedidorRG = ${toSqlValue(pessoa.OrgaoExpedidorRG)},
        @DataEmissaoRG = ${parseDate(pessoa.DataEmissaoRG)},
        @Sexo = ${pessoa.Sexo ? '1' : '0'},
        @EstadoCivil = ${toSqlValue(pessoa.EstadoCivil)},
        @DataNascimento = ${parseDateNascimento(pessoa.DataNascimento)},
        @CidadeNascimento = ${toSqlValue(pessoa.CidadeNascimento)},
        @InscricaoMunicipal = ${toSqlValue(pessoa.InscricaoMunicipal)},
        @InscricaoEstadual = ${toSqlValue(pessoa.InscricaoEstadual)},
        @DocumentoExterior = ${toSqlValue(pessoa.DocumentoExterior)},
        @PISPASEP = ${toSqlValue(pessoa.NumeroPISPASEP)},
        @RNTRC = ${toSqlValue(pessoa.RNTRC)},
        @ValidadeRNTRC = ${parseDate(pessoa.DataValidadeRNTRC)},
        @NomePai = ${toSqlValue(pessoa.NomePai)},
        @NomeMae = ${toSqlValue(pessoa.NomeMae)},
        @Contrinuinte = ${pessoa.Contrinuinte ? '1' : '0'},
        @Site = ${toSqlValue(pessoa.Site)},
        @CodCNAE = ${toSqlValue(pessoa.CodigoCNAE)},
        @RegimeFiscal = ${toSqlValue(pessoa.RegimeFiscal)},
        @CodSuframa = ${toSqlValue(pessoa.CodigoSuframa)},
        @Observacao = ${toSqlValue(pessoa.Observacao)},
        @DataCadastro = ${parseDate(pessoa.DataCadastro)},
        @CodUsuarioCadastro = ${pessoa.CodUsuarioCadastro ?? 0},
        @UsuarioCadastro = ${toSqlValue(pessoa.UsuarioCadastro)},
        @Ativo = ${pessoa.Ativo ? '1' : '0'},
        @FlgIntegradoSenior = 0;
    `;

    await prisma.$executeRawUnsafe(sql);
    logger.info(
      {
        codPessoa: pessoa.CodPessoa,
        codPessoaEsl: pessoa.CodPessoaEsl,
        mensagem:
          'Stored procedure executada. Verificar se pessoa foi atualizada nas tabelas da Senior.',
      },
      'Pessoa alterada no AFS_INTEGRADO - verificar integração Senior',
    );

    // Verificar status de integração Senior em background (não bloqueia resposta)
    // Esta verificação é apenas informativa e não precisa bloquear a resposta
    (async () => {
      try {
        const pessoaVerificada = await prisma.$queryRawUnsafe<
          Array<{
            CodPessoa: number;
            FlgIntegradoSenior: number;
            NomeRazaoSocial: string;
          }>
        >(`
          SELECT CodPessoa, FlgIntegradoSenior, NomeRazaoSocial
          FROM [AFS_INTEGRADOR].[dbo].[Pessoa]
          WHERE CodPessoa = ${pessoa.CodPessoa ?? 0}
        `);

        if (pessoaVerificada && pessoaVerificada.length > 0 && pessoaVerificada[0]) {
          logger.info(
            {
              codPessoa: pessoa.CodPessoa,
              flgIntegradoSenior: pessoaVerificada[0].FlgIntegradoSenior,
              nomeRazaoSocial: pessoaVerificada[0].NomeRazaoSocial,
              mensagem:
                pessoaVerificada[0].FlgIntegradoSenior === 1
                  ? 'FlgIntegradoSenior = 1 - Pessoa marcada como integrada na Senior'
                  : 'FlgIntegradoSenior = 0 - Pessoa NÃO foi integrada na Senior (verificar stored procedure)',
            },
            'Status da integração Senior verificado após alteração (background)',
          );
        }
      } catch (verifyError: any) {
        logger.warn(
          {
            error: verifyError.message,
            codPessoa: pessoa.CodPessoa,
          },
          'Erro ao verificar status de integração Senior após alteração (background)',
        );
      }
    })(); // Executar em background
  } catch (error: any) {
    logger.error(
      { error: error.message, codPessoaEsl: pessoa.CodPessoaEsl },
      'Erro ao alterar dados da pessoa',
    );
    throw error;
  }
}

/**
 * Insere contatos da pessoa
 */
export async function inserirContatosPessoa(codPessoa: number, contato: any): Promise<void> {
  try {
    const sql = `
      EXEC dbo.P_CONTATO_PESSOA_ESL_INCLUIR
        @CodPessoa = ${codPessoa},
        @CodTipoContato = ${contato.CodTipoContato ?? contato.TipoContato?.CodTipoContato ?? 0},
        @Nome = ${toSqlValue(contato.Nome)},
        @Cpf = ${toSqlValue(contato.Cpf)},
        @DDD = ${toSqlValue(contato.Ddd)},
        @Telefone = ${toSqlValue(contato.Telefone)},
        @Celular = ${toSqlValue(contato.Celular)},
        @Departamento = ${toSqlValue(contato.Departamento)},
        @Email = ${toSqlValue(contato.Email)},
        @Observacao = ${toSqlValue(contato.Observacao)};
    `;

    await prisma.$executeRawUnsafe(sql);
  } catch (error: any) {
    logger.error({ error: error.message, codPessoa }, 'Erro ao inserir contato da pessoa');
    throw error;
  }
}

/**
 * Insere endereços da pessoa
 */
export async function inserirEnderecosPessoa(codPessoa: number, endereco: any): Promise<void> {
  try {
    const sql = `
      EXEC dbo.P_ENDERECO_PESSOA_ESL_INCLUIR
        @CodPessoa = ${codPessoa},
        @CodTipoEndereco = ${endereco.CodTipoEndereco ?? endereco.TipoEndereco?.CodTipoEndereco ?? 0},
        @Cep = ${toSqlValue(endereco.Cep)},
        @Logradouro = ${toSqlValue(endereco.Logradouro)},
        @Numero = ${toSqlValue(endereco.Numero)},
        @Complemento = ${toSqlValue(endereco.Complemento)},
        @Bairro = ${toSqlValue(endereco.Bairro)},
        @Cidade = ${toSqlValue(endereco.Cidade)},
        @Estado = ${toSqlValue(endereco.Estado)};
    `;

    await prisma.$executeRawUnsafe(sql);
  } catch (error: any) {
    logger.error({ error: error.message, codPessoa }, 'Erro ao inserir endereço da pessoa');
    throw error;
  }
}

/**
 * Obtém código SQL do banco
 */
export async function obterCodigoSqlBanco(codigo: string): Promise<number> {
  try {
    const sql = `
      EXEC dbo.P_DADOS_BANCARIO_PESSOA_CODIGO_BANCO_OBTER
        @Codigo = ${codigo};
    `;

    const result = await prisma.$queryRawUnsafe<Array<{ CodBanco: number }>>(sql);
    if (result && result.length > 0 && result[0]?.CodBanco) {
      return Number(result[0].CodBanco);
    }
    return 0;
  } catch (error: any) {
    logger.error({ error: error.message, codigo }, 'Erro ao obter código SQL do banco');
    return 0;
  }
}

/**
 * Garante que o banco existe na tabela Banco do AFS_INTEGRADOR
 * Se não existir, cria o banco com os dados fornecidos
 */
async function garantirBancoExiste(codBanco: number, nomeBanco?: string): Promise<void> {
  try {
    // Verificar se o banco já existe
    const checkSql = `
      SELECT COUNT(*) as total
      FROM dbo.Banco
      WHERE CodBanco = ${codBanco};
    `;

    const result = await prisma.$queryRawUnsafe<Array<{ total: number }>>(checkSql);
    const existe = result && result.length > 0 && result[0] && result[0].total > 0;

    if (existe) {
      logger.debug({ codBanco }, 'Banco já existe na tabela Banco');
      return;
    }

    // Se não existe, criar o banco usando stored procedure
    // A stored procedure P_BANCO_CRIAR_OU_VERIFICAR lida com IDENTITY_INSERT automaticamente
    const nome = nomeBanco || `Banco ${codBanco}`;

    // Tentar usar stored procedure se existir, caso contrário usar INSERT direto
    try {
      const spSql = `
        EXEC dbo.P_BANCO_CRIAR_OU_VERIFICAR
          @CodBanco = ${codBanco},
          @Nome = ${toSqlValue(nome)};
      `;
      await prisma.$executeRawUnsafe(spSql);
      logger.info({ codBanco, nome }, 'Banco criado/verificado via stored procedure');
    } catch (spError: any) {
      // Se a stored procedure não existir, tentar INSERT direto com IDENTITY_INSERT
      logger.debug(
        { error: spError.message, codBanco },
        'Stored procedure não encontrada, tentando INSERT direto',
      );
      const insertSql = `
        SET IDENTITY_INSERT dbo.Banco ON;
        INSERT INTO dbo.Banco (CodBanco, Codigo, Nome)
        VALUES (${codBanco}, ${codBanco}, ${toSqlValue(nome)});
        SET IDENTITY_INSERT dbo.Banco OFF;
      `;
      await prisma.$executeRawUnsafe(insertSql);
      logger.info({ codBanco, nome }, 'Banco criado na tabela Banco');
    }
  } catch (error: any) {
    // Se der erro de chave duplicada, significa que o banco já existe (concorrência)
    if (error.code === '2627' || (error.message && error.message.includes('duplicate key'))) {
      logger.debug({ codBanco }, 'Banco já existe (criado por outro processo)');
      return;
    }
    // Se der erro de IDENTITY_INSERT, pode ser que a coluna não seja IDENTITY
    // ou que já exista outro registro. Tentar inserção sem especificar CodBanco
    if (error.code === '544' || (error.message && error.message.includes('IDENTITY_INSERT'))) {
      logger.warn(
        {
          error: error.message,
          codBanco,
          mensagem: 'Tentando criar banco sem especificar CodBanco (pode ser IDENTITY)',
        },
        'Erro ao criar banco com IDENTITY_INSERT - banco pode precisar ser criado manualmente',
      );
      // Não lançar erro - o banco precisa ser criado manualmente no banco de dados
      return;
    }
    logger.warn(
      {
        error: error.message,
        codBanco,
      },
      'Erro ao verificar/criar banco na tabela Banco - continuando mesmo assim',
    );
    // Não lançar erro - tentar inserir dados bancários mesmo se a criação do banco falhou
  }
}

/**
 * Insere dados bancários da pessoa
 */
export async function inserirDadosBancariosPessoa(
  codPessoa: number,
  dadosBancario: any,
): Promise<void> {
  try {
    // Usar banco.codigo do JSON diretamente como CodBanco na tabela (sem mapeamento)
    // O banco.codigo (ex: 237) será salvo diretamente no campo CodBanco da tabela DadosBancario
    const codigoBanco = dadosBancario.Banco?.Codigo || dadosBancario.banco?.codigo;

    if (!codigoBanco) {
      logger.warn({ codPessoa }, 'Dados bancários sem banco.codigo, pulando inserção');
      return;
    }

    // Usar banco.codigo diretamente como CodBanco (sem mapeamento)
    const codBanco = parseInt(String(codigoBanco));

    if (isNaN(codBanco) || codBanco <= 0) {
      logger.warn({ codPessoa, codigo: codigoBanco }, 'Código de banco inválido, pulando inserção');
      return;
    }

    logger.info(
      { codBanco, codigo: codigoBanco },
      'Usando banco.codigo do JSON diretamente como CodBanco na tabela',
    );

    // Garantir que o banco existe na tabela Banco antes de inserir os dados bancários
    const nomeBanco = dadosBancario.Banco?.Nome || dadosBancario.banco?.nome;
    await garantirBancoExiste(codBanco, nomeBanco);

    const codTipoConta = dadosBancario.TipoConta?.CodTipoConta ?? 3;
    const codTitularidade = dadosBancario.Titularidade?.CodTitularidade ?? 1;

    // Obter codigo do banco (código FEBRABAN) do JSON para salvar no campo Codigo
    let codigo = parseInt(String(codigoBanco)); // banco.codigo do JSON (ex: 237)

    // Validar codigo - usar CodBanco como fallback se inválido
    if (isNaN(codigo) || codigo <= 0) {
      logger.warn(
        { codPessoa, codigoBanco },
        'Código FEBRABAN inválido, usando CodBanco como fallback',
      );
      codigo = codBanco;
    }

    // CpfCnpj é obrigatório na stored procedure - usar string vazia se não fornecido
    const cpfCnpj = dadosBancario.CpfCnpj || dadosBancario.Cpf || dadosBancario.Cnpj || '';

    logger.info(
      {
        codPessoa,
        codBanco,
        codigo,
        codTipoConta,
        codTitularidade,
        numAgencia: dadosBancario.NumAgencia,
        numConta: dadosBancario.NumConta,
        nomeTitular: dadosBancario.NomeTitular,
        cpfCnpj: cpfCnpj ? '***' : '(vazio)',
        sqlPreview: `EXEC P_DADOS_BANCARIO_PESSOA_ESL_INCLUIR @CodPessoa=${codPessoa}, @CodBanco=${codBanco}, @Codigo=${codigo}...`,
      },
      'Preparando inserção de dados bancários',
    );

    const sql = `
      EXEC dbo.P_DADOS_BANCARIO_PESSOA_ESL_INCLUIR
        @CodPessoa = ${codPessoa},
        @CodBanco = ${codBanco},
        @Codigo = ${codigo},
        @CodTipoConta = ${codTipoConta},
        @CodTitularidade = ${codTitularidade},
        @NumAgencia = ${toSqlValue(dadosBancario.NumAgencia)},
        @NumConta = ${toSqlValue(dadosBancario.NumConta)},
        @NomeTitular = ${toSqlValue(dadosBancario.NomeTitular)},
        @CpfCnpj = ${toSqlValue(cpfCnpj)};
    `;

    try {
      logger.debug(
        { codPessoa, sql },
        'Executando stored procedure P_DADOS_BANCARIO_PESSOA_ESL_INCLUIR',
      );
      const result = await prisma.$executeRawUnsafe(sql);
      logger.info(
        { codPessoa, codBanco, codigo, result },
        'Dados bancários inseridos com sucesso na tabela DadosBancario',
      );
    } catch (error: any) {
      // Tratamento específico para erro de Foreign Key (547) - banco não existe na tabela Banco
      if (
        error.code === '547' ||
        (error.message &&
          (error.message.includes('FOREIGN KEY constraint') ||
            error.message.includes('FK_dbo.DadosBancario_Banco') ||
            error.message.includes('table "dbo.Banco"')))
      ) {
        logger.error(
          {
            error: error.message,
            codigo: codigoBanco,
            codBanco,
            codPessoa,
            mensagem:
              'Banco não encontrado na tabela Banco do AFS_INTEGRADOR. Verifique se o banco existe ou se o mapeamento está correto.',
          },
          'Erro de Foreign Key: Banco não existe na tabela Banco',
        );
        throw new Error(
          `Banco com código ${codigoBanco} (CodBanco: ${codBanco}) não encontrado na tabela Banco do AFS_INTEGRADOR. Verifique se o banco está cadastrado.`,
        );
      }

      // Se o erro for relacionado à tabela sisbanco (integração Senior), logar mas não falhar
      if (
        error.message &&
        (error.message.includes('sisbanco') ||
          error.message.includes(env.SENIOR_DATABASE) ||
          error.message.includes('SOFTRAN_BRASILMAXI') ||
          error.message.includes('SOFTRAN_BRASILMAXI') ||
          error.message.includes('Invalid object name'))
      ) {
        logger.warn(
          {
            error: error.message,
            codPessoa,
            codBanco,
            mensagem:
              'Erro na integração com Senior ERP (tabela sisbanco), mas dados foram inseridos na ESL',
          },
          'Aviso: Integração Senior ERP falhou, mas dados bancários foram salvos na ESL',
        );
        // Não lançar erro - os dados foram inseridos na ESL mesmo que a integração Senior tenha falhado
        return;
      }
      // Para outros erros, lançar normalmente
      logger.error(
        { error: error.message, codPessoa, codBanco },
        'Erro ao inserir dados bancários da pessoa',
      );
      throw error;
    }
  } catch (error: any) {
    logger.error(
      { error: error.message, codPessoa },
      'Erro ao processar dados bancários da pessoa',
    );
    throw error;
  }
}

/**
 * Insere chaves PIX da pessoa
 */
export async function inserirChavesPixPessoa(codPessoa: number, chavePix: any): Promise<void> {
  try {
    const sql = `
      EXEC dbo.P_CHAVESPIX_PESSOA_ESL_INCLUIR
        @CodPessoa = ${codPessoa},
        @CodTipoChave = ${chavePix.TipoChave?.CodTipoChave ?? 0},
        @ChavePix = ${toSqlValue(chavePix.ChavePix)},
        @NomeTitular = ${toSqlValue(chavePix.NomeTitular)};
    `;

    await prisma.$executeRawUnsafe(sql);
  } catch (error: any) {
    logger.error({ error: error.message, codPessoa }, 'Erro ao inserir chave PIX da pessoa');
    throw error;
  }
}

/**
 * Insere personagem da pessoa
 */
export async function inserirPessoaPersonagem(
  codPessoa: number,
  pessoaPersonagem: any,
): Promise<void> {
  try {
    const sql = `
      EXEC dbo.P_PERSONAGEM_PESSOA_ESL_INCLUIR
        @CodPessoa = ${codPessoa},
        @CodPersonagem = ${pessoaPersonagem.CodPessoaPersonagem};
    `;

    await prisma.$executeRawUnsafe(sql);
  } catch (error: any) {
    // Verificar se é erro de chave duplicada (PRIMARY KEY constraint)
    // Pode acontecer em condições de corrida ou quando personagem já existe
    const errorMessage = error.message || '';
    const errorCode = error.code || '';

    if (
      errorCode === '2627' ||
      errorMessage.includes('PRIMARY KEY') ||
      errorMessage.includes('duplicate key') ||
      errorMessage.includes('Violation of PRIMARY KEY constraint')
    ) {
      logger.warn(
        {
          codPessoa,
          codPersonagem: pessoaPersonagem.CodPessoaPersonagem,
          errorCode,
          errorMessage: errorMessage.substring(0, 200),
        },
        'Personagem já existe na tabela PessoaPersonagem (ignorando duplicata)',
      );
      // Não lançar erro - personagem já existe, é um caso aceitável
      return;
    }

    logger.error(
      { error: error.message, codPessoa, codPersonagem: pessoaPersonagem.CodPessoaPersonagem },
      'Erro ao inserir personagem da pessoa',
    );
    throw error;
  }
}

/**
 * Insere funcionário da pessoa
 */
export async function inserirPessoaFuncionario(
  codPessoa: number,
  pessoaFuncionario: any,
): Promise<void> {
  try {
    const sql = `
      EXEC dbo.P_PESSOA_FUNCIONARIO_ESL_INCLUIR
        @CdFuncionario = ${pessoaFuncionario.CdFuncionario ?? 0},
        @CodPessoa = ${codPessoa},
        @CodPessoaEsl = ${toSqlValue(pessoaFuncionario.CodPessoaEsl)},
        @DsNumCtps = ${toSqlValue(pessoaFuncionario.DsNumCtps)},
        @DsSerieCtps = ${toSqlValue(pessoaFuncionario.DsSerieCtps)},
        @DsUfCtps = ${toSqlValue(pessoaFuncionario.DsUfCtps)},
        @DtAdmissao = ${parseDate(pessoaFuncionario.DtAdmissao)},
        @VlSalarioAtual = ${pessoaFuncionario.VlSalarioAtual ?? 0},
        @DsCorPele = ${toSqlValue(pessoaFuncionario.DsCorPele)},
        @DtEmissaoCTPS = ${parseDate(pessoaFuncionario.DtEmissaoCTPS)},
        @VlSalarioInicial = ${pessoaFuncionario.VlSalarioInicial ?? 0};
    `;

    await prisma.$executeRawUnsafe(sql);
  } catch (error: any) {
    // Verificar se é erro de chave duplicada (PRIMARY KEY constraint)
    const errorMessage = error.message || '';
    const errorCode = error.code || '';

    if (
      errorCode === '2627' ||
      errorMessage.includes('PRIMARY KEY') ||
      errorMessage.includes('duplicate key') ||
      errorMessage.includes('Violation of PRIMARY KEY constraint')
    ) {
      logger.warn(
        {
          codPessoa,
          cdFuncionario: pessoaFuncionario.CdFuncionario,
          errorCode,
          errorMessage: errorMessage.substring(0, 200),
        },
        'Funcionário já existe na tabela PessoaFuncionario (ignorando duplicata)',
      );
      // Não lançar erro - funcionário já existe, é um caso aceitável
      return;
    }

    logger.error({ error: error.message, codPessoa }, 'Erro ao inserir funcionário da pessoa');
    throw error;
  }
}

/**
 * Insere motorista da pessoa
 */
export async function inserirPessoaMotorista(
  codPessoa: number,
  pessoaMotorista: any,
): Promise<void> {
  try {
    const sql = `
      EXEC dbo.P_PESSOA_MOTORISTA_ESL_INCLUIR
        @CodPessoa = ${codPessoa},
        @CodPessoaEsl = ${toSqlValue(pessoaMotorista.CodPessoaEsl)},
        @NumeroCNH = ${toSqlValue(pessoaMotorista.NumeroCNH)},
        @NumeroRegistro = ${toSqlValue(pessoaMotorista.NumeroRegistroCNH)},
        @DataPrimeiraCNH = ${parseDate(pessoaMotorista.DataPrimeiraCNH)},
        @DataEmissaoCNH = ${parseDate(pessoaMotorista.DataEmissaoCNH)},
        @DataValidadeCNH = ${parseDate(pessoaMotorista.DataVencimentoCNH)},
        @TipoCNH = ${toSqlValue(pessoaMotorista.TipoCNH)},
        @CidadeCNH = ${toSqlValue(pessoaMotorista.CidadeCNH)},
        @CodSegurancaCNH = ${toSqlValue(pessoaMotorista.CodSegurancaCNH)},
        @NumeroRenach = ${toSqlValue(pessoaMotorista.NumeroRenach)},
        @DataCadastro = ${parseDate(pessoaMotorista.DataCadastro)},
        @CodUsuarioCadastro = ${pessoaMotorista.CodUsuarioCadastro ?? 0},
        @UsuarioCadastro = ${toSqlValue(pessoaMotorista.UsuarioCadastro)};
    `;

    await prisma.$executeRawUnsafe(sql);
  } catch (error: any) {
    // Verificar se é erro de chave duplicada (PRIMARY KEY constraint)
    const errorMessage = error.message || '';
    const errorCode = error.code || '';

    if (
      errorCode === '2627' ||
      errorMessage.includes('PRIMARY KEY') ||
      errorMessage.includes('duplicate key') ||
      errorMessage.includes('Violation of PRIMARY KEY constraint')
    ) {
      logger.warn(
        {
          codPessoa,
          codPessoaEsl: pessoaMotorista.CodPessoaEsl,
          errorCode,
          errorMessage: errorMessage.substring(0, 200),
        },
        'Motorista já existe na tabela PessoaMotorista (ignorando duplicata)',
      );
      // Não lançar erro - motorista já existe, é um caso aceitável
      return;
    }

    logger.error({ error: error.message, codPessoa }, 'Erro ao inserir motorista da pessoa');
    throw error;
  }
}

/**
 * Exclui contatos da pessoa
 */
export async function excluirContatos(codPessoa: number): Promise<void> {
  try {
    const sql = `EXEC dbo.P_CONTATO_PESSOA_ESL_EXCLUIR @CodPessoa = ${codPessoa};`;
    await prisma.$executeRawUnsafe(sql);
  } catch (error: any) {
    logger.error({ error: error.message, codPessoa }, 'Erro ao excluir contatos da pessoa');
    throw error;
  }
}

/**
 * Exclui endereços da pessoa
 */
export async function excluirEnderecos(codPessoa: number): Promise<void> {
  try {
    const sql = `EXEC dbo.P_ENDERECO_PESSOA_ESL_EXCLUIR @CodPessoa = ${codPessoa};`;
    await prisma.$executeRawUnsafe(sql);
  } catch (error: any) {
    logger.error({ error: error.message, codPessoa }, 'Erro ao excluir endereços da pessoa');
    throw error;
  }
}

/**
 * Exclui dados bancários da pessoa
 */
export async function excluirDadosBancarios(codPessoa: number): Promise<void> {
  try {
    const sql = `EXEC dbo.P_DADOS_BANCARIO_PESSOA_ESL_EXCLUIR @CodPessoa = ${codPessoa};`;
    await prisma.$executeRawUnsafe(sql);
  } catch (error: any) {
    logger.error({ error: error.message, codPessoa }, 'Erro ao excluir dados bancários da pessoa');
    throw error;
  }
}

/**
 * Exclui chaves PIX da pessoa
 */
export async function excluirChavesPix(codPessoa: number): Promise<void> {
  try {
    const sql = `EXEC dbo.P_CHAVESPIX_PESSOA_ESL_EXCLUIR @CodPessoa = ${codPessoa};`;
    await prisma.$executeRawUnsafe(sql);
  } catch (error: any) {
    logger.error({ error: error.message, codPessoa }, 'Erro ao excluir chaves PIX da pessoa');
    throw error;
  }
}

/**
 * Exclui personagens da pessoa
 */
export async function excluirPessoaPersonagem(codPessoa: number): Promise<void> {
  try {
    const sql = `EXEC dbo.P_PERSONAGEM_PESSOA_ESL_EXCLUIR @CodPessoa = ${codPessoa};`;
    await prisma.$executeRawUnsafe(sql);
  } catch (error: any) {
    logger.error({ error: error.message, codPessoa }, 'Erro ao excluir personagens da pessoa');
    throw error;
  }
}

/**
 * Exclui funcionário da pessoa
 */
export async function excluirPessoaFuncionario(codPessoa: number): Promise<void> {
  try {
    const sql = `EXEC dbo.P_PESSOA_FUNCIONARIO_ESL_EXCLUIR @CodPessoa = ${codPessoa};`;
    await prisma.$executeRawUnsafe(sql);
  } catch (error: any) {
    logger.error({ error: error.message, codPessoa }, 'Erro ao excluir funcionário da pessoa');
    throw error;
  }
}

/**
 * Exclui motorista da pessoa
 */
export async function excluirPessoaMotorista(codPessoa: number): Promise<void> {
  try {
    const sql = `EXEC dbo.P_PESSOA_MOTORISTA_ESL_EXCLUIR @CodPessoa = ${codPessoa};`;
    await prisma.$executeRawUnsafe(sql);
  } catch (error: any) {
    logger.error({ error: error.message, codPessoa }, 'Erro ao excluir motorista da pessoa');
    throw error;
  }
}

/**
 * Interface para rastrear inserções e erros por tabela
 */
interface TabelaResultado {
  tabela: string;
  sucesso: boolean;
  erro?: string;
  detalhes?: any;
}

/**
 * Processa inserção/alteração completa da pessoa
 */
export async function inserirPessoa(pessoa: Pessoa): Promise<{
  status: boolean;
  mensagem: string;
  codPessoa?: number;
  tabelasInseridas?: string[];
  tabelasFalhadas?: Array<{ tabela: string; erro: string }>;
  created?: boolean; // Indica se foi criado (true) ou atualizado (false)
}> {
  const lockKey = pessoa.CodPessoaEsl;

  // Se já existe um processamento para esta pessoa (baseado em CodPessoaEsl), aguardar e retornar resultado
  if (pessoasEmProcessamentoEsl.has(lockKey)) {
    logger.warn(
      { codPessoaEsl: lockKey },
      'Requisição duplicada detectada, aguardando processamento existente',
    );
    try {
      const resultado = await pessoasEmProcessamentoEsl.get(lockKey);
      if (resultado) {
        return resultado;
      }
    } catch (error: any) {
      logger.error(
        { error: error.message, codPessoaEsl: lockKey },
        'Erro ao aguardar processamento duplicado',
      );
    }
  }

  // Criar promise de processamento e adicionar ao Map
  const processamentoPromise = (async () => {
    try {
      if (shouldBypassPessoaLegacyFlow()) {
        const localAnterior = await buscarPessoaLocal(pessoa.CodPessoaEsl);
        const codPessoaLocal = localAnterior.codPessoa ?? pessoa.CodPessoa ?? undefined;

        await prisma.pessoa.upsert({
          where: { id: pessoa.CodPessoaEsl },
          create: {
            id: pessoa.CodPessoaEsl,
            nomeRazaoSocial: pessoa.NomeRazaoSocial ?? null,
            nomeFantasia: pessoa.NomeFantasia ?? null,
            cpf: pessoa.Cpf ?? null,
            cnpj: pessoa.Cnpj ?? null,
            inscricaoMunicipal: pessoa.InscricaoMunicipal ?? null,
            inscricaoEstadual: pessoa.InscricaoEstadual ?? null,
            ativo: pessoa.Ativo ?? null,
            dataCadastro: pessoa.DataCadastro ?? null,
            usuarioCadastro: pessoa.UsuarioCadastro ?? null,
            payload: toPessoaPayloadText(pessoa, codPessoaLocal),
          },
          update: {
            nomeRazaoSocial: pessoa.NomeRazaoSocial ?? null,
            nomeFantasia: pessoa.NomeFantasia ?? null,
            cpf: pessoa.Cpf ?? null,
            cnpj: pessoa.Cnpj ?? null,
            inscricaoMunicipal: pessoa.InscricaoMunicipal ?? null,
            inscricaoEstadual: pessoa.InscricaoEstadual ?? null,
            ativo: pessoa.Ativo ?? null,
            dataCadastro: pessoa.DataCadastro ?? null,
            usuarioCadastro: pessoa.UsuarioCadastro ?? null,
            payload: toPessoaPayloadText(pessoa, codPessoaLocal),
          },
        });

        logger.warn(
          {
            codPessoaEsl: pessoa.CodPessoaEsl,
            persistidoLocal: true,
            ...buildBypassMetadata('pessoaService'),
          },
          'Fluxo legado de Pessoa (SQL Server/Senior) desativado em modo migracao',
        );

        return {
          status: true,
          mensagem:
            'Pessoa recebida em modo de migração. Dados mínimos persistidos no PostgreSQL; integração legada SQL Server/Senior desativada temporariamente.',
          created: !localAnterior.existe,
          ...(codPessoaLocal ? { codPessoa: codPessoaLocal } : {}),
          tabelasInseridas: ['Pessoa (PostgreSQL local - modo migracao)'],
          tabelasFalhadas: [],
        };
      }

      // Se PessoaPersonagemList for null ou vazio, atribui Cliente (código 3) como padrão
      if (!pessoa.PessoaPersonagemList || pessoa.PessoaPersonagemList.length === 0) {
        logger.info(
          { codPessoaEsl: pessoa.CodPessoaEsl },
          'PessoaPersonagemList vazio ou null - Atribuindo personagem Cliente (3) como padrão',
        );
        pessoa.PessoaPersonagemList = [
          {
            CodPessoaPersonagem: 3,
            Descricao: 'Cliente',
          },
        ];
      }

      if (!pessoa.PessoaPersonagemList || pessoa.PessoaPersonagemList.length === 0) {
        return {
          status: false,
          mensagem: 'PessoaPersonagemList não pode estar vazio',
          tabelasInseridas: [],
          tabelasFalhadas: [
            { tabela: 'PessoaPersonagemList', erro: 'PessoaPersonagemList não pode estar vazio' },
          ],
        };
      }

      let codPessoa = 0;
      const verificacao = await verificaExistenciaPessoa(pessoa.CodPessoaEsl);
      const existe = verificacao.existe;
      if (existe && verificacao.codPessoa) {
        codPessoa = verificacao.codPessoa;
      }

      // Arrays para rastrear inserções e erros
      const tabelasInseridas: string[] = [];
      const tabelasFalhadas: Array<{ tabela: string; erro: string }> = [];

      if (!existe || codPessoa <= 0) {
        // Verificar novamente se não foi criado entre a verificação e agora (condição de corrida)
        // Isso garante que mesmo com requisições simultâneas, não haja duplicação
        const verificacaoFinal = await verificaExistenciaPessoa(pessoa.CodPessoaEsl);
        if (
          verificacaoFinal.existe &&
          verificacaoFinal.codPessoa &&
          verificacaoFinal.codPessoa > 0
        ) {
          // Pessoa foi criada entre a verificação inicial e agora, usar UPDATE
          codPessoa = verificacaoFinal.codPessoa;
          pessoa.CodPessoa = codPessoa;
          logger.info(
            {
              codPessoa,
              codPessoaEsl: pessoa.CodPessoaEsl,
            },
            'Pessoa encontrada durante inserção (condição de corrida detectada), será atualizada ao invés de inserida',
          );

          // Fazer UPDATE ao invés de INSERT
          try {
            await alterarDadosPessoa(pessoa);
            tabelasInseridas.push('Pessoa (AFS_INTEGRADO)');
          } catch (updateError: any) {
            logger.error(
              {
                error: updateError.message,
                codPessoa,
                codPessoaEsl: pessoa.CodPessoaEsl,
              },
              'Erro ao atualizar pessoa (condição de corrida)',
            );
            tabelasFalhadas.push({
              tabela: 'Pessoa (AFS_INTEGRADO)',
              erro: updateError.message || 'Erro ao atualizar pessoa',
            });
          }

          // Processar operações adicionais em background
          processarOperacoesAdicionaisPessoa(
            codPessoa,
            pessoa,
            tabelasInseridas,
            tabelasFalhadas,
          ).catch((error: any) => {
            logger.error(
              { error: error.message, codPessoa },
              'Erro ao processar operações adicionais da pessoa em background',
            );
          });

          // Retornar com created: false pois foi atualizado
          return {
            status: true,
            mensagem: 'Pessoa atualizada com sucesso!',
            codPessoa,
            tabelasInseridas: ['Pessoa (AFS_INTEGRADO)'],
            tabelasFalhadas: [],
            created: false, // Foi atualizado, não criado
          };
        } else {
          // Inserir nova pessoa
          try {
            codPessoa = await inserirDadosPessoa(pessoa);
            if (codPessoa > 0) {
              tabelasInseridas.push('Pessoa (AFS_INTEGRADO)');
              // Verificar se foi integrado na Senior em background (não bloqueia resposta)
              (async () => {
                try {
                  const pessoaVerificada = await prisma.$queryRawUnsafe<
                    Array<{
                      FlgIntegradoSenior: number;
                    }>
                  >(`
                  SELECT FlgIntegradoSenior
                  FROM [AFS_INTEGRADOR].[dbo].[Pessoa]
                  WHERE CodPessoa = ${codPessoa}
                `);
                  if (
                    pessoaVerificada &&
                    pessoaVerificada.length > 0 &&
                    pessoaVerificada[0] &&
                    pessoaVerificada[0].FlgIntegradoSenior === 1
                  ) {
                    logger.debug(
                      { codPessoa },
                      'Pessoa integrada na Senior (FlgIntegradoSenior = 1)',
                    );
                  }
                } catch (verifyError: any) {
                  // Não é crítico, apenas não adiciona à lista
                  logger.debug(
                    { error: verifyError.message, codPessoa },
                    'Erro ao verificar integração Senior (não crítico)',
                  );
                }
              })();
            }
          } catch (error: any) {
            // Se houve erro na inserção, verificar se a pessoa foi criada mesmo assim
            const codPessoaVerificacao = await obterCodigoPessoa(pessoa.CodPessoaEsl);
            if (codPessoaVerificacao > 0) {
              codPessoa = codPessoaVerificacao;
              logger.warn(
                {
                  codPessoa,
                  codPessoaEsl: pessoa.CodPessoaEsl,
                  error: error.message,
                },
                'Erro ao inserir pessoa, mas registro encontrado na tabela (pode ter sido criado por outra requisição)',
              );
              tabelasInseridas.push('Pessoa (AFS_INTEGRADO) - encontrada após erro');
            } else {
              tabelasFalhadas.push({
                tabela: 'Pessoa (AFS_INTEGRADO)',
                erro: error.message || 'Erro desconhecido ao inserir pessoa principal',
              });
            }
          }
        }

        // Se a stored procedure retornou 0, mas pode ter inserido mesmo assim
        // Tentar buscar o CodPessoa pela tabela usando CodPessoaEsl
        if (codPessoa <= 0) {
          logger.warn(
            { codPessoaEsl: pessoa.CodPessoaEsl },
            'Stored procedure retornou 0 ou pessoa não foi inserida, tentando buscar CodPessoa na tabela',
          );
          codPessoa = await obterCodigoPessoa(pessoa.CodPessoaEsl);

          if (codPessoa > 0) {
            logger.info(
              { codPessoa, codPessoaEsl: pessoa.CodPessoaEsl },
              'CodPessoa encontrado na tabela após inserção (stored procedure retornou 0 mas registro existe)',
            );
          } else {
            return {
              status: false,
              mensagem: 'Registro não inserido, favor verificar log!',
              tabelasInseridas: [],
              tabelasFalhadas: [
                {
                  tabela: 'Pessoa (AFS_INTEGRADO)',
                  erro: 'Registro não inserido, favor verificar log!',
                },
              ],
            };
          }
        }

        // Verificação final: se a pessoa já existia antes da inserção ou foi criada duplicada,
        // garantir que usamos o registro mais recente e fazemos UPDATE
        // Isso garante que mesmo se houver múltiplos registros com o mesmo CodPessoaEsl,
        // sempre atualizaremos o mais recente (ORDER BY CodPessoa DESC)
        let foiAtualizadoNaVerificacaoFinal = false;
        const verificacaoPosInsercao = await verificaExistenciaPessoa(pessoa.CodPessoaEsl);
        if (
          verificacaoPosInsercao.existe &&
          verificacaoPosInsercao.codPessoa &&
          verificacaoPosInsercao.codPessoa > 0
        ) {
          // Sempre usar o CodPessoa mais recente encontrado na tabela
          const codPessoaExistente = verificacaoPosInsercao.codPessoa;
          if (codPessoaExistente !== codPessoa) {
            logger.warn(
              {
                codPessoaRetornado: codPessoa,
                codPessoaExistente: codPessoaExistente,
                codPessoaEsl: pessoa.CodPessoaEsl,
              },
              'Inconsistência detectada: CodPessoa retornado diferente do encontrado na tabela. Usando o mais recente e atualizando.',
            );
            codPessoa = codPessoaExistente;
            foiAtualizadoNaVerificacaoFinal = true;
          }
          // Sempre fazer UPDATE para garantir que os dados estão atualizados
          pessoa.CodPessoa = codPessoa;
          try {
            await alterarDadosPessoa(pessoa);
            logger.info(
              { codPessoa, codPessoaEsl: pessoa.CodPessoaEsl },
              'Pessoa atualizada após inserção (verificação final)',
            );
            foiAtualizadoNaVerificacaoFinal = true;
            // Atualizar lista de tabelas inseridas
            const index = tabelasInseridas.findIndex((t) => t.includes('Pessoa (AFS_INTEGRADO)'));
            if (index >= 0) {
              tabelasInseridas[index] = 'Pessoa (AFS_INTEGRADO) - atualizada';
            } else {
              tabelasInseridas.push('Pessoa (AFS_INTEGRADO) - atualizada');
            }
          } catch (updateError: any) {
            logger.error(
              {
                error: updateError.message,
                codPessoa,
                codPessoaEsl: pessoa.CodPessoaEsl,
              },
              'Erro ao atualizar pessoa após verificação final',
            );
            tabelasFalhadas.push({
              tabela: 'Pessoa (AFS_INTEGRADO)',
              erro: updateError.message || 'Erro ao atualizar pessoa',
            });
          }
        }

        // Processar operações adicionais em background (não bloqueia resposta)
        // TODAS as operações adicionais (funcionário, personagens, contatos, endereços, dados bancários, PIX, motorista, Senior)
        // são processadas em background para retornar resposta imediata
        processarOperacoesAdicionaisPessoa(
          codPessoa,
          pessoa,
          tabelasInseridas,
          tabelasFalhadas,
        ).catch((error: any) => {
          logger.error(
            { error: error.message, codPessoa },
            'Erro ao processar operações adicionais da pessoa em background (inserção)',
          );
        });

        // Retornar imediatamente após salvar pessoa principal
        // Se foi atualizado na verificação final, usar mensagem e created adequados
        const foiRealmenteCriado = !foiAtualizadoNaVerificacaoFinal;
        return {
          status: true,
          mensagem: foiRealmenteCriado
            ? 'Pessoa inserida com sucesso! Operações adicionais sendo processadas em background.'
            : 'Pessoa atualizada com sucesso! Operações adicionais sendo processadas em background.',
          codPessoa,
          tabelasInseridas: ['Pessoa (AFS_INTEGRADO)'],
          tabelasFalhadas: [],
          created: foiRealmenteCriado, // true se foi criado, false se foi atualizado
        };
      } else {
        // Alterar pessoa existente
        // Usar o codPessoa já obtido na verificação, ou buscar novamente
        if (!codPessoa || codPessoa <= 0) {
          codPessoa = await obterCodigoPessoa(pessoa.CodPessoaEsl);
        }

        if (codPessoa <= 0) {
          // Se ainda não encontrou, tentar buscar diretamente na tabela
          const verificacaoFinal = await verificaExistenciaPessoa(pessoa.CodPessoaEsl);
          if (
            verificacaoFinal.existe &&
            verificacaoFinal.codPessoa &&
            verificacaoFinal.codPessoa > 0
          ) {
            codPessoa = verificacaoFinal.codPessoa;
          } else {
            return {
              status: false,
              mensagem: 'Código da pessoa não encontrado!',
              tabelasInseridas: [],
              tabelasFalhadas: [{ tabela: 'Pessoa', erro: 'Código da pessoa não encontrado' }],
            };
          }
        }

        pessoa.CodPessoa = codPessoa;
        logger.info(
          {
            codPessoa,
            codPessoaEsl: pessoa.CodPessoaEsl,
          },
          'Pessoa existente detectada, será atualizada ao invés de inserida',
        );
        try {
          await alterarDadosPessoa(pessoa);
          tabelasInseridas.push('Pessoa (AFS_INTEGRADO)');
        } catch (error: any) {
          // Log do erro, mas continua processando (não retorna erro)
          logger.warn(
            {
              error: error.message,
              codPessoa,
              codPessoaEsl: pessoa.CodPessoaEsl,
            },
            'Aviso: Erro ao alterar pessoa principal, mas continuando processamento',
          );
          tabelasFalhadas.push({
            tabela: 'Pessoa (AFS_INTEGRADO)',
            erro: error.message || 'Erro desconhecido ao alterar pessoa principal',
          });
          // Continua e retorna sucesso mesmo com erro (conforme solicitado pelo usuário)
        }

        // Processar operações adicionais em background (não bloqueia resposta)
        // Essas operações são importantes mas não críticas para a resposta imediata
        processarOperacoesAdicionaisPessoa(
          codPessoa,
          pessoa,
          tabelasInseridas,
          tabelasFalhadas,
        ).catch((error: any) => {
          logger.error(
            { error: error.message, codPessoa },
            'Erro ao processar operações adicionais da pessoa em background',
          );
        });

        // Retornar imediatamente após salvar pessoa principal
        // Sempre retorna sucesso quando pessoa existe (mesmo que haja erros menores)
        return {
          status: true,
          mensagem: 'Pessoa atualizada com sucesso!',
          codPessoa,
          tabelasInseridas: ['Pessoa (AFS_INTEGRADO)'],
          tabelasFalhadas: [],
          created: false, // Indica que foi atualizado (não criado)
        };
      }
    } catch (error: any) {
      logger.error(
        { error: error.message, codPessoaEsl: pessoa.CodPessoaEsl },
        'Erro ao processar pessoa',
      );
      return {
        status: false,
        mensagem: error.message || 'Erro ao processar pessoa',
        tabelasInseridas: [],
        tabelasFalhadas: [{ tabela: 'Pessoa', erro: error.message || 'Erro ao processar pessoa' }],
      };
    } finally {
      // Remover do Map quando terminar (sucesso ou erro)
      pessoasEmProcessamentoEsl.delete(lockKey);
    }
  })();

  // Adicionar ao Map ANTES de executar
  pessoasEmProcessamentoEsl.set(lockKey, processamentoPromise);

  // Aguardar e retornar resultado
  return await processamentoPromise;
}

/**
 * Processa operações adicionais da pessoa em background (não bloqueia resposta)
 * Usa lock para evitar processamento duplicado quando múltiplas requisições chegam simultaneamente
 */
async function processarOperacoesAdicionaisPessoa(
  codPessoa: number,
  pessoa: Pessoa,
  tabelasInseridas: string[],
  tabelasFalhadas: Array<{ tabela: string; erro: string }>,
): Promise<void> {
  // Verificar se já está sendo processado - evitar processamento duplicado
  const processamentoExistente = pessoasEmProcessamento.get(codPessoa);
  if (processamentoExistente) {
    logger.info(
      { codPessoa },
      'Pessoa já está sendo processada, aguardando processamento existente',
    );
    try {
      await processamentoExistente;
    } catch (error: any) {
      // Ignorar erros do processamento anterior
      logger.warn(
        { error: error.message, codPessoa },
        'Erro no processamento anterior, iniciando novo',
      );
    }
    // Verificar novamente após aguardar
    if (pessoasEmProcessamento.has(codPessoa)) {
      logger.info(
        { codPessoa },
        'Processamento anterior ainda em andamento, pulando processamento duplicado',
      );
      return;
    }
  }

  // Criar promise de processamento e adicionar ao Map
  const processamentoPromise = (async () => {
    try {
      await processarOperacoesAdicionaisPessoaInternal(
        codPessoa,
        pessoa,
        tabelasInseridas,
        tabelasFalhadas,
      );
    } finally {
      // Remover do Map quando terminar (sucesso ou erro)
      pessoasEmProcessamento.delete(codPessoa);
    }
  })();

  pessoasEmProcessamento.set(codPessoa, processamentoPromise);
  await processamentoPromise;
}

/**
 * Processamento interno das operações adicionais (sem lock)
 */
async function processarOperacoesAdicionaisPessoaInternal(
  codPessoa: number,
  pessoa: Pessoa,
  tabelasInseridas: string[],
  tabelasFalhadas: Array<{ tabela: string; erro: string }>,
): Promise<void> {
  // Verificar se foi integrado na Senior
  try {
    const pessoaVerificada = await prisma.$queryRawUnsafe<
      Array<{
        FlgIntegradoSenior: number;
      }>
    >(`
      SELECT FlgIntegradoSenior
      FROM [AFS_INTEGRADOR].[dbo].[Pessoa]
      WHERE CodPessoa = ${codPessoa}
    `);
    if (
      pessoaVerificada &&
      pessoaVerificada.length > 0 &&
      pessoaVerificada[0] &&
      pessoaVerificada[0].FlgIntegradoSenior === 1
    ) {
      tabelasInseridas.push('siscliente (Senior)');
    }
  } catch (verifyError: any) {
    // Não é crítico
  }

  // Inserir/atualizar funcionário
  if (pessoa.PessoaFuncionario) {
    try {
      // Tentar excluir primeiro (pode não existir em novas inserções - ignorar erro)
      try {
        await excluirPessoaFuncionario(codPessoa);
      } catch (excludeError: any) {
        // Ignorar erro de exclusão - pode não existir ainda
      }
      pessoa.PessoaFuncionario.CdFuncionario =
        pessoa.cdFuncionario ?? pessoa.PessoaFuncionario.CdFuncionario;
      pessoa.PessoaFuncionario.CodPessoa = pessoa.CodPessoa;
      pessoa.PessoaFuncionario.CodPessoaEsl = pessoa.CodPessoaEsl;
      await inserirPessoaFuncionario(codPessoa, pessoa.PessoaFuncionario);
      tabelasInseridas.push('PessoaFuncionario');
    } catch (error: any) {
      // Ignorar erros de chave duplicada em condições de corrida
      if (error.message?.includes('PRIMARY KEY') || error.message?.includes('duplicate key')) {
        logger.warn({ codPessoa }, 'Funcionário já existe (ignorando duplicata)');
      } else {
        tabelasFalhadas.push({
          tabela: 'PessoaFuncionario',
          erro: error.message || 'Erro desconhecido',
        });
      }
    }
  }

  // Inserir/atualizar personagens
  if (pessoa.PessoaPersonagemList && pessoa.PessoaPersonagemList.length > 0) {
    try {
      // Tentar excluir primeiro (pode não existir em novas inserções - ignorar erro)
      try {
        await excluirPessoaPersonagem(codPessoa);
      } catch (excludeError: any) {
        // Ignorar erro de exclusão - pode não existir ainda
      }
      for (const personagem of pessoa.PessoaPersonagemList) {
        try {
          await inserirPessoaPersonagem(codPessoa, personagem);
          // inserirPessoaPersonagem agora trata erros de chave duplicada internamente
        } catch (error: any) {
          // Apenas logar outros erros (não relacionados a chave duplicada)
          logger.warn(
            {
              codPessoa,
              personagem: personagem.CodPessoaPersonagem,
              error: error.message,
            },
            'Erro ao inserir personagem (não é chave duplicada)',
          );
        }
      }
      if (!tabelasInseridas.includes('PessoaPersonagem')) {
        tabelasInseridas.push('PessoaPersonagem');
      }
    } catch (error: any) {
      if (!tabelasFalhadas.some((t) => t.tabela === 'PessoaPersonagem')) {
        tabelasFalhadas.push({
          tabela: 'PessoaPersonagem',
          erro: error.message || 'Erro desconhecido',
        });
      }
    }
  }

  // Inserir/atualizar contatos
  if (pessoa.ContatoList && pessoa.ContatoList.length > 0) {
    try {
      // Tentar excluir primeiro (pode não existir em novas inserções - ignorar erro)
      try {
        await excluirContatos(codPessoa);
      } catch (excludeError: any) {
        // Ignorar erro de exclusão - pode não existir ainda
      }
      for (const contato of pessoa.ContatoList) {
        try {
          await inserirContatosPessoa(codPessoa, contato);
        } catch (error: any) {
          // Ignorar erros de chave duplicada
          if (
            !error.message?.includes('PRIMARY KEY') &&
            !error.message?.includes('duplicate key')
          ) {
            throw error;
          }
        }
      }
      if (!tabelasInseridas.includes('Contato')) {
        tabelasInseridas.push('Contato');
      }
    } catch (error: any) {
      if (!tabelasFalhadas.some((t) => t.tabela === 'Contato')) {
        tabelasFalhadas.push({ tabela: 'Contato', erro: error.message || 'Erro desconhecido' });
      }
    }
  }

  // Inserir/atualizar endereços
  if (pessoa.EnderecoList && pessoa.EnderecoList.length > 0) {
    try {
      // Tentar excluir primeiro (pode não existir em novas inserções - ignorar erro)
      try {
        await excluirEnderecos(codPessoa);
      } catch (excludeError: any) {
        // Ignorar erro de exclusão - pode não existir ainda
      }
      for (const endereco of pessoa.EnderecoList) {
        try {
          await inserirEnderecosPessoa(codPessoa, endereco);
        } catch (error: any) {
          // Ignorar erros de chave duplicada
          if (
            !error.message?.includes('PRIMARY KEY') &&
            !error.message?.includes('duplicate key')
          ) {
            throw error;
          }
        }
      }
      if (!tabelasInseridas.includes('Endereco')) {
        tabelasInseridas.push('Endereco');
      }
    } catch (error: any) {
      if (!tabelasFalhadas.some((t) => t.tabela === 'Endereco')) {
        tabelasFalhadas.push({ tabela: 'Endereco', erro: error.message || 'Erro desconhecido' });
      }
    }
  }

  // Inserir/atualizar dados bancários
  logger.info(
    {
      codPessoa,
      temDadosBancarioList: !!pessoa.DadosBancarioList,
      totalDadosBancarios: pessoa.DadosBancarioList?.length || 0,
      dadosBancarioList: pessoa.DadosBancarioList
        ? JSON.stringify(pessoa.DadosBancarioList)
        : 'null',
    },
    'Verificando dados bancários para inserção',
  );

  if (pessoa.DadosBancarioList && pessoa.DadosBancarioList.length > 0) {
    logger.info(
      { codPessoa, totalDadosBancarios: pessoa.DadosBancarioList.length },
      'Iniciando inserção/atualização de dados bancários',
    );
    try {
      // Tentar excluir primeiro (pode não existir em novas inserções - ignorar erro)
      try {
        await excluirDadosBancarios(codPessoa);
        logger.info({ codPessoa }, 'Dados bancários excluídos com sucesso');
      } catch (error: any) {
        // Ignorar erro de exclusão - pode não existir ainda
        logger.debug(
          { error: error.message, codPessoa },
          'Dados bancários não existem para exclusão (nova inserção)',
        );
      }
      let sucessoBancario = false;
      let errosBancario: string[] = [];
      for (const dadosBancario of pessoa.DadosBancarioList) {
        try {
          await inserirDadosBancariosPessoa(codPessoa, dadosBancario);
          sucessoBancario = true;
          logger.info({ codPessoa }, 'Dados bancários reinseridos com sucesso');
        } catch (error: any) {
          errosBancario.push(error.message || 'Erro desconhecido');
          logger.error(
            {
              error: error.message,
              stack: error.stack,
              codPessoa,
              dadosBancario: JSON.stringify(dadosBancario),
            },
            'Erro ao reinserir dados bancários - continuando com próximo item',
          );
        }
      }
      if (sucessoBancario) {
        if (!tabelasInseridas.includes('DadosBancario')) {
          tabelasInseridas.push('DadosBancario');
        }
      }
      if (errosBancario.length > 0 && !sucessoBancario) {
        tabelasFalhadas.push({
          tabela: 'DadosBancario',
          erro: errosBancario.join('; '),
        });
      }
    } catch (error: any) {
      if (!tabelasFalhadas.some((t) => t.tabela === 'DadosBancario')) {
        tabelasFalhadas.push({
          tabela: 'DadosBancario',
          erro: error.message || 'Erro desconhecido',
        });
      }
    }
  }

  // Inserir/atualizar chaves PIX
  if (pessoa.ChavesPixList && pessoa.ChavesPixList.length > 0) {
    try {
      // Tentar excluir primeiro (pode não existir em novas inserções - ignorar erro)
      try {
        await excluirChavesPix(codPessoa);
      } catch (excludeError: any) {
        // Ignorar erro de exclusão - pode não existir ainda
      }
      for (const chavePix of pessoa.ChavesPixList) {
        try {
          await inserirChavesPixPessoa(codPessoa, chavePix);
        } catch (error: any) {
          // Ignorar erros de chave duplicada
          if (
            !error.message?.includes('PRIMARY KEY') &&
            !error.message?.includes('duplicate key')
          ) {
            throw error;
          }
        }
      }
      if (!tabelasInseridas.includes('ChavesPix')) {
        tabelasInseridas.push('ChavesPix');
      }
    } catch (error: any) {
      if (!tabelasFalhadas.some((t) => t.tabela === 'ChavesPix')) {
        tabelasFalhadas.push({ tabela: 'ChavesPix', erro: error.message || 'Erro desconhecido' });
      }
    }
  }

  // Inserir/atualizar motorista
  if (pessoa.PessoaMotorista) {
    try {
      // Tentar excluir primeiro (pode não existir em novas inserções - ignorar erro)
      try {
        await excluirPessoaMotorista(codPessoa);
      } catch (excludeError: any) {
        // Ignorar erro de exclusão - pode não existir ainda
      }
      await inserirPessoaMotorista(codPessoa, pessoa.PessoaMotorista);
      tabelasInseridas.push('PessoaMotorista');
    } catch (error: any) {
      // Ignorar erros de chave duplicada
      if (error.message?.includes('PRIMARY KEY') || error.message?.includes('duplicate key')) {
        logger.warn({ codPessoa }, 'Motorista já existe (ignorando duplicata)');
      } else {
        tabelasFalhadas.push({
          tabela: 'PessoaMotorista',
          erro: error.message || 'Erro desconhecido',
        });
      }
    }
  }

  // Integrar pessoa com Senior APÓS inserir todos os dados (incluindo dados bancários)
  // Aguardar um pequeno delay para garantir que os dados bancários foram commitados no banco
  logger.info({ codPessoa }, 'Aguardando inserção de dados bancários antes de integrar com Senior');

  // Aguardar um pequeno delay para garantir que os dados foram commitados
  await new Promise((resolve) => setTimeout(resolve, 500));

  // A integração é feita em background após inserir todos os dados relacionados
  logger.info(
    { codPessoa },
    'Agendando integração com Senior em background (após inserção de dados relacionados)',
  );
  inserirDadosPessoaSISCLI(codPessoa)
    .then(async () => {
      await alterarIntegracaoPessoa(codPessoa);
      logger.info({ codPessoa }, 'Pessoa integrada com Senior com sucesso (background)');
    })
    .catch((error: any) => {
      // Não falhar o processo se a integração Senior falhar
      logger.warn(
        {
          error: error.message,
          codPessoa,
          mensagem:
            'Erro na integração com Senior (background), mas pessoa foi atualizada no AFS_INTEGRADO',
        },
        'Aviso: Integração Senior falhou em background, mas dados foram salvos',
      );
    });
}
