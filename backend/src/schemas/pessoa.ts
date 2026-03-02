import { z } from 'zod';

// Schema para TipoPessoa
export const tipoPessoaSchema = z.object({
  CodTipoPessoa: z.number(),
  Descricao: z.string().optional(),
});

// Schema para PessoaPersonagem
export const pessoaPersonagemSchema = z.object({
  CodPessoaPersonagem: z.number(),
  Descricao: z.string().optional(),
});

// Schema para Contato
export const contatoSchema = z.object({
  CodContato: z.number().optional(),
  CodTipoContato: z.number().optional(),
  Nome: z.string().optional(),
  Cpf: z.string().optional(),
  Ddd: z.string().optional(),
  Telefone: z.string().optional(),
  Celular: z.string().optional(),
  Departamento: z.string().optional(),
  Email: z.string().optional(),
  Site: z.string().optional(),
  Observacao: z.string().optional(),
});

// Schema para Endereco
export const enderecoSchema = z.object({
  CodEndereco: z.number().optional(),
  CodTipoEndereco: z.number().optional(),
  Cep: z.string().optional(),
  Logradouro: z.string().optional(),
  Numero: z.string().optional(),
  Complemento: z.string().optional(),
  Bairro: z.string().optional(),
  Cidade: z.string().optional(),
  Estado: z.string().optional(),
});

// Schema para Banco
export const bancoSchema = z.object({
  CodBanco: z.number().optional(),
  Codigo: z.number().optional(),
  Nome: z.string().optional(),
});

// Schema para TipoConta
export const tipoContaSchema = z.object({
  CodTipoConta: z.number().optional(),
  Descricao: z.string().optional(),
});

// Schema para Titularidade
export const titularidadeSchema = z.object({
  CodTitularidade: z.number().optional(),
  Descricao: z.string().optional(),
});

// Schema para DadosBancario
export const dadosBancarioSchema = z.object({
  CodDadosBancario: z.number().optional(),
  Banco: bancoSchema.optional(),
  TipoConta: tipoContaSchema.optional(),
  Titularidade: titularidadeSchema.optional(),
  NumAgencia: z.string().optional(),
  NumConta: z.string().optional(),
  NomeTitular: z.string().optional(),
  CpfCnpj: z.string().optional(),
});

// Schema para TipoChave
export const tipoChaveSchema = z.object({
  CodTipoChave: z.number().optional(),
  Descricao: z.string().optional(),
});

// Schema para ChavesPix
export const chavesPixSchema = z.object({
  CodChavesPix: z.number().optional(),
  TipoChave: tipoChaveSchema.optional(),
  ChavePix: z.string().optional(),
  NomeTitular: z.string().optional(),
});

// Schema para PessoaFuncionario
export const pessoaFuncionarioSchema = z.object({
  CodPessoaFuncionario: z.number().optional(),
  CdFuncionario: z.number().optional(),
  CodPessoa: z.number().optional(),
  CodPessoaEsl: z.string().optional(),
  DsNumCtps: z.string().optional(),
  DsSerieCtps: z.string().optional(),
  DsUfCtps: z.string().optional(),
  DtAdmissao: z.string().nullable().optional(),
  VlSalarioAtual: z.number().optional(),
  DsCorPele: z.string().optional(),
  DtEmissaoCTPS: z.string().nullable().optional(),
  VlSalarioInicial: z.number().optional(),
});

// Schema para PessoaMotorista
export const pessoaMotoristaSchema = z.object({
  CodPessoaMotorista: z.number().optional(),
  CodPessoa: z.number().optional(),
  CodPessoaEsl: z.string().optional(),
  NumeroCNH: z.string().optional(),
  NumeroRegistroCNH: z.string().optional(),
  DataPrimeiraCNH: z.string().nullable().optional(),
  DataEmissaoCNH: z.string().nullable().optional(),
  DataVencimentoCNH: z.string().nullable().optional(),
  TipoCNH: z.string().optional(),
  CidadeCNH: z.string().optional(),
  CodSegurancaCNH: z.string().optional(),
  NumeroRenach: z.string().optional(),
  DataCadastro: z.string().nullable().optional(),
  CodUsuarioCadastro: z.number().optional(),
  UsuarioCadastro: z.string().optional(),
});

// Schema principal para Pessoa
export const pessoaSchema = z.object({
  CodPessoa: z.number().optional(),
  CodPessoaEsl: z.string(),
  TipoPessoa: tipoPessoaSchema,
  CodNaturezaOperacao: z.number().optional(),
  DescNaturezaOperacao: z.string().optional(),
  CodFilialResponsavel: z.string().optional(),
  DescFilialResponsavel: z.string().optional(),
  NomeRazaoSocial: z.string().optional(),
  NomeFantasia: z.string().optional(),
  DataFundacao: z.string().nullable().optional(),
  Cpf: z.string().optional(),
  Cnpj: z.string().optional(),
  RG: z.string().optional(),
  CidadeExpedicaoRG: z.string().optional(),
  OrgaoExpedidorRG: z.string().optional(),
  DataEmissaoRG: z.string().nullable().optional(),
  Sexo: z.boolean().optional(),
  EstadoCivil: z.string().optional(),
  DataNascimento: z.string().nullable().optional(),
  CidadeNascimento: z.string().optional(),
  InscricaoMunicipal: z.string().optional(),
  InscricaoEstadual: z.string().optional(),
  DocumentoExterior: z.string().optional(),
  NumeroPISPASEP: z.string().optional(),
  RNTRC: z.string().optional(),
  DataValidadeRNTRC: z.string().nullable().optional(),
  NomePai: z.string().optional(),
  NomeMae: z.string().optional(),
  Contrinuinte: z.boolean().optional(),
  Site: z.string().optional(),
  CodigoCNAE: z.string().optional(),
  RegimeFiscal: z.string().optional(),
  CodigoSuframa: z.string().optional(),
  Observacao: z.string().optional(),
  DataCadastro: z.string().nullable().optional(),
  CodUsuarioCadastro: z.number().optional(),
  UsuarioCadastro: z.string().optional(),
  Ativo: z.boolean().optional(),
  PessoaPersonagemList: z.array(pessoaPersonagemSchema).optional(),
  ContatoList: z.array(contatoSchema).optional(),
  EnderecoList: z.array(enderecoSchema).optional(),
  DadosBancarioList: z.array(dadosBancarioSchema).optional(),
  ChavesPixList: z.array(chavesPixSchema).optional(),
  PessoaFuncionario: pessoaFuncionarioSchema.optional(),
  PessoaMotorista: pessoaMotoristaSchema.optional(),
  cdFuncionario: z.number().optional(),
});

// Tipo TypeScript derivado do schema
export type Pessoa = z.infer<typeof pessoaSchema>;
export type TipoPessoa = z.infer<typeof tipoPessoaSchema>;
export type PessoaPersonagem = z.infer<typeof pessoaPersonagemSchema>;
export type Contato = z.infer<typeof contatoSchema>;
export type Endereco = z.infer<typeof enderecoSchema>;
export type DadosBancario = z.infer<typeof dadosBancarioSchema>;
export type ChavesPix = z.infer<typeof chavesPixSchema>;
export type PessoaFuncionario = z.infer<typeof pessoaFuncionarioSchema>;
export type PessoaMotorista = z.infer<typeof pessoaMotoristaSchema>;

