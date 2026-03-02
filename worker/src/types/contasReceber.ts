export interface Fatura {
  id: number;
  external_id: number;
  type: string | null;
  document: string | null;
  issue_date: Date | null;
  due_date: Date | null;
  value: number;
  installment_period: string | null;
  comments: string | null;
  installment_count: number;
}

export interface Filial {
  id: number;
  external_id: number;
  person_id: number | null;
  nickname: string | null;
  cnpj: string | null;
}

export interface Cliente {
  id: number;
  external_id: number;
  name: string | null;
  type: string | null;
  cnpj: string | null;
  cpf: string | null;
  person_type: string | null;
  email: string | null;
  phone: string | null;
}

export interface ContaContabil {
  id: number;
  external_id: number;
  name: string | null;
  code_cahe: string | null;
}

export interface CentroCusto {
  id: number;
  external_id: number;
  credit_invoice_id: number | null;
  cost_center_id: number | null;
  name: string | null;
}

export interface Parcela {
  id: number;
  external_id: number;
  credit_invoice_id: number | null;
  position: number | null;
  due_date: Date | null;
  value: number;
  interest_value: number;
  discount_value: number;
  payment_method: string | null;
  comments: string | null;
  status: string | null;
  payment_date: Date | null;
}

export interface FaturaItens {
  id: number;
  external_id: number;
  credit_invoice_id: number | null;
  freight_id: number | null;
  cte_key: string | null;
  cte_number: number | null;
  cte_series: number | null;
  payer_name: string | null;
  draft_number: string | null;
  nfse_number: string | null;
  nfse_series: string | null;
  total: number;
  type: string | null;
}

export interface ContasReceberCompleto {
  document: string;
  Fatura: Fatura;
  Filial: Filial;
  Cliente: Cliente;
  ContaContabil: ContaContabil;
  CentroCusto: CentroCusto | null;
  Parcelas: Parcela[];
  FaturaItens: FaturaItens[];
}

export interface SISCliFa {
  CdRepresentante: string | null;
  CdPortador: number;
  CdCondicaoPagamento: number;
  cdCentroCusto: number;
  CdEspeciePagar: number;
  CdCarteiraPagar: number;
  CdPlanoContaAPagar: number;
}
