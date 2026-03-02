// Tipos para CT-e baseados no CteRepository.cs

export interface CteData {
  id: number;
  external_id: number;
  authorization_number: number;
  status: string;
  xml: string;
  event_xml?: string | null;
  processed: boolean;
  created_at: Date;
  updated_at: Date;
}

export interface ContasReceberPayload {
  cte: CteData;
}
