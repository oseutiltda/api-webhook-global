import { logger } from '../utils/logger';

/**
 * Funções auxiliares para extração mais precisa de dados do XML do CT-e
 */

/**
 * Extrai texto de uma tag específica dentro de um contexto
 */
export const extrairTextoDoContexto = (
  xml: string,
  contexto: string,
  tag: string,
): string | null => {
  try {
    const contextoMatch = xml.match(
      new RegExp(`<${contexto}[^>]*>([\\s\\S]*?)</${contexto}>`, 'i'),
    );
    if (contextoMatch && contextoMatch[1]) {
      const tagMatch = contextoMatch[1].match(new RegExp(`<${tag}[^>]*>(.*?)</${tag}>`, 's'));
      if (tagMatch && tagMatch[1]) {
        return tagMatch[1].trim();
      }
    }
  } catch (error: any) {
    logger.warn({ error: error.message, contexto, tag }, 'Erro ao extrair texto do contexto');
  }
  return null;
};

/**
 * Extrai valor numérico de uma tag específica dentro de um contexto
 */
export const extrairValorDoContexto = (xml: string, contexto: string, tag: string): number => {
  try {
    const texto = extrairTextoDoContexto(xml, contexto, tag);
    if (texto) {
      const valor = parseFloat(texto);
      if (!Number.isNaN(valor)) {
        return valor;
      }
    }
  } catch (error: any) {
    logger.warn({ error: error.message, contexto, tag }, 'Erro ao extrair valor do contexto');
  }
  return 0;
};

/**
 * Extrai CEP do remetente
 */
export const extrairCepRemetente = (xml: string): string => {
  const cep =
    extrairTextoDoContexto(xml, 'enderReme', 'CEP') || extrairTextoDoContexto(xml, 'rem', 'CEP');
  return cep || '00000000';
};

/**
 * Extrai CEP do destinatário
 */
export const extrairCepDestinatario = (xml: string): string => {
  const cep =
    extrairTextoDoContexto(xml, 'enderDest', 'CEP') || extrairTextoDoContexto(xml, 'dest', 'CEP');
  return cep || '00000000';
};

/**
 * Extrai CEP do emitente
 */
export const extrairCepEmitente = (xml: string): string => {
  const cep =
    extrairTextoDoContexto(xml, 'enderEmit', 'CEP') || extrairTextoDoContexto(xml, 'emit', 'CEP');
  return cep || '00000000';
};

/**
 * Extrai inscrição estadual do remetente
 */
export const extrairIERemetente = (xml: string): string | null => {
  return extrairTextoDoContexto(xml, 'rem', 'IE');
};

/**
 * Extrai inscrição estadual do destinatário
 */
export const extrairIEDestinatario = (xml: string): string | null => {
  return extrairTextoDoContexto(xml, 'dest', 'IE');
};

/**
 * Extrai peso real (tpMed = "Peso real")
 */
export const extrairPesoReal = (xml: string): number => {
  try {
    const infQMatches = xml.matchAll(/<infQ>[\s\S]*?<\/infQ>/gi);
    for (const infQMatch of infQMatches) {
      const infQ = infQMatch[0];
      const tpMed = infQ.match(/<tpMed>(.*?)<\/tpMed>/);
      if (tpMed && tpMed[1]?.toLowerCase().includes('peso real')) {
        const qCarga = infQ.match(/<qCarga>(.*?)<\/qCarga>/);
        if (qCarga && qCarga[1]) {
          const valor = parseFloat(qCarga[1].trim());
          if (!Number.isNaN(valor)) {
            return valor;
          }
        }
      }
    }
  } catch (error: any) {
    logger.warn({ error: error.message }, 'Erro ao extrair peso real');
  }
  return 0;
};

/**
 * Extrai peso cubado (tpMed = "Peso cubado")
 */
export const extrairPesoCubado = (xml: string): number => {
  try {
    const infQMatches = xml.matchAll(/<infQ>[\s\S]*?<\/infQ>/gi);
    for (const infQMatch of infQMatches) {
      const infQ = infQMatch[0];
      const tpMed = infQ.match(/<tpMed>(.*?)<\/tpMed>/);
      if (tpMed && tpMed[1]?.toLowerCase().includes('peso cubado')) {
        const qCarga = infQ.match(/<qCarga>(.*?)<\/qCarga>/);
        if (qCarga && qCarga[1]) {
          const valor = parseFloat(qCarga[1].trim());
          if (!Number.isNaN(valor)) {
            return valor;
          }
        }
      }
    }
  } catch (error: any) {
    logger.warn({ error: error.message }, 'Erro ao extrair peso cubado');
  }
  return 0;
};

/**
 * Extrai quantidade de volumes (tpMed = "Volumes" e cUnid = "03")
 */
export const extrairQuantidadeVolumes = (xml: string): number => {
  try {
    const infQMatches = xml.matchAll(/<infQ>[\s\S]*?<\/infQ>/gi);
    for (const infQMatch of infQMatches) {
      const infQ = infQMatch[0];
      const cUnid = infQ.match(/<cUnid>(.*?)<\/cUnid>/);
      const tpMed = infQ.match(/<tpMed>(.*?)<\/tpMed>/);
      if (cUnid && cUnid[1] === '03' && tpMed && tpMed[1]?.toLowerCase().includes('volume')) {
        const qCarga = infQ.match(/<qCarga>(.*?)<\/qCarga>/);
        if (qCarga && qCarga[1]) {
          const valor = parseFloat(qCarga[1].trim());
          if (!Number.isNaN(valor)) {
            return Math.round(valor); // Volumes sempre inteiro
          }
        }
      }
    }
  } catch (error: any) {
    logger.warn({ error: error.message }, 'Erro ao extrair quantidade de volumes');
  }
  return 0;
};

/**
 * Extrai valor da carga (vCarga dentro de infCarga)
 */
export const extrairValorCarga = (xml: string): number => {
  return extrairValorDoContexto(xml, 'infCarga', 'vCarga');
};

/**
 * Extrai componentes do frete (Comp dentro de vPrest)
 */
export interface ComponentesFrete {
  vlFretePeso: number;
  vlFreteValor: number; // Ad Valorem
  vlPedagio: number;
  vlICMS: number;
  vlOutros: number;
}

export const extrairComponentesFrete = (xml: string): ComponentesFrete => {
  const componentes: ComponentesFrete = {
    vlFretePeso: 0,
    vlFreteValor: 0,
    vlPedagio: 0,
    vlICMS: 0,
    vlOutros: 0,
  };

  try {
    const vPrestMatch = xml.match(/<vPrest>[\s\S]*?<\/vPrest>/i);
    if (vPrestMatch) {
      const vPrest = vPrestMatch[0];
      const compMatches = vPrest.matchAll(/<Comp>[\s\S]*?<\/Comp>/gi);

      for (const compMatch of compMatches) {
        const comp = compMatch[0];
        const xNome = comp.match(/<xNome>(.*?)<\/xNome>/i);
        const vComp = comp.match(/<vComp>(.*?)<\/vComp>/i);

        if (xNome && xNome[1] && vComp && vComp[1]) {
          const nome = xNome[1].trim().toLowerCase();
          const valor = parseFloat(vComp[1].trim()) || 0;

          if (nome.includes('frete peso') || nome.includes('fretepeso')) {
            componentes.vlFretePeso += valor;
          } else if (nome.includes('ad valorem') || nome.includes('advalorem')) {
            componentes.vlFreteValor += valor;
          } else if (nome.includes('pedagio') || nome.includes('pedágio')) {
            componentes.vlPedagio += valor;
          } else if (nome.includes('icms')) {
            componentes.vlICMS += valor;
          } else {
            componentes.vlOutros += valor;
          }
        }
      }
    }
  } catch (error: any) {
    logger.warn({ error: error.message }, 'Erro ao extrair componentes do frete');
  }

  return componentes;
};

/**
 * Extrai CFOP do XML (dentro da tag <ide>)
 */
export const extrairCFOP = (xml: string): number => {
  try {
    // Buscar dentro do contexto <ide> para garantir que pega o CFOP correto
    const cfop = extrairValorDoContexto(xml, 'ide', 'CFOP');
    if (cfop > 0) {
      return cfop;
    }
    // Fallback: buscar em qualquer lugar do XML
    const cfopMatch = xml.match(/<CFOP>(.*?)<\/CFOP>/i);
    if (cfopMatch && cfopMatch[1]) {
      const cfopVal = parseInt(cfopMatch[1].trim(), 10);
      if (!Number.isNaN(cfopVal)) {
        return cfopVal;
      }
    }
  } catch (error: any) {
    logger.warn({ error: error.message }, 'Erro ao extrair CFOP');
  }
  return 0;
};

/**
 * Extrai natureza da operação (natOp) do XML (dentro da tag <ide>)
 */
export const extrairNaturezaOperacao = (xml: string): string | null => {
  try {
    // Buscar dentro do contexto <ide> para garantir que pega a natOp correta
    const natOp = extrairTextoDoContexto(xml, 'ide', 'natOp');
    if (natOp) {
      return natOp;
    }
    // Fallback: buscar em qualquer lugar do XML
    const natOpMatch = xml.match(/<natOp>(.*?)<\/natOp>/i);
    if (natOpMatch && natOpMatch[1]) {
      return natOpMatch[1].trim();
    }
  } catch (error: any) {
    logger.warn({ error: error.message }, 'Erro ao extrair natureza da operação');
  }
  return null;
};
