import { Request, Response, NextFunction } from 'express';
import { logger } from '../utils/logger';

/**
 * Corrige JSON mal formatado substituindo valores sem aspas em campos específicos
 * Exemplo: "numero": S/N -> "numero": "S/N"
 */
export function fixMalformedJson(bodyStr: string): string {
  // Campos que podem conter valores como "S/N" sem aspas
  const fieldsToFix = ['numero', 'Numero', 'NUMERO'];
  
  let corrected = bodyStr;
  
  for (const field of fieldsToFix) {
    // Regex para encontrar: "campo": valor_sem_aspas (seguido de , ou } ou espaço)
    // Captura casos como: "numero": S/N,  ou  "numero": S/N}
    // Também captura casos no final de objeto sem vírgula
    const regex = new RegExp(
      `("${field}"\\s*:\\s*)([A-Za-z][A-Za-z0-9/\\-]+?)(\\s*[,\\}\\n])`,
      'g'
    );
    
    corrected = corrected.replace(regex, (match, prefix, value, suffix) => {
      // Se o valor já tem aspas, não modificar
      if (value.startsWith('"') || value.startsWith("'")) {
        return match;
      }
      // Se o valor é número, booleano ou null, não modificar
      if (/^(true|false|null|\d+\.?\d*)$/i.test(value)) {
        return match;
      }
      // Adicionar aspas ao valor (escapar aspas internas se houver)
      const escapedValue = value.replace(/"/g, '\\"');
      logger.debug({ field, value, corrected: escapedValue }, 'Corrigindo valor sem aspas no JSON');
      return `${prefix}"${escapedValue}"${suffix}`;
    });
  }
  
  return corrected;
}
