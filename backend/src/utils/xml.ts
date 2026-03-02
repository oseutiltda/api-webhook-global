import { XMLParser } from 'fast-xml-parser';

const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: '' });

export function parseCteAutorizado(xml: string) {
  const doc = parser.parse(xml);
  const cteProc = doc?.cteProc || doc?.['cteProc'];
  const CTe = cteProc?.CTe;
  const infCte = CTe?.infCte;
  const ide = infCte?.ide;
  const emit = infCte?.emit;
  const dest = infCte?.dest;
  const vPrest = infCte?.vPrest;
  const protCTe = cteProc?.protCTe;
  const infProt = protCTe?.infProt;

  return {
    chCTe: infProt?.chCTe || null,
    nProt: infProt?.nProt || null,
    dhEmi: ide?.dhEmi || null,
    dhRecbto: infProt?.dhRecbto || null,
    serie: ide?.serie || null,
    nCT: ide?.nCT || null,
    emitCnpj: emit?.CNPJ || null,
    destCnpj: dest?.CNPJ || null,
    vTPrest: vPrest?.vTPrest ? Number(vPrest.vTPrest) : null,
    vRec: vPrest?.vRec ? Number(vPrest.vRec) : null,
  } as const;
}

export function parseCteCancelado(xml: string) {
  const doc = parser.parse(xml);
  const procEvento = doc?.procEventoCTe || doc?.['procEventoCTe'];
  const eventoCTe = procEvento?.eventoCTe;
  const infEvento = eventoCTe?.infEvento;
  const detEvento = infEvento?.detEvento;
  const evCancCTe = detEvento?.evCancCTe;
  const retEventoCTe = procEvento?.retEventoCTe;
  const infEventoRet = retEventoCTe?.infEvento;

  return {
    chCTe: infEventoRet?.chCTe || infEvento?.chCTe || null,
    nProt: infEventoRet?.nProt || evCancCTe?.nProt || null,
    dhRegEvento: infEventoRet?.dhRegEvento || infEvento?.dhEvento || null,
    tpEvento: infEventoRet?.tpEvento || infEvento?.tpEvento || null,
    xEvento: infEventoRet?.xEvento || 'Cancelamento',
    emitCnpj: infEvento?.CNPJ || null,
  } as const;
}


