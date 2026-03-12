'use client';

import { useState, useEffect, useRef } from 'react';
import { createPortal } from 'react-dom';
import { Copy, Check } from 'lucide-react';
import { cn } from '@/lib/utils';

interface MetadataCellProps {
  metadata: string | null | undefined;
  className?: string;
}

export function MetadataCell({ metadata, className }: MetadataCellProps) {
  const [copied, setCopied] = useState(false);
  const [showTooltip, setShowTooltip] = useState(false);
  const [tooltipPosition, setTooltipPosition] = useState({ x: 0, y: 0 });
  const cellRef = useRef<HTMLDivElement>(null);
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
  }, []);

  if (!metadata) {
    return <span className={cn('text-xs text-muted-foreground', className)}>-</span>;
  }

  let parsedMetadata: Record<string, unknown> | null = null;
  let metadataString = '';

  try {
    parsedMetadata = typeof metadata === 'string' ? JSON.parse(metadata) : metadata;
    metadataString = typeof metadata === 'string' ? metadata : JSON.stringify(metadata, null, 2);
  } catch {
    metadataString = String(metadata);
  }

  // Criar resumo dos metadados
  const getSummary = () => {
    if (!parsedMetadata) {
      return metadataString.substring(0, 50) + (metadataString.length > 50 ? '...' : '');
    }

    const parts: string[] = [];

    // Informações principais
    if (parsedMetadata.codPessoa) parts.push(`Pessoa: ${String(parsedMetadata.codPessoa)}`);
    if (parsedMetadata.cteId) parts.push(`CT-e: ${String(parsedMetadata.cteId)}`);
    if (parsedMetadata.manifestId) parts.push(`CIOT: ${String(parsedMetadata.manifestId)}`);
    if (parsedMetadata.nrciot) parts.push(`CIOT: ${String(parsedMetadata.nrciot)}`);

    // Tabelas inseridas
    if (parsedMetadata.tabelasInseridas && Array.isArray(parsedMetadata.tabelasInseridas)) {
      parts.push(`✓ ${parsedMetadata.tabelasInseridas.length} tabela(s)`);
    }

    // Tabelas falhadas
    if (parsedMetadata.tabelasFalhadas && Array.isArray(parsedMetadata.tabelasFalhadas)) {
      parts.push(`✗ ${parsedMetadata.tabelasFalhadas.length} falha(s)`);
    }

    // Resumo
    if (
      parsedMetadata.resumo &&
      typeof parsedMetadata.resumo === 'object' &&
      !Array.isArray(parsedMetadata.resumo)
    ) {
      const { sucesso, falhas } = parsedMetadata.resumo as Record<string, unknown>;
      if (sucesso !== undefined || falhas !== undefined) {
        parts.push(`${String(sucesso || 0)} ok, ${String(falhas || 0)} erro(s)`);
      }
    }

    // Etapa
    if (parsedMetadata.etapa) {
      parts.push(String(parsedMetadata.etapa));
    }

    return parts.length > 0
      ? parts.join(' | ')
      : metadataString.substring(0, 50) + (metadataString.length > 50 ? '...' : '');
  };

  const summary = getSummary();
  const fullText = metadataString;

  const handleDoubleClick = async () => {
    try {
      await navigator.clipboard.writeText(fullText);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch (err) {
      console.error('Erro ao copiar:', err);
    }
  };

  const handleMouseEnter = (e: React.MouseEvent<HTMLDivElement>) => {
    if (cellRef.current) {
      const rect = cellRef.current.getBoundingClientRect();
      const viewportWidth = window.innerWidth;
      const viewportHeight = window.innerHeight;

      // Calcular posição centralizada, mas ajustar se estiver muito perto das bordas
      let x = rect.left + rect.width / 2;
      let y = rect.top + rect.height / 2;

      // Ajustar para não sair da tela
      const tooltipWidth = Math.min(500, viewportWidth * 0.9);
      const tooltipHeight = Math.min(viewportHeight * 0.7, 400);

      if (x - tooltipWidth / 2 < 10) x = tooltipWidth / 2 + 10;
      if (x + tooltipWidth / 2 > viewportWidth - 10) x = viewportWidth - tooltipWidth / 2 - 10;
      if (y - tooltipHeight / 2 < 10) y = tooltipHeight / 2 + 10;
      if (y + tooltipHeight / 2 > viewportHeight - 10) y = viewportHeight - tooltipHeight / 2 - 10;

      setTooltipPosition({ x, y });
    }
    setShowTooltip(true);
  };

  const tooltipContent =
    showTooltip && mounted ? (
      <div
        className="fixed z-[9999] w-[90vw] sm:w-[500px] max-w-[90vw] max-h-[70vh] overflow-auto bg-popover border border-border rounded-xl shadow-[0_20px_48px_rgba(30,47,91,0.14)] p-3 text-xs"
        style={{
          left: `${tooltipPosition.x}px`,
          top: `${tooltipPosition.y}px`,
          transform: 'translate(-50%, -50%)',
          maxWidth: 'min(90vw, 500px)',
        }}
        onMouseEnter={() => setShowTooltip(true)}
        onMouseLeave={() => setShowTooltip(false)}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-start justify-between gap-2 mb-2 sticky top-0 bg-popover pb-2 border-b z-10">
          <span className="font-semibold">Metadados completos:</span>
          <button
            onClick={handleDoubleClick}
            className="flex items-center gap-1 text-primary hover:text-primary/80 transition-colors shrink-0"
            title="Copiar"
          >
            {copied ? (
              <>
                <Check className="h-3 w-3" />
                <span className="text-xs">Copiado!</span>
              </>
            ) : (
              <>
                <Copy className="h-3 w-3" />
                <span className="text-xs">Copiar</span>
              </>
            )}
          </button>
        </div>
        <pre className="whitespace-pre-wrap break-words text-xs font-mono bg-muted/50 p-2 rounded-lg overflow-x-auto">
          {fullText}
        </pre>
      </div>
    ) : null;

  return (
    <>
      <div
        ref={cellRef}
        className={cn('relative', className)}
        onMouseEnter={handleMouseEnter}
        onMouseLeave={() => setShowTooltip(false)}
      >
        <div
          className="cursor-pointer text-xs text-muted-foreground truncate max-w-[150px] sm:max-w-xs hover:text-foreground transition-colors"
          onDoubleClick={handleDoubleClick}
          title="Duplo clique para copiar"
        >
          {summary}
        </div>
      </div>
      {mounted && createPortal(tooltipContent, document.body)}
    </>
  );
}
