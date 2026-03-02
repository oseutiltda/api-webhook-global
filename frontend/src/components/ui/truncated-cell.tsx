'use client'

import { useState, useEffect, useRef } from 'react'
import { createPortal } from 'react-dom'
import { cn } from '@/lib/utils'

interface TruncatedCellProps {
  text: string | null | undefined
  className?: string
  maxLength?: number
  title?: string
  textClassName?: string
}

export function TruncatedCell({ 
  text, 
  className, 
  maxLength = 50,
  title = 'Texto completo',
  textClassName
}: TruncatedCellProps) {
  const [showTooltip, setShowTooltip] = useState(false)
  const [tooltipPosition, setTooltipPosition] = useState({ x: 0, y: 0 })
  const cellRef = useRef<HTMLDivElement>(null)
  const [mounted, setMounted] = useState(false)

  useEffect(() => {
    setMounted(true)
  }, [])

  if (!text) {
    return <span className={cn('text-xs text-muted-foreground', className)}>-</span>
  }

  const truncatedText = text.length > maxLength ? text.substring(0, maxLength) + '...' : text
  const needsTooltip = text.length > maxLength

  const handleMouseEnter = (e: React.MouseEvent<HTMLDivElement>) => {
    if (!needsTooltip) return
    
    if (cellRef.current) {
      const rect = cellRef.current.getBoundingClientRect()
      const viewportWidth = window.innerWidth
      const viewportHeight = window.innerHeight
      
      // Calcular posição centralizada, mas ajustar se estiver muito perto das bordas
      let x = rect.left + rect.width / 2
      let y = rect.top + rect.height / 2
      
      // Ajustar para não sair da tela
      const tooltipWidth = Math.min(500, viewportWidth * 0.9)
      const tooltipHeight = Math.min(viewportHeight * 0.7, 400)
      
      if (x - tooltipWidth / 2 < 10) x = tooltipWidth / 2 + 10
      if (x + tooltipWidth / 2 > viewportWidth - 10) x = viewportWidth - tooltipWidth / 2 - 10
      if (y - tooltipHeight / 2 < 10) y = tooltipHeight / 2 + 10
      if (y + tooltipHeight / 2 > viewportHeight - 10) y = viewportHeight - tooltipHeight / 2 - 10
      
      setTooltipPosition({ x, y })
    }
    setShowTooltip(true)
  }

  const tooltipContent = showTooltip && mounted && needsTooltip ? (
    <div 
      className="fixed z-[9999] w-[90vw] sm:w-[500px] max-w-[90vw] max-h-[70vh] overflow-auto bg-popover border border-border rounded-md shadow-2xl p-3 text-xs"
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
        <span className="font-semibold">{title}:</span>
      </div>
      <pre className="whitespace-pre-wrap break-words text-xs font-mono bg-muted/50 p-2 rounded overflow-x-auto">
        {text}
      </pre>
    </div>
  ) : null

  return (
    <>
      <div
        ref={cellRef}
        className={cn('relative', className)}
        onMouseEnter={handleMouseEnter}
        onMouseLeave={() => setShowTooltip(false)}
      >
        <div
          className={cn(
            'text-xs truncate max-w-[150px] sm:max-w-xs hover:text-foreground transition-colors',
            textClassName || 'text-muted-foreground',
            needsTooltip && 'cursor-help'
          )}
        >
          {truncatedText}
        </div>
      </div>
      {mounted && createPortal(tooltipContent, document.body)}
    </>
  )
}

