'use client'

import { useEffect, useState } from 'react'
import {
  AlertTriangle,
  ArrowUpRight,
  Bell,
  Download,
  RefreshCw,
  ShieldCheck,
  Settings,
  TrendingUp,
  CheckCircle2,
  XCircle,
  Clock,
  Activity,
  BarChart3,
  PieChart as PieChartIcon,
  LineChart as LineChartIcon,
  Home as HomeIcon,
} from "lucide-react"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Separator } from "@/components/ui/separator"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { ChartContainer, ChartTooltip, ChartTooltipContent, ChartLegend, ChartLegendContent } from "@/components/ui/chart"
import { cn } from "@/lib/utils"
import Link from "next/link"
import { useRouter } from "next/navigation"
import { MetadataCell } from "@/components/ui/metadata-cell"
import { TruncatedCell } from "@/components/ui/truncated-cell"
import {
  LineChart,
  Line,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
  AreaChart,
  Area,
} from 'recharts'

// Função helper para obter a URL base da API
// Deve ser chamada dentro de funções que executam no cliente
// Quando o Nginx está fazendo proxy reverso, usar URLs relativas para evitar Mixed Content
const getApiBase = (): string => {
  // Se NEXT_PUBLIC_API_BASE_URL estiver definido e for uma URL absoluta, usar ela
  if (process.env.NEXT_PUBLIC_API_BASE_URL) {
    const apiUrl = process.env.NEXT_PUBLIC_API_BASE_URL
    // Se for uma URL absoluta (começa com http:// ou https://), usar ela
    if (apiUrl.startsWith('http://') || apiUrl.startsWith('https://')) {
      // Se estiver no browser e a página for HTTPS, forçar HTTPS na URL da API
      if (typeof window !== 'undefined' && window.location.protocol === 'https:') {
        // Se a URL da API for HTTP mas a página for HTTPS, converter para HTTPS
        if (apiUrl.startsWith('http://')) {
          return apiUrl.replace('http://', 'https://')
        }
      }
      return apiUrl
    }
    // Se for uma URL relativa, retornar como está (será usada com window.location.origin)
    return apiUrl
  }
  // Se estiver no browser, usar URL relativa (mesmo domínio/porta via Nginx)
  if (typeof window !== 'undefined') {
    // Usar URL relativa para evitar Mixed Content quando o Nginx faz proxy reverso
    return '' // URL relativa - será concatenada com o path da API
  }
  // Fallback para desenvolvimento local (apenas para SSR)
  return 'http://localhost:3000'
}

type Status =
  | "accepted"
  | "queued"
  | "pending"
  | "processing"
  | "processed"
  | "duplicate"
  | "error"
  | "failed"
  | "retrying"

const statusStyles: Record<Status, string> = {
  accepted: "border-emerald-200 bg-emerald-500/10 text-emerald-600",
  queued: "border-blue-200 bg-blue-500/10 text-blue-600",
  pending: "border-blue-200 bg-blue-500/10 text-blue-600",
  processing: "border-sky-200 bg-sky-500/10 text-sky-600",
  processed: "border-emerald-200 bg-emerald-500/10 text-emerald-600",
  duplicate: "border-purple-200 bg-purple-500/10 text-purple-600",
  error: "border-rose-200 bg-rose-500/10 text-rose-600",
  failed: "border-rose-200 bg-rose-500/10 text-rose-600",
  retrying: "border-amber-200 bg-amber-500/10 text-amber-600",
}

interface WorkerStats {
  total: number
  totalLast24h?: number // Total de eventos nas últimas 24h
  pending: number
  processing: number
  processed: number
  failed: number
  failedLast24h?: number // Falhas nas últimas 24h
  processedLast24h: number
  successRate: number
  eventsByType: Array<{ source: string; count: number }>
  integrationStats?: {
    integrated: number
    pending: number
    failed: number
    skipped: number
  }
  uniqueRecords?: {
    ciot: { total: number; unique: number }
    nfse: { total: number; unique: number }
    cte: { total: number; unique: number }
    pessoa: { total: number; unique: number }
  }
}

interface WorkerEvent {
  id: string
  source: string
  receivedAt: string
  status: string | null
  processedAt: string | null
  errorMessage: string | null
  retryCount: number
  integrationStatus?: string | null
  processingTimeMs?: number | null
  integrationTimeMs?: number | null
  seniorId?: string | null
  metadata?: string | null
}

interface PerformanceMetrics {
  avgProcessingTimeMs: number
  avgProcessingTimeSeconds: number
  totalProcessed: number
  hourlyStats: Array<{ hour: number; count: number }>
}

interface HealthStatus {
  timestamp: string
  services: {
    backend: {
      status: 'online' | 'offline'
      lastCheck: string
      uptime: number
    }
    database: {
      status: 'online' | 'offline'
      lastCheck: string
      responseTimeMs: number
      error: string | null
    }
    worker: {
      status: 'online' | 'offline' | 'unknown'
      lastCheck: string
      lastActivity: string | null
      responseTimeMs: number
      error: string | null
    }
  }
}

const CORES_GRAFICOS = [
  '#10B981', // Verde
  '#3B82F6', // Azul
  '#F59E0B', // Amarelo
  '#EF4444', // Vermelho
  '#8B5CF6', // Roxo
  '#06B6D4', // Ciano
  '#EC4899', // Rosa
  '#FF6B35', // Laranja
]

function StatusBadge({ status }: { status: Status | string | null }) {
  const normalizedStatus = (status || 'pending') as Status
  const config = statusStyles[normalizedStatus] || statusStyles.pending
  const label = normalizedStatus === 'processed' ? 'Processado' :
                normalizedStatus === 'pending' ? 'Pendente' :
                normalizedStatus === 'processing' ? 'Processando' :
                normalizedStatus === 'failed' ? 'Falhou' :
                normalizedStatus.replace("_", " ")
  
  return (
    <Badge variant="outline" className={cn("capitalize", config)}>
      {label}
    </Badge>
  )
}

function IntegrationStatusBadge({ status }: { status: string | null | undefined }) {
  const statusMap: Record<string, { label: string; className: string }> = {
    integrated: { label: 'Integrado', className: 'border-emerald-200 bg-emerald-500/10 text-emerald-600' },
    pending: { label: 'Pendente', className: 'border-amber-200 bg-amber-500/10 text-amber-600' },
    failed: { label: 'Falhou', className: 'border-rose-200 bg-rose-500/10 text-rose-600' },
    skipped: { label: 'Ignorado', className: 'border-gray-200 bg-gray-500/10 text-gray-600' },
  }

  const config = statusMap[status || 'pending'] || statusMap.pending

  return (
    <Badge variant="outline" className={cn("capitalize", config.className)}>
      {config.label}
    </Badge>
  )
}

function getSourceLabel(source: string): string {
  const labels: Record<string, string> = {
    '/webhooks/cte/autorizado': 'CT-e Autorizado',
    '/webhooks/cte/cancelado': 'CT-e Cancelado',
    '/webhooks/ctrb/ciot/base': 'CIOT Base',
    '/webhooks/ctrb/ciot/parcelas': 'CIOT Parcelas',
    '/webhooks/faturas/pagar/criar': 'Fatura Pagar - Criar',
    '/webhooks/faturas/pagar/baixar': 'Fatura Pagar - Baixar',
    '/webhooks/faturas/pagar/cancelar': 'Fatura Pagar - Cancelar',
    '/webhooks/faturas/receber/criar': 'Fatura Receber - Criar',
    '/webhooks/faturas/receber/baixar': 'Fatura Receber - Baixar',
    '/webhooks/nfse/autorizado': 'NFSe Autorizado',
    '/webhooks/pessoa/upsert': 'Pessoa Upsert',
    '/api/CIOT/InserirContasPagarCIOT': 'CIOT - Inserir Contas a Pagar',
    '/api/CIOT/CancelarContasPagarCIOT': 'CIOT - Cancelar Contas a Pagar',
    '/api/NFSe/InserirNFSe': 'NFSe - Inserir',
    '/api/Pessoa/InserirPessoa': 'Pessoa - Inserir',
  }
  
  return labels[source] || source
}

function formatDate(dateString: string | null): string {
  if (!dateString) return '-'
  try {
    const date = new Date(dateString)
    return date.toLocaleString('pt-BR', {
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    })
  } catch {
    return dateString
  }
}

function formatTimeAgo(dateString: string | null): string {
  if (!dateString) return '-'
  try {
    const date = new Date(dateString)
    const now = new Date()
    const diffMs = now.getTime() - date.getTime()
    const diffMins = Math.floor(diffMs / 60000)
    const diffHours = Math.floor(diffMs / 3600000)
    const diffDays = Math.floor(diffMs / 86400000)
    
    if (diffMins < 1) return 'agora'
    if (diffMins < 60) return `há ${diffMins} min`
    if (diffHours < 24) return `há ${diffHours}h`
    return `há ${diffDays} dia${diffDays > 1 ? 's' : ''}`
  } catch {
    return '-'
  }
}

function formatUptime(seconds: number): string {
  const days = Math.floor(seconds / 86400)
  const hours = Math.floor((seconds % 86400) / 3600)
  const minutes = Math.floor((seconds % 3600) / 60)
  
  if (days > 0) return `${days}d ${hours}h`
  if (hours > 0) return `${hours}h ${minutes}m`
  return `${minutes}m`
}

export default function Home() {
  const router = useRouter()
  const [stats, setStats] = useState<WorkerStats | null>(null)
  const [events, setEvents] = useState<WorkerEvent[]>([])
  const [failures, setFailures] = useState<WorkerEvent[]>([])
  const [performance, setPerformance] = useState<PerformanceMetrics | null>(null)
  const [health, setHealth] = useState<HealthStatus | null>(null)
  const [loading, setLoading] = useState(true)
  const [lastSync, setLastSync] = useState<Date>(new Date())

  // Proteção de rota: exige login (auth_token no localStorage)
  useEffect(() => {
    if (typeof window === 'undefined') return
    const token = localStorage.getItem('auth_token')
    if (!token) {
      // Usar router.push() ao invés de window.location.href para evitar erro de header inválido
      router.push('/login')
    }
  }, [router])

  const fetchStats = async () => {
    try {
      const apiBase = getApiBase()
      const url = apiBase ? `${apiBase}/api/worker/stats` : '/api/worker/stats'
      const res = await fetch(url)
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      const data = await res.json()
      setStats(data || {
        total: 0,
        pending: 0,
        processing: 0,
        processed: 0,
        failed: 0,
        processedLast24h: 0,
        successRate: 0,
        eventsByType: [],
        integrationStats: { integrated: 0, pending: 0, failed: 0, skipped: 0 },
        uniqueRecords: { ciot: { total: 0, unique: 0 }, nfse: { total: 0, unique: 0 }, cte: { total: 0, unique: 0 }, pessoa: { total: 0, unique: 0 } },
      })
    } catch (error) {
      console.error('Erro ao buscar estatísticas:', error)
      setStats({
        total: 0,
        pending: 0,
        processing: 0,
        processed: 0,
        failed: 0,
        processedLast24h: 0,
        successRate: 0,
        eventsByType: [],
        integrationStats: { integrated: 0, pending: 0, failed: 0, skipped: 0 },
        uniqueRecords: { ciot: { total: 0, unique: 0 }, nfse: { total: 0, unique: 0 }, cte: { total: 0, unique: 0 }, pessoa: { total: 0, unique: 0 } },
      })
    }
  }

  const fetchEvents = async () => {
    try {
      const apiBase = getApiBase()
      const url = apiBase ? `${apiBase}/api/worker/events?limit=20&page=1` : '/api/worker/events?limit=20&page=1'
      const res = await fetch(url)
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      const data = await res.json()
      setEvents(data.events || [])
    } catch (error) {
      console.error('Erro ao buscar eventos:', error)
      setEvents([])
    }
  }

  const fetchFailures = async () => {
    try {
      const apiBase = getApiBase()
      const url = apiBase ? `${apiBase}/api/worker/failures` : '/api/worker/failures'
      const res = await fetch(url)
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      const data = await res.json()
      setFailures(data || [])
    } catch (error) {
      console.error('Erro ao buscar falhas:', error)
      setFailures([])
    }
  }

  const fetchPerformance = async () => {
    try {
      const apiBase = getApiBase()
      const url = apiBase ? `${apiBase}/api/worker/performance` : '/api/worker/performance'
      const res = await fetch(url)
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      const data = await res.json()
      setPerformance(data || {
        avgProcessingTimeMs: 0,
        avgProcessingTimeSeconds: 0,
        totalProcessed: 0,
        hourlyStats: [],
      })
    } catch (error) {
      console.error('Erro ao buscar performance:', error)
      setPerformance({
        avgProcessingTimeMs: 0,
        avgProcessingTimeSeconds: 0,
        totalProcessed: 0,
        hourlyStats: [],
      })
    }
  }

  const fetchHealth = async () => {
    try {
      const apiBase = getApiBase()
      const url = apiBase ? `${apiBase}/api/health` : '/api/health'
      const res = await fetch(url)
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      const data = await res.json()
      
      // Debug: log da resposta recebida
      console.log('Health check response:', { 
        hasData: !!data, 
        hasServices: !!(data && data.services),
        dataKeys: data ? Object.keys(data) : [],
        dataSample: data ? JSON.stringify(data).substring(0, 200) : 'null'
      })
      
      // Garantir que a estrutura está completa
      if (data && data.services && data.services.backend && data.services.database && data.services.worker) {
        setHealth(data)
      } else {
        // Se a estrutura estiver incompleta, usar fallback
        console.warn('Estrutura de health incompleta:', data)
        setHealth({
          timestamp: new Date().toISOString(),
          services: {
            backend: {
              status: 'offline',
              lastCheck: new Date().toISOString(),
              uptime: 0,
            },
            database: {
              status: 'offline',
              lastCheck: new Date().toISOString(),
              responseTimeMs: 0,
              error: data?.status === 'ok' ? 'API retornou apenas status básico' : 'Estrutura de dados incompleta',
            },
            worker: {
              status: 'offline',
              lastCheck: new Date().toISOString(),
              lastActivity: null,
              responseTimeMs: 0,
              error: data?.status === 'ok' ? 'API retornou apenas status básico' : 'Estrutura de dados incompleta',
            },
          },
        })
      }
    } catch (error) {
      console.error('Erro ao buscar health status:', error)
      // Se não conseguir buscar, assumir que backend está offline
      setHealth({
        timestamp: new Date().toISOString(),
        services: {
          backend: {
            status: 'offline',
            lastCheck: new Date().toISOString(),
            uptime: 0,
          },
          database: {
            status: 'offline',
            lastCheck: new Date().toISOString(),
            responseTimeMs: 0,
            error: 'Não foi possível verificar o status',
          },
          worker: {
            status: 'offline',
            lastCheck: new Date().toISOString(),
            lastActivity: null,
            responseTimeMs: 0,
            error: 'Não foi possível verificar o status',
          },
        },
      })
    }
  }

  const loadAll = async () => {
    setLoading(true)
    await Promise.all([
      fetchStats(),
      fetchEvents(),
      fetchFailures(),
      fetchPerformance(),
      fetchHealth(),
    ])
    setLastSync(new Date())
    setLoading(false)
  }

  useEffect(() => {
    loadAll()
    const interval = setInterval(loadAll, 60000) // Atualizar a cada 1 minuto (60000ms)
    return () => clearInterval(interval)
  }, [])

  const summaryCards = stats ? [
    {
      title: "Eventos recebidos (24h)",
      value: (stats.totalLast24h ?? stats.processedLast24h).toLocaleString('pt-BR'),
      change: `Taxa de sucesso: ${stats.successRate.toFixed(1)}%`,
      trend: stats.successRate >= 95 ? "positive" : stats.successRate >= 80 ? "neutral" : "negative",
  },
  {
      title: "Processamento médio",
      value: performance ? `${performance.avgProcessingTimeSeconds.toFixed(2)}s` : "0s",
      change: performance ? `(${performance.avgProcessingTimeMs.toLocaleString('pt-BR')}ms)` : "",
      trend: "positive",
    },
    {
      title: "Eventos pendentes",
      value: stats.pending.toLocaleString('pt-BR'),
      change: `${stats.processing} em processamento`,
      trend: stats.pending > 10 ? "negative" : "neutral",
    },
    {
      title: "Falhas (24h)",
      value: (stats.failedLast24h ?? stats.failed).toLocaleString('pt-BR'),
      change: failures.length > 0 ? `${failures.length} requerem atenção` : "Nenhuma falha crítica",
      trend: (stats.failedLast24h ?? stats.failed) > 0 ? "negative" : "positive",
  },
  ] : []

  // Dados para gráficos - usar diretamente o source do backend (já agrupado por tipo)
  const eventosPorTipo = stats?.eventsByType.map((item, idx) => ({
    nome: item.source, // Já vem agrupado do backend (NFSe, CIOT, CT-e, etc.)
    valor: item.count,
    origem: item.source,
    cor: CORES_GRAFICOS[idx % CORES_GRAFICOS.length],
  })) || []

  const dadosPorHora = performance?.hourlyStats
    .sort((a, b) => a.hour - b.hour)
    .map(h => ({
      hora: `${String(h.hour).padStart(2, '0')}:00`,
      quantidade: h.count,
    })) || []

  const dadosStatusIntegracao = stats?.integrationStats ? [
    { nome: 'Integrados', valor: stats.integrationStats.integrated, cor: '#10B981' },
    { nome: 'Pendentes', valor: stats.integrationStats.pending, cor: '#F59E0B' },
    { nome: 'Falhas', valor: stats.integrationStats.failed, cor: '#EF4444' },
    { nome: 'Ignorados', valor: stats.integrationStats.skipped, cor: '#6B7280' },
  ].filter(item => item.valor > 0) : []

  const serviceHealth = stats?.eventsByType
    .slice(0, 4)
    .map((item) => ({
      name: getSourceLabel(item.source),
      route: item.source,
      sla: performance ? `${performance.avgProcessingTimeSeconds.toFixed(2)}s` : "N/A",
      status: item.count > 0 ? "accepted" : "error",
      integrationStatus: stats.integrationStats ? 
        (stats.integrationStats.integrated > 0 ? "integrated" : "pending") : 
        "pending",
    })) || []

  const recentErrors = failures.slice(0, 5).map((failure) => ({
    id: failure.id,
    event: getSourceLabel(failure.source),
    detail: failure.errorMessage || 'Erro desconhecido',
    resolution: failure.retryCount >= 3 ? 'Requer intervenção manual' : 'Aguardar retry automático',
    lastAttempt: formatTimeAgo(failure.receivedAt),
  }))

  // Usar a taxa de sucesso calculada pelo backend (já considera apenas últimas 24h)
  const successRate = stats ? stats.successRate.toFixed(1) : "0.0"

  return (
    <div className="min-h-screen bg-muted/40 pb-12">
      <div className="mx-auto flex max-w-7xl flex-col gap-6 px-4 pb-8 pt-10 lg:px-0">
        <header className="flex flex-col gap-4 rounded-3xl border bg-card/70 p-6 shadow-sm backdrop-blur lg:flex-row lg:items-center lg:justify-between">
          <div>
            <div className="flex items-center gap-2 mb-2">
              <p className="text-sm font-medium text-muted-foreground">
                Última sincronização: {lastSync.toLocaleTimeString('pt-BR')}
              </p>
              <Badge variant="outline" className="text-xs">
                <Activity className="h-3 w-3 mr-1" />
                Auto-refresh: 1 min
              </Badge>
            </div>
            <h1 className="text-3xl font-semibold tracking-tight text-foreground">
              Dashboard de Integrações ESL/Senior
            </h1>
            <p className="text-sm text-muted-foreground">
              Monitoramento em tempo real de webhooks, integrações e alertas de reprocessamento.
            </p>
          </div>
          <div className="flex flex-wrap gap-3">
            <Button
              onClick={() => router.push('/dashboard')}
              variant="outline"
              size="sm"
            >
              <HomeIcon className="w-4 h-4 mr-2" />
              Voltar
            </Button>
            <Button 
              variant="outline" 
              className="gap-2"
              onClick={() => loadAll()}
              disabled={loading}
            >
              <RefreshCw className={cn("h-4 w-4", loading && "animate-spin")} />
              Atualizar
            </Button>
            <Link href="/worker">
              <Button variant="outline" className="gap-2">
                <Settings className="h-4 w-4" />
                Dashboard Worker
              </Button>
            </Link>
            <Link href="/worker1">
            <Button variant="outline" className="gap-2">
                <BarChart3 className="h-4 w-4" />
                Análise Detalhada
            </Button>
            </Link>
          </div>
        </header>

        {/* Status dos Serviços */}
        {health && health.services && (
          <section className="grid gap-4 md:grid-cols-3">
            {/* Backend Status */}
            <Card className={cn(
              "border-muted",
              health.services?.backend?.status === 'online' 
                ? "border-emerald-200 bg-emerald-50/30" 
                : "border-rose-200 bg-rose-50/30"
            )}>
              <CardHeader className="pb-3">
                <div className="flex items-center justify-between">
                  <CardTitle className="text-lg flex items-center gap-2">
                    <Activity className={cn(
                      "h-5 w-5",
                      health.services?.backend?.status === 'online' ? "text-emerald-600" : "text-rose-600"
                    )} />
                    Backend API
                  </CardTitle>
                  <Badge 
                    variant="outline" 
                    className={cn(
                      health.services?.backend?.status === 'online' 
                        ? "border-emerald-200 bg-emerald-500/10 text-emerald-600" 
                        : "border-rose-200 bg-rose-500/10 text-rose-600"
                    )}
                  >
                    {health.services?.backend?.status === 'online' ? 'Online' : 'Offline'}
                  </Badge>
                </div>
                <CardDescription className="mt-2">
                  Uptime: {formatUptime(health.services?.backend?.uptime || 0)}
                </CardDescription>
                <CardDescription className="text-xs">
                  Verificado: {formatTimeAgo(health.services?.backend?.lastCheck || null)}
                </CardDescription>
              </CardHeader>
            </Card>

            {/* Database Status */}
            <Card className={cn(
              "border-muted",
              health.services?.database?.status === 'online' 
                ? "border-emerald-200 bg-emerald-50/30" 
                : "border-rose-200 bg-rose-50/30"
            )}>
              <CardHeader className="pb-3">
                <div className="flex items-center justify-between">
                  <CardTitle className="text-lg flex items-center gap-2">
                    <ShieldCheck className={cn(
                      "h-5 w-5",
                      health.services?.database?.status === 'online' ? "text-emerald-600" : "text-rose-600"
                    )} />
                    Banco de Dados
                  </CardTitle>
                  <Badge 
                    variant="outline" 
                    className={cn(
                      health.services?.database?.status === 'online' 
                        ? "border-emerald-200 bg-emerald-500/10 text-emerald-600" 
                        : "border-rose-200 bg-rose-500/10 text-rose-600"
                    )}
                  >
                    {health.services?.database?.status === 'online' ? 'Online' : 'Offline'}
                  </Badge>
                </div>
                <CardDescription className="mt-2">
                  {health.services?.database?.status === 'online' 
                    ? `Tempo de resposta: ${health.services?.database?.responseTimeMs || 0}ms`
                    : health.services?.database?.error || 'Erro desconhecido'
                  }
                </CardDescription>
                <CardDescription className="text-xs">
                  Verificado: {formatTimeAgo(health.services?.database?.lastCheck || null)}
                </CardDescription>
              </CardHeader>
            </Card>

            {/* Worker Status */}
            <Card className={cn(
              "border-muted",
              health.services?.worker?.status === 'online' 
                ? "border-emerald-200 bg-emerald-50/30" 
                : health.services?.worker?.status === 'unknown'
                ? "border-amber-200 bg-amber-50/30"
                : "border-rose-200 bg-rose-50/30"
            )}>
              <CardHeader className="pb-3">
                <div className="flex items-center justify-between">
                  <CardTitle className="text-lg flex items-center gap-2">
                    <Settings className={cn(
                      "h-5 w-5",
                      health.services?.worker?.status === 'online' ? "text-emerald-600" 
                        : health.services?.worker?.status === 'unknown' ? "text-amber-600"
                        : "text-rose-600"
                    )} />
                    Worker
                  </CardTitle>
                  <Badge 
                    variant="outline" 
                    className={cn(
                      health.services?.worker?.status === 'online' 
                        ? "border-emerald-200 bg-emerald-500/10 text-emerald-600" 
                        : health.services?.worker?.status === 'unknown'
                        ? "border-amber-200 bg-amber-500/10 text-amber-600"
                        : "border-rose-200 bg-rose-500/10 text-rose-600"
                    )}
                  >
                    {health.services?.worker?.status === 'online' ? 'Online' 
                      : health.services?.worker?.status === 'unknown' ? 'Desconhecido'
                      : 'Offline'}
                  </Badge>
                </div>
                <CardDescription className="mt-2">
                  {health.services?.worker?.status === 'online' && health.services?.worker?.lastActivity
                    ? `Última atividade: ${formatTimeAgo(health.services.worker.lastActivity)}`
                    : health.services?.worker?.status === 'unknown'
                    ? 'Sem atividade recente (pode estar ocioso)'
                    : health.services?.worker?.error || 'Erro desconhecido'
                  }
                </CardDescription>
                <CardDescription className="text-xs">
                  Verificado: {formatTimeAgo(health.services?.worker?.lastCheck || null)}
                </CardDescription>
              </CardHeader>
            </Card>
          </section>
        )}

        {/* Cards Principais */}
        <section className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
          {summaryCards.map((card) => (
            <Card key={card.title} className="border-muted">
              <CardHeader className="pb-2">
                <CardDescription>{card.title}</CardDescription>
                <CardTitle className="text-3xl font-semibold">
                  {loading ? "..." : card.value}
                </CardTitle>
              </CardHeader>
              <CardFooter>
                <p
                  className={cn(
                    "text-sm font-medium",
                    card.trend === "positive" && "text-emerald-600",
                    card.trend === "negative" && "text-rose-600",
                    card.trend === "neutral" && "text-muted-foreground",
                  )}
                >
                  {card.change}
                </p>
              </CardFooter>
            </Card>
          ))}
        </section>

        {/* Cards de Registros Únicos */}
        {stats?.uniqueRecords && (
          <section>
            <h2 className="text-lg font-semibold mb-4">Registros Únicos Processados (24h)</h2>
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
              <Card className="border-muted">
                <CardHeader className="pb-2">
                  <CardDescription className="flex items-center gap-2">
                    <ShieldCheck className="h-4 w-4 text-blue-600" />
                    CIOT Únicos
                  </CardDescription>
                  <CardTitle className="text-3xl font-semibold text-blue-600">
                    {loading ? "..." : stats.uniqueRecords.ciot.unique.toLocaleString('pt-BR')}
                  </CardTitle>
                </CardHeader>
                <CardFooter>
                  <p className="text-sm text-muted-foreground">
                    {stats.uniqueRecords.ciot.total.toLocaleString('pt-BR')} eventos processados
                    {stats.uniqueRecords.ciot.total > 0 && (
                      <span className="ml-2 text-xs">
                        ({((stats.uniqueRecords.ciot.unique / stats.uniqueRecords.ciot.total) * 100).toFixed(1)}% únicos)
                      </span>
                    )}
                  </p>
                </CardFooter>
              </Card>
              <Card className="border-muted">
                <CardHeader className="pb-2">
                  <CardDescription className="flex items-center gap-2">
                    <ShieldCheck className="h-4 w-4 text-green-600" />
                    NFSE Únicos
                  </CardDescription>
                  <CardTitle className="text-3xl font-semibold text-green-600">
                    {loading ? "..." : stats.uniqueRecords.nfse.unique.toLocaleString('pt-BR')}
                  </CardTitle>
                </CardHeader>
                <CardFooter>
                  <p className="text-sm text-muted-foreground">
                    {stats.uniqueRecords.nfse.total.toLocaleString('pt-BR')} eventos processados
                    {stats.uniqueRecords.nfse.total > 0 && (
                      <span className="ml-2 text-xs">
                        ({((stats.uniqueRecords.nfse.unique / stats.uniqueRecords.nfse.total) * 100).toFixed(1)}% únicos)
                      </span>
                    )}
                  </p>
                </CardFooter>
              </Card>
              <Card className="border-muted">
                <CardHeader className="pb-2">
                  <CardDescription className="flex items-center gap-2">
                    <ShieldCheck className="h-4 w-4 text-purple-600" />
                    CT-e Únicos
                  </CardDescription>
                  <CardTitle className="text-3xl font-semibold text-purple-600">
                    {loading ? "..." : stats.uniqueRecords.cte.unique.toLocaleString('pt-BR')}
                  </CardTitle>
                </CardHeader>
                <CardFooter>
                  <p className="text-sm text-muted-foreground">
                    {stats.uniqueRecords.cte.total.toLocaleString('pt-BR')} eventos processados
                    {stats.uniqueRecords.cte.total > 0 && (
                      <span className="ml-2 text-xs">
                        ({((stats.uniqueRecords.cte.unique / stats.uniqueRecords.cte.total) * 100).toFixed(1)}% únicos)
                      </span>
                    )}
                  </p>
                </CardFooter>
              </Card>
              <Card className="border-muted">
                <CardHeader className="pb-2">
                  <CardDescription className="flex items-center gap-2">
                    <ShieldCheck className="h-4 w-4 text-orange-600" />
                    Pessoa Únicos
                  </CardDescription>
                  <CardTitle className="text-3xl font-semibold text-orange-600">
                    {loading ? "..." : stats.uniqueRecords.pessoa.unique.toLocaleString('pt-BR')}
                  </CardTitle>
                </CardHeader>
                <CardFooter>
                  <p className="text-sm text-muted-foreground">
                    {stats.uniqueRecords.pessoa.total.toLocaleString('pt-BR')} eventos processados
                    {stats.uniqueRecords.pessoa.total > 0 && (
                      <span className="ml-2 text-xs">
                        ({((stats.uniqueRecords.pessoa.unique / stats.uniqueRecords.pessoa.total) * 100).toFixed(1)}% únicos)
                      </span>
                    )}
                  </p>
                </CardFooter>
              </Card>
            </div>
          </section>
        )}

        {/* Gráficos Principais */}
        <section className="grid gap-6 lg:grid-cols-2">
          {/* Gráfico de Linha - Eventos por Hora */}
          <Card className="border-muted">
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <LineChartIcon className="h-5 w-5" />
                Eventos por Hora (Últimas 24h)
              </CardTitle>
              <CardDescription>Distribuição temporal de processamento</CardDescription>
            </CardHeader>
            <CardContent>
              <ChartContainer
                config={{
                  quantidade: { label: 'Quantidade', color: '#3B82F6' },
                }}
                className="h-[300px]"
              >
                <AreaChart data={dadosPorHora}>
                  <defs>
                    <linearGradient id="colorQuantidade" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#3B82F6" stopOpacity={0.8}/>
                      <stop offset="95%" stopColor="#3B82F6" stopOpacity={0}/>
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                  <XAxis 
                    dataKey="hora" 
                    className="text-xs"
                    tick={{ fill: 'currentColor' }}
                  />
                  <YAxis 
                    className="text-xs"
                    tick={{ fill: 'currentColor' }}
                  />
                  <ChartTooltip content={<ChartTooltipContent />} />
                  <Area
                    type="monotone"
                    dataKey="quantidade"
                    stroke="#3B82F6"
                    fillOpacity={1}
                    fill="url(#colorQuantidade)"
                  />
                </AreaChart>
              </ChartContainer>
            </CardContent>
          </Card>

          {/* Gráfico de Pizza - Status de Integração */}
          <Card className="border-muted">
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <PieChartIcon className="h-5 w-5" />
                Status de Integração com Senior
              </CardTitle>
              <CardDescription>Distribuição de eventos integrados (últimas 24h)</CardDescription>
            </CardHeader>
            <CardContent>
              {dadosStatusIntegracao.length > 0 ? (
                <ChartContainer
                  config={dadosStatusIntegracao.reduce((acc, item) => {
                    acc[item.nome.toLowerCase()] = {
                      label: item.nome,
                      color: item.cor,
                    }
                    return acc
                  }, {} as Record<string, { label: string; color: string }>)}
                  className="h-[300px]"
                >
                  <PieChart>
                    <Pie
                      data={dadosStatusIntegracao}
                      cx="50%"
                      cy="50%"
                      labelLine={false}
                      label={({ nome, percent }) => `${nome}: ${(percent * 100).toFixed(0)}%`}
                      outerRadius={100}
                      fill="#8884d8"
                      dataKey="valor"
                    >
                      {dadosStatusIntegracao.map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={entry.cor} />
                      ))}
                    </Pie>
                    <ChartTooltip content={<ChartTooltipContent />} />
                    <ChartLegend content={<ChartLegendContent />} />
                  </PieChart>
                </ChartContainer>
              ) : (
                <div className="flex items-center justify-center h-[300px] text-muted-foreground">
                  <p>Nenhum dado de integração disponível</p>
                </div>
              )}
            </CardContent>
          </Card>
        </section>

        {/* Gráfico de Barras - Eventos por Tipo */}
        {eventosPorTipo.length > 0 && (
          <Card className="border-muted">
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <BarChart3 className="h-5 w-5" />
                Eventos por Tipo (Últimas 24h)
              </CardTitle>
              <CardDescription>Distribuição de eventos por tipo de serviço</CardDescription>
            </CardHeader>
            <CardContent>
              <ChartContainer
                config={eventosPorTipo.reduce((acc, item, idx) => {
                  acc[item.nome.toLowerCase().replace(/\s+/g, '_')] = {
                    label: item.nome,
                    color: item.cor,
                  }
                  return acc
                }, {} as Record<string, { label: string; color: string }>)}
                className="h-[300px]"
              >
                <BarChart data={eventosPorTipo.slice(0, 8)} layout="vertical">
                  <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                  <XAxis type="number" className="text-xs" />
                  <YAxis 
                    dataKey="nome" 
                    type="category" 
                    width={150}
                    className="text-xs"
                    tick={{ fill: 'currentColor' }}
                  />
                  <ChartTooltip content={<ChartTooltipContent />} />
                  <Bar dataKey="valor" radius={[0, 4, 4, 0]}>
                    {eventosPorTipo.slice(0, 8).map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.cor} />
                    ))}
                  </Bar>
                </BarChart>
              </ChartContainer>
            </CardContent>
          </Card>
        )}

        {/* Tabelas e Detalhes */}
        <section className="grid gap-4 lg:grid-cols-[2fr,1fr]">
          <Card className="border-muted">
            <CardHeader className="pb-3">
              <div className="flex flex-col gap-2 lg:flex-row lg:items-center lg:justify-between">
                <div>
                  <CardTitle>Eventos recentes</CardTitle>
                  <CardDescription>
                    Últimas notificações recebidas do ESL
                  </CardDescription>
                </div>
                <div className="flex flex-col sm:flex-row items-stretch sm:items-center gap-2">
                  <Input placeholder="Buscar por ID ou rota" className="w-full sm:w-56" />
                  <Link href="/worker" className="w-full sm:w-auto">
                  <Button variant="secondary" size="sm" className="gap-1 w-full sm:w-auto">
                    Ver todos
                    <ArrowUpRight className="h-4 w-4" />
                  </Button>
                  </Link>
                </div>
              </div>
            </CardHeader>
            <Separator />
            <CardContent className="p-0">
              <div className="overflow-x-auto">
                <ScrollArea className="h-[320px]">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead className="min-w-[120px]">ID do evento</TableHead>
                        <TableHead className="min-w-[120px]">Tipo</TableHead>
                        <TableHead className="min-w-[100px]">Status</TableHead>
                        <TableHead className="min-w-[120px] hidden md:table-cell">Integração</TableHead>
                        <TableHead className="min-w-[100px] hidden lg:table-cell">Tempo</TableHead>
                        <TableHead className="text-right min-w-[140px]">Recebido</TableHead>
                        <TableHead className="min-w-[180px]">Metadados</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {loading && events.length === 0 ? (
                        <TableRow>
                          <TableCell colSpan={7} className="text-center text-muted-foreground">
                            Carregando eventos...
                          </TableCell>
                        </TableRow>
                      ) : events.length === 0 ? (
                        <TableRow>
                          <TableCell colSpan={7} className="text-center text-muted-foreground">
                            Nenhum evento encontrado
                          </TableCell>
                        </TableRow>
                      ) : (
                        events.map((evt) => {
                          const isFailed = evt.status === 'failed';
                          const metadata = evt.metadata ? (() => {
                            try {
                              return JSON.parse(evt.metadata);
                            } catch {
                              return null;
                            }
                          })() : null;
                          
                          return (
                            <TableRow key={evt.id} className={isFailed ? 'bg-rose-50/50' : ''}>
                          <TableCell className="font-mono text-xs break-all">
                            {evt.id}
                          </TableCell>
                          <TableCell className="text-xs sm:text-sm">
                                <div className="flex flex-col">
                                  <span>{getSourceLabel(evt.source)}</span>
                                  {metadata?.step && (
                                    <span className="text-xs text-muted-foreground">
                                      Step: {metadata.step}
                                    </span>
                                  )}
                                </div>
                          </TableCell>
                              <TableCell>
                                <StatusBadge status={evt.status} />
                          </TableCell>
                              <TableCell className="hidden md:table-cell">
                                <div className="flex flex-col gap-1">
                                  <IntegrationStatusBadge status={evt.integrationStatus} />
                                  {evt.seniorId && (
                                    <span className="text-xs text-muted-foreground">
                                      ID: {evt.seniorId}
                                    </span>
                                  )}
                                </div>
                              </TableCell>
                              <TableCell className="hidden lg:table-cell">
                                <div className="flex flex-col text-xs">
                                  {evt.processingTimeMs && (
                                    <span className="text-muted-foreground">
                                      Proc: {evt.processingTimeMs}ms
                                    </span>
                                  )}
                                  {evt.integrationTimeMs && (
                                    <span className="text-muted-foreground">
                                      Int: {evt.integrationTimeMs}ms
                                    </span>
                                  )}
                                </div>
                              </TableCell>
                              <TableCell className="text-right">
                                <div className="flex justify-end">
                                  <TruncatedCell 
                                    text={formatDate(evt.receivedAt)} 
                                    maxLength={20}
                                    title="Data e hora completa"
                                  />
                                </div>
                          </TableCell>
                          <TableCell className="relative">
                            <MetadataCell metadata={evt.metadata} />
                          </TableCell>
                        </TableRow>
                          );
                        })
                      )}
                    </TableBody>
                  </Table>
                </ScrollArea>
              </div>
            </CardContent>
          </Card>

          <Card className="border-muted">
            <CardHeader>
              <CardTitle>Alertas ativos</CardTitle>
              <CardDescription>Critérios automáticos configurados</CardDescription>
            </CardHeader>
            <CardContent className="space-y-5">
              {stats && stats.failed > 0 && (
                <div className="rounded-xl border border-dashed bg-rose-50/40 p-4">
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <p className="text-sm font-semibold">Falhas detectadas</p>
                      <p className="text-sm text-muted-foreground">
                        {stats.failed} eventos com falha
                      </p>
                    </div>
                    <Badge variant="outline" className="border-rose-200 text-rose-700">
                      Ativo
                    </Badge>
                  </div>
                </div>
              )}
              {stats && stats.pending > 10 && (
                <div className="rounded-xl border border-dashed bg-amber-50/40 p-4">
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <p className="text-sm font-semibold">Fila de processamento</p>
                      <p className="text-sm text-muted-foreground">
                        {stats.pending} eventos pendentes
                  </p>
                </div>
                    <Badge variant="outline" className="border-amber-200 text-amber-700">
                      Ativo
                    </Badge>
                  </div>
                </div>
              )}
              {stats?.integrationStats && stats.integrationStats.failed > 0 && (
                <div className="rounded-xl border border-dashed bg-rose-50/40 p-4">
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <p className="text-sm font-semibold">Falhas de integração</p>
                      <p className="text-sm text-muted-foreground">
                        {stats.integrationStats.failed} eventos não integrados
                      </p>
                    </div>
                    <Badge variant="outline" className="border-rose-200 text-rose-700">
                      Ativo
                    </Badge>
                  </div>
                </div>
              )}
              {(!stats || (stats.failed === 0 && stats.pending <= 10 && (!stats.integrationStats || stats.integrationStats.failed === 0))) && (
                <p className="text-center text-sm text-muted-foreground py-4">
                  Nenhum alerta ativo
                </p>
              )}
            </CardContent>
          </Card>
        </section>

        {/* Saúde das Integrações */}
        <section className="grid gap-4 lg:grid-cols-3">
          <Card className="border-muted lg:col-span-2">
            <CardHeader>
              <CardTitle>Saúde das integrações</CardTitle>
              <CardDescription>Status das principais rotas ESL e integração com Senior</CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              {loading && serviceHealth.length === 0 ? (
                <p className="text-center text-sm text-muted-foreground py-4">
                  Carregando...
                </p>
              ) : serviceHealth.length === 0 ? (
                <p className="text-center text-sm text-muted-foreground py-4">
                  Nenhum serviço encontrado
                </p>
              ) : (
                serviceHealth.map((service) => (
                <div
                  key={service.route}
                  className="flex flex-wrap items-center justify-between gap-4 rounded-2xl border bg-background/80 p-4"
                >
                  <div>
                    <p className="text-sm font-medium">{service.name}</p>
                    <p className="text-xs text-muted-foreground">
                      {service.route}
                    </p>
                  </div>
                  <div className="flex items-center gap-4 text-sm">
                    <span className="font-medium">{service.sla}</span>
                    <StatusBadge status={service.status as Status} />
                      <IntegrationStatusBadge status={service.integrationStatus} />
                  </div>
                </div>
                ))
              )}
            </CardContent>
          </Card>

          <Card className="border-muted">
            <CardHeader>
              <CardTitle>Falhas recentes</CardTitle>
              <CardDescription>Eventos que exigem intervenção</CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              {loading && failures.length === 0 ? (
                <p className="text-center text-sm text-muted-foreground py-4">
                  Carregando...
                </p>
              ) : recentErrors.length === 0 ? (
                <p className="text-center text-sm text-muted-foreground py-4">
                  Nenhuma falha crítica encontrada
                </p>
              ) : (
                recentErrors.map((err) => (
                <div
                  key={err.id}
                  className="rounded-2xl border border-rose-200/70 bg-rose-50/70 p-4 text-sm text-rose-900"
                >
                  <div className="flex items-start gap-3">
                    <AlertTriangle className="mt-0.5 h-4 w-4 text-rose-500" />
                    <div className="space-y-1">
                      <p className="font-semibold">{err.event}</p>
                        <p className="text-xs">{err.detail}</p>
                      <p className="text-xs text-rose-700">
                        {err.resolution} • {err.lastAttempt}
                      </p>
                    </div>
                  </div>
                </div>
                ))
              )}
              <Link href="/worker">
              <Button variant="outline" className="w-full gap-2">
                <ShieldCheck className="h-4 w-4" />
                Abrir fila de reprocessos
              </Button>
              </Link>
            </CardContent>
          </Card>
        </section>

        {/* Visão Consolidada */}
        <Card className="border-muted">
          <CardHeader className="pb-3">
            <Tabs defaultValue="eventos" className="w-full">
              <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
                <div>
                  <CardTitle>Visão consolidada</CardTitle>
                  <CardDescription>
                    Compare eventos recebidos, processados e integrados por categoria
                  </CardDescription>
                </div>
                <TabsList className="bg-muted/60">
                  <TabsTrigger value="eventos">Eventos</TabsTrigger>
                  <TabsTrigger value="integracao">Integração</TabsTrigger>
                  <TabsTrigger value="falhas">Falhas</TabsTrigger>
                </TabsList>
              </div>

              <TabsContent value="eventos" className="mt-6">
                <div className="grid gap-4 md:grid-cols-3">
                  <Card className="border border-emerald-200 bg-emerald-50/60">
                    <CardHeader className="pb-2">
                      <CardDescription>Processados com sucesso</CardDescription>
                      <CardTitle className="text-3xl">
                        {loading ? "..." : `${successRate}%`}
                      </CardTitle>
                    </CardHeader>
                    <CardContent className="text-sm text-emerald-900">
                      {stats && stats.successRate >= 95
                        ? "Dentro da meta (≥ 95%)"
                        : stats && stats.successRate >= 80
                        ? "Abaixo da meta"
                        : "Atenção necessária"}
                    </CardContent>
                  </Card>
                  <Card className="border border-blue-200 bg-blue-50/60">
                    <CardHeader className="pb-2">
                      <CardDescription>Tempo médio</CardDescription>
                      <CardTitle className="text-3xl">
                        {loading ? "..." : performance ? `${performance.avgProcessingTimeSeconds.toFixed(2)}s` : "0s"}
                      </CardTitle>
                    </CardHeader>
                    <CardContent className="text-sm text-blue-900">
                      {performance && performance.hourlyStats.length > 0
                        ? `Maior pico às ${performance.hourlyStats.reduce((a, b) => b.count > a.count ? b : a).hour}h`
                        : "Sem dados"}
                    </CardContent>
                  </Card>
                  <Card className="border border-amber-200 bg-amber-50/60">
                    <CardHeader className="pb-2">
                      <CardDescription>Eventos em retry</CardDescription>
                      <CardTitle className="text-3xl">
                        {loading ? "..." : failures.filter(f => f.retryCount > 0).length}
                      </CardTitle>
                    </CardHeader>
                    <CardContent className="text-sm text-amber-900">
                      {failures.filter(f => f.retryCount >= 3).length} aguardando intervenção
                    </CardContent>
                  </Card>
                </div>
              </TabsContent>

              <TabsContent value="integracao" className="mt-6">
                <div className="grid gap-4 md:grid-cols-4">
                  <Card className="border border-emerald-200 bg-emerald-50/60">
                    <CardHeader className="pb-2">
                      <CardDescription>Integrados</CardDescription>
                      <CardTitle className="text-3xl">
                        {loading ? "..." : stats?.integrationStats?.integrated || 0}
                      </CardTitle>
                    </CardHeader>
                    <CardContent className="text-sm text-emerald-900">
                      Eventos integrados com sucesso na Senior
                    </CardContent>
                  </Card>
                  <Card className="border border-amber-200 bg-amber-50/60">
                    <CardHeader className="pb-2">
                      <CardDescription>Pendentes</CardDescription>
                      <CardTitle className="text-3xl">
                        {loading ? "..." : stats?.integrationStats?.pending || 0}
                      </CardTitle>
                    </CardHeader>
                    <CardContent className="text-sm text-amber-900">
                      Aguardando integração
                    </CardContent>
                  </Card>
                  <Card className="border border-rose-200 bg-rose-50/60">
                    <CardHeader className="pb-2">
                      <CardDescription>Falhas</CardDescription>
                      <CardTitle className="text-3xl">
                        {loading ? "..." : stats?.integrationStats?.failed || 0}
                      </CardTitle>
                    </CardHeader>
                    <CardContent className="text-sm text-rose-900">
                      Falhas na integração
                    </CardContent>
                  </Card>
                  <Card className="border border-gray-200 bg-gray-50/60">
                    <CardHeader className="pb-2">
                      <CardDescription>Ignorados</CardDescription>
                      <CardTitle className="text-3xl">
                        {loading ? "..." : stats?.integrationStats?.skipped || 0}
                      </CardTitle>
                    </CardHeader>
                    <CardContent className="text-sm text-gray-900">
                      Eventos não requerem integração
                    </CardContent>
                  </Card>
                </div>
              </TabsContent>

              <TabsContent value="falhas" className="mt-6">
                <div className="space-y-4 text-sm">
                  {stats && stats.failed > 0 ? (
                  <div className="flex items-center justify-between rounded-2xl border border-rose-200 bg-rose-50/70 p-4">
                    <div>
                      <p className="font-medium text-rose-900">
                          Eventos com falha
                      </p>
                      <p className="text-xs text-rose-700">
                          {stats.failed} falhas nas últimas 24h
                      </p>
                    </div>
                    <Badge variant="outline" className="border-rose-200 text-rose-700">
                        {stats.total > 0 ? ((stats.failed / stats.total) * 100).toFixed(1) : 0}%
                    </Badge>
                  </div>
                  ) : (
                    <p className="text-center text-muted-foreground py-4">
                      Nenhuma falha registrada
                      </p>
                  )}
                </div>
              </TabsContent>
            </Tabs>
          </CardHeader>
        </Card>
      </div>
    </div>
  )
}
