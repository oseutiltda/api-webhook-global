'use client';

import { useEffect, useState } from 'react';
import {
  Activity,
  AlertCircle,
  CheckCircle2,
  Clock,
  RefreshCw,
  TrendingUp,
  TrendingDown,
  XCircle,
  Zap,
  FileText,
  Database,
  Server,
  BarChart3,
  PieChart as PieChartIcon,
  LineChart as LineChartIcon,
  Calendar,
  Target,
  ArrowUpRight,
  ArrowDownRight,
  Home as HomeIcon,
} from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  ChartLegend,
  ChartLegendContent,
} from '@/components/ui/chart';
import { cn } from '@/lib/utils';
import { useRouter } from 'next/navigation';
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
  Legend,
  AreaChart,
  Area,
  Tooltip,
  ComposedChart,
} from 'recharts';

// Função helper para obter a URL base da API.
// Em produção, evita `localhost` no browser do cliente e usa o mesmo host na porta 3000.
const getApiBase = (): string => {
  const normalizeBase = (base: string): string => base.replace(/\/$/, '').replace(/\/api$/i, '');

  const resolveClientApiBase = (): string => {
    if (typeof window === 'undefined') return 'http://localhost:3000';
    return `${window.location.protocol}//${window.location.hostname}:3000`;
  };

  const configured = process.env.NEXT_PUBLIC_API_BASE_URL?.trim();
  if (!configured) {
    return normalizeBase(resolveClientApiBase());
  }

  if (!configured.startsWith('http://') && !configured.startsWith('https://')) {
    return normalizeBase(configured);
  }

  if (typeof window === 'undefined') {
    return normalizeBase(configured);
  }

  try {
    const parsed = new URL(configured);
    if (parsed.hostname === 'localhost' || parsed.hostname === '127.0.0.1') {
      return normalizeBase(resolveClientApiBase());
    }

    if (window.location.protocol === 'https:' && parsed.protocol === 'http:') {
      parsed.protocol = 'https:';
      return normalizeBase(parsed.toString());
    }

    return normalizeBase(configured);
  } catch {
    return normalizeBase(resolveClientApiBase());
  }
};

// API_BASE será calculado dinamicamente dentro das funções de fetch

interface ProductivityData {
  period: string;
  summary: {
    totalEvents: number;
    totalProcessed: number;
    totalFailed: number;
    successRate: number;
    avgProcessingTimeMs: number;
    avgProcessingTimeSeconds: number;
  };
  byPeriod: Array<{
    periodo: string;
    total: number;
    processados: number;
    falhas: number;
    pendentes: number;
    processando: number;
    tempoMedioMs: number | null;
    taxaSucesso: number;
    integrados: number;
    integracaoFalhas: number;
    ciot: { unicos: number; total: number };
    nfse: { unicos: number; total: number };
    cte: { unicos: number; total: number };
  }>;
  byType: Array<{
    source: string;
    total: number;
    processados: number;
    falhas: number;
    tempoMedioMs: number | null;
    taxaSucesso: number;
  }>;
}

const CORES_GRAFICOS = [
  '#1E2F5B',
  '#3F5D97',
  '#6D85B6',
  '#EE3124',
  '#A96A1A',
  '#2F7A58',
  '#B7C4D9',
  '#667085',
];

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
    '/api/CIOT/InserirContasPagarCIOT': 'CIOT - Inserir',
    '/api/CIOT/CancelarContasPagarCIOT': 'CIOT - Cancelar',
    '/api/NFSe/InserirNFSe': 'NFSe - Inserir',
  };

  return labels[source] || source;
}

export default function Worker1Dashboard() {
  const router = useRouter();
  const [productivity, setProductivity] = useState<ProductivityData | null>(null);
  const [loading, setLoading] = useState(true);
  const [timePeriod, setTimePeriod] = useState<'diario' | 'semanal' | 'mensal'>('mensal');

  // Proteção de rota: exige login (auth_token no localStorage)
  useEffect(() => {
    if (typeof window === 'undefined') return;
    const token = localStorage.getItem('auth_token');
    if (!token) {
      // Usar router.push() ao invés de window.location.href para evitar erro de header inválido
      router.push('/login');
    }
  }, [router]);

  const fetchProductivity = async () => {
    try {
      const apiBase = getApiBase();
      const url = apiBase
        ? `${apiBase}/api/worker/productivity?period=${timePeriod}`
        : `/api/worker/productivity?period=${timePeriod}`;
      const res = await fetch(url);
      if (!res.ok) {
        const errorText = await res.text();
        console.error(`HTTP ${res.status}:`, errorText);
        throw new Error(`HTTP ${res.status}: ${errorText}`);
      }
      const data = await res.json();
      console.log('Dados de produtividade recebidos:', {
        period: data.period,
        summary: data.summary,
        byPeriodCount: data.byPeriod?.length || 0,
        byTypeCount: data.byType?.length || 0,
      });
      setProductivity(data);
    } catch (error) {
      console.error('Erro ao buscar produtividade:', error);
      setProductivity(null);
    }
  };

  const loadAll = async () => {
    setLoading(true);
    await fetchProductivity();
    setLoading(false);
  };

  useEffect(() => {
    loadAll();
    const interval = setInterval(loadAll, 60000); // Atualizar a cada 1 minuto
    return () => clearInterval(interval);
  }, [timePeriod]);

  // Calcular comparações e tendências
  const calculateTrend = (data: ProductivityData['byPeriod']) => {
    if (data.length < 2) return { direction: 'neutral', percentage: 0 };
    const recent = data.slice(-7);
    const previous = data.slice(-14, -7);
    const recentAvg = recent.reduce((sum, d) => sum + d.processados, 0) / recent.length;
    const previousAvg =
      previous.length > 0
        ? previous.reduce((sum, d) => sum + d.processados, 0) / previous.length
        : recentAvg;
    const change = ((recentAvg - previousAvg) / (previousAvg || 1)) * 100;
    return {
      direction: change > 0 ? 'up' : change < 0 ? 'down' : 'neutral',
      percentage: Math.abs(change),
    };
  };

  const trend = productivity?.byPeriod
    ? calculateTrend(productivity.byPeriod)
    : { direction: 'neutral', percentage: 0 };

  // Preparar dados para gráficos
  const dadosPorPeriodo = productivity?.byPeriod || [];
  const dadosRegistrosUnicos =
    productivity?.byPeriod.map((p) => ({
      periodo: p.periodo,
      ciot: p.ciot.unicos,
      nfse: p.nfse.unicos,
      cte: p.cte.unicos,
    })) || [];

  const dadosTaxaSucesso =
    productivity?.byPeriod.map((p) => ({
      periodo: p.periodo,
      taxaSucesso: p.taxaSucesso,
    })) || [];

  const dadosTempoMedio =
    productivity?.byPeriod
      .filter((p) => p.tempoMedioMs !== null)
      .map((p) => ({
        periodo: p.periodo,
        tempoSegundos: p.tempoMedioMs ? (p.tempoMedioMs / 1000).toFixed(2) : 0,
      })) || [];

  const dadosPorTipo =
    productivity?.byType
      .sort((a, b) => b.total - a.total)
      .slice(0, 8)
      .map((item, idx) => ({
        nome: item.source, // Já vem agrupado do backend (NFSe, CIOT, CT-e, etc.)
        total: item.total,
        processados: item.processados,
        falhas: item.falhas,
        taxaSucesso: item.taxaSucesso,
        cor: CORES_GRAFICOS[idx % CORES_GRAFICOS.length],
      })) || [];

  const dadosIntegracao =
    productivity?.byPeriod.map((p) => ({
      periodo: p.periodo,
      integrados: p.integrados,
      falhasIntegracao: p.integracaoFalhas,
    })) || [];

  const summary = productivity?.summary || {
    totalEvents: 0,
    totalProcessed: 0,
    totalFailed: 0,
    successRate: 0,
    avgProcessingTimeMs: 0,
    avgProcessingTimeSeconds: 0,
  };

  return (
    <div className="page-shell">
      <div className="mx-auto max-w-7xl space-y-6 px-4 pb-8 pt-10">
        {/* Cabeçalho */}
        <div className="page-header flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
          <div>
            <Badge variant="default" className="mb-3">
              Analytics Global
            </Badge>
            <h1 className="text-3xl font-semibold tracking-tight text-foreground">
              Analytics operacional
            </h1>
            <p className="text-sm text-muted-foreground">
              Análise detalhada de performance, produtividade e integração do sistema
            </p>
          </div>
          <div className="flex items-center gap-4">
            <Tabs
              value={timePeriod}
              onValueChange={(v) => setTimePeriod(v as 'diario' | 'semanal' | 'mensal')}
            >
              <TabsList>
                <TabsTrigger value="diario">
                  <Calendar className="h-4 w-4 mr-2" />
                  Diário
                </TabsTrigger>
                <TabsTrigger value="semanal">
                  <Calendar className="h-4 w-4 mr-2" />
                  Semanal
                </TabsTrigger>
                <TabsTrigger value="mensal">
                  <Calendar className="h-4 w-4 mr-2" />
                  Mensal
                </TabsTrigger>
              </TabsList>
            </Tabs>
            <Button onClick={() => router.push('/dashboard')} variant="outline" size="sm">
              <HomeIcon className="w-4 h-4 mr-2" />
              Voltar
            </Button>
            <Button onClick={loadAll} disabled={loading} variant="outline" size="sm">
              <RefreshCw className={cn('mr-2 h-4 w-4', loading && 'animate-spin')} />
              Atualizar
            </Button>
          </div>
        </div>

        {/* Cards Principais de Métricas */}
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
          <Card className="border-muted">
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Total de Eventos</CardTitle>
              <Database className="h-4 w-4 text-[var(--brand-accent)]" />
            </CardHeader>
            <CardContent>
              <div className="metric-brand text-3xl font-bold">
                {loading ? '...' : summary.totalEvents.toLocaleString('pt-BR')}
              </div>
              <div className="flex items-center gap-2 mt-2">
                {trend.direction === 'up' && (
                  <>
                    <ArrowUpRight className="h-4 w-4 text-[var(--brand-success)]" />
                    <span className="text-xs text-[var(--brand-success)]">
                      +{trend.percentage.toFixed(1)}%
                    </span>
                  </>
                )}
                {trend.direction === 'down' && (
                  <>
                    <ArrowDownRight className="h-4 w-4 text-[var(--brand-danger)]" />
                    <span className="text-xs text-[var(--brand-danger)]">
                      -{trend.percentage.toFixed(1)}%
                    </span>
                  </>
                )}
                {trend.direction === 'neutral' && (
                  <span className="text-xs text-muted-foreground">Sem mudança</span>
                )}
              </div>
            </CardContent>
          </Card>

          <Card className="border-muted">
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Taxa de Sucesso</CardTitle>
              <Target className="h-4 w-4 text-[var(--brand-success)]" />
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold text-[var(--brand-success)]">
                {loading ? '...' : `${summary.successRate.toFixed(1)}%`}
              </div>
              <p className="text-xs text-muted-foreground mt-1">
                {summary.totalProcessed.toLocaleString('pt-BR')} processados
              </p>
            </CardContent>
          </Card>

          <Card className="border-muted">
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Tempo Médio</CardTitle>
              <Clock className="h-4 w-4 text-[var(--brand-info)]" />
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold text-[var(--brand-info)]">
                {loading ? '...' : `${summary.avgProcessingTimeSeconds.toFixed(2)}s`}
              </div>
              <p className="text-xs text-muted-foreground mt-1">
                {summary.avgProcessingTimeMs.toLocaleString('pt-BR')}ms
              </p>
            </CardContent>
          </Card>

          <Card className="border-muted">
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Falhas</CardTitle>
              <XCircle className="h-4 w-4 text-[var(--brand-danger)]" />
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold text-[var(--brand-danger)]">
                {loading ? '...' : summary.totalFailed.toLocaleString('pt-BR')}
              </div>
              <p className="text-xs text-muted-foreground mt-1">
                {summary.totalEvents > 0
                  ? `${((summary.totalFailed / summary.totalEvents) * 100).toFixed(1)}% do total`
                  : '0%'}
              </p>
            </CardContent>
          </Card>
        </div>

        {/* Gráfico 1: Eventos Processados vs Falhas por Período */}
        <Card className="border-muted">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <LineChartIcon className="h-5 w-5" />
              Eventos Processados vs Falhas por Período
            </CardTitle>
            <CardDescription>
              Comparação de eventos processados e falhas ao longo do tempo ({timePeriod})
            </CardDescription>
          </CardHeader>
          <CardContent>
            <ChartContainer
              config={{
                processados: { label: 'Processados', color: '#2F7A58' },
                falhas: { label: 'Falhas', color: '#EE3124' },
                pendentes: { label: 'Pendentes', color: '#A96A1A' },
              }}
              className="h-[400px]"
            >
              <ComposedChart data={dadosPorPeriodo}>
                <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                <XAxis dataKey="periodo" className="text-xs" tick={{ fill: 'currentColor' }} />
                <YAxis className="text-xs" tick={{ fill: 'currentColor' }} />
                <ChartTooltip content={<ChartTooltipContent />} />
                <ChartLegend content={<ChartLegendContent />} />
                <Bar dataKey="processados" fill="#2F7A58" name="Processados" />
                <Bar dataKey="falhas" fill="#EE3124" name="Falhas" />
                <Line
                  type="monotone"
                  dataKey="pendentes"
                  stroke="#A96A1A"
                  strokeWidth={2}
                  name="Pendentes"
                />
              </ComposedChart>
            </ChartContainer>
          </CardContent>
        </Card>

        {/* Gráficos Linha 2: Taxa de Sucesso e Registros Únicos */}
        <div className="grid gap-6 lg:grid-cols-2">
          {/* Taxa de Sucesso por Período */}
          <Card className="border-muted">
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <TrendingUp className="h-5 w-5" />
                Taxa de Sucesso por Período
              </CardTitle>
              <CardDescription>Percentual de eventos processados com sucesso</CardDescription>
            </CardHeader>
            <CardContent>
              <ChartContainer
                config={{
                  taxaSucesso: { label: 'Taxa de Sucesso (%)', color: '#1E2F5B' },
                }}
                className="h-[300px]"
              >
                <AreaChart data={dadosTaxaSucesso}>
                  <defs>
                    <linearGradient id="colorTaxaSucesso" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#1E2F5B" stopOpacity={0.8} />
                      <stop offset="95%" stopColor="#1E2F5B" stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                  <XAxis dataKey="periodo" className="text-xs" tick={{ fill: 'currentColor' }} />
                  <YAxis className="text-xs" domain={[0, 100]} tick={{ fill: 'currentColor' }} />
                  <ChartTooltip content={<ChartTooltipContent />} />
                  <Area
                    type="monotone"
                    dataKey="taxaSucesso"
                    stroke="#1E2F5B"
                    fillOpacity={1}
                    fill="url(#colorTaxaSucesso)"
                  />
                </AreaChart>
              </ChartContainer>
            </CardContent>
          </Card>

          {/* Registros Únicos por Tipo */}
          <Card className="border-muted">
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <BarChart3 className="h-5 w-5" />
                Registros Únicos por Tipo
              </CardTitle>
              <CardDescription>Quantidade de registros únicos processados</CardDescription>
            </CardHeader>
            <CardContent>
              <ChartContainer
                config={{
                  ciot: { label: 'CIOT', color: '#1E2F5B' },
                  nfse: { label: 'NFSE', color: '#2F7A58' },
                  cte: { label: 'CT-e', color: '#3F5D97' },
                }}
                className="h-[300px]"
              >
                <BarChart data={dadosRegistrosUnicos}>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                  <XAxis dataKey="periodo" className="text-xs" tick={{ fill: 'currentColor' }} />
                  <YAxis className="text-xs" tick={{ fill: 'currentColor' }} />
                  <ChartTooltip content={<ChartTooltipContent />} />
                  <ChartLegend content={<ChartLegendContent />} />
                  <Bar dataKey="ciot" stackId="a" fill="#1E2F5B" name="CIOT" />
                  <Bar dataKey="nfse" stackId="a" fill="#2F7A58" name="NFSE" />
                  <Bar dataKey="cte" stackId="a" fill="#3F5D97" name="CT-e" />
                </BarChart>
              </ChartContainer>
            </CardContent>
          </Card>
        </div>

        {/* Gráficos Linha 3: Tempo Médio e Integração */}
        <div className="grid gap-6 lg:grid-cols-2">
          {/* Tempo Médio de Processamento */}
          <Card className="border-muted">
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Zap className="h-5 w-5" />
                Tempo Médio de Processamento
              </CardTitle>
              <CardDescription>Evolução do tempo médio de processamento</CardDescription>
            </CardHeader>
            <CardContent>
              <ChartContainer
                config={{
                  tempoSegundos: { label: 'Tempo (s)', color: '#A96A1A' },
                }}
                className="h-[300px]"
              >
                <LineChart data={dadosTempoMedio}>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                  <XAxis dataKey="periodo" className="text-xs" tick={{ fill: 'currentColor' }} />
                  <YAxis className="text-xs" tick={{ fill: 'currentColor' }} />
                  <ChartTooltip content={<ChartTooltipContent />} />
                  <Line
                    type="monotone"
                    dataKey="tempoSegundos"
                    stroke="#A96A1A"
                    strokeWidth={3}
                    dot={{ fill: '#A96A1A', r: 5 }}
                  />
                </LineChart>
              </ChartContainer>
            </CardContent>
          </Card>

          {/* Status de Integração */}
          <Card className="border-muted">
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Activity className="h-5 w-5" />
                Status de Integração
              </CardTitle>
              <CardDescription>Integrados vs Falhas de integração</CardDescription>
            </CardHeader>
            <CardContent>
              <ChartContainer
                config={{
                  integrados: { label: 'Integrados', color: '#2F7A58' },
                  falhasIntegracao: { label: 'Falhas', color: '#EE3124' },
                }}
                className="h-[300px]"
              >
                <AreaChart data={dadosIntegracao}>
                  <defs>
                    <linearGradient id="colorIntegrados" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#2F7A58" stopOpacity={0.8} />
                      <stop offset="95%" stopColor="#2F7A58" stopOpacity={0} />
                    </linearGradient>
                    <linearGradient id="colorFalhasIntegracao" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#EE3124" stopOpacity={0.8} />
                      <stop offset="95%" stopColor="#EE3124" stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                  <XAxis dataKey="periodo" className="text-xs" tick={{ fill: 'currentColor' }} />
                  <YAxis className="text-xs" tick={{ fill: 'currentColor' }} />
                  <ChartTooltip content={<ChartTooltipContent />} />
                  <ChartLegend content={<ChartLegendContent />} />
                  <Area
                    type="monotone"
                    dataKey="integrados"
                    stroke="#2F7A58"
                    fillOpacity={1}
                    fill="url(#colorIntegrados)"
                  />
                  <Area
                    type="monotone"
                    dataKey="falhasIntegracao"
                    stroke="#EE3124"
                    fillOpacity={1}
                    fill="url(#colorFalhasIntegracao)"
                  />
                </AreaChart>
              </ChartContainer>
            </CardContent>
          </Card>
        </div>

        {/* Gráfico: Distribuição por Tipo de Evento */}
        <Card className="border-muted">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <PieChartIcon className="h-5 w-5" />
              Distribuição por Tipo de Evento
            </CardTitle>
            <CardDescription>Top 8 tipos de eventos mais processados</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="grid gap-6 lg:grid-cols-2">
              <ChartContainer
                config={dadosPorTipo.reduce(
                  (acc, item) => {
                    acc[item.nome.toLowerCase().replace(/\s+/g, '_')] = {
                      label: item.nome,
                      color: item.cor,
                    };
                    return acc;
                  },
                  {} as Record<string, { label: string; color: string }>,
                )}
                className="h-[300px]"
              >
                <PieChart>
                  <Pie
                    data={dadosPorTipo}
                    cx="50%"
                    cy="50%"
                    labelLine={false}
                    label={({ nome, percent }) => `${nome}: ${(percent * 100).toFixed(0)}%`}
                    outerRadius={100}
                    fill="#8884d8"
                    dataKey="total"
                  >
                    {dadosPorTipo.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.cor} />
                    ))}
                  </Pie>
                  <ChartTooltip content={<ChartTooltipContent />} />
                  <ChartLegend content={<ChartLegendContent />} />
                </PieChart>
              </ChartContainer>

              {/* Tabela de Detalhes */}
              <div>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Tipo</TableHead>
                      <TableHead className="text-right">Total</TableHead>
                      <TableHead className="text-right">Taxa Sucesso</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {dadosPorTipo.length === 0 ? (
                      <TableRow>
                        <TableCell colSpan={3} className="text-center text-muted-foreground">
                          Nenhum dado disponível
                        </TableCell>
                      </TableRow>
                    ) : (
                      dadosPorTipo.map((item, idx) => (
                        <TableRow key={idx}>
                          <TableCell className="flex items-center gap-2">
                            <div
                              className="h-3 w-3 rounded-full"
                              style={{ backgroundColor: item.cor }}
                            />
                            {item.nome}
                          </TableCell>
                          <TableCell className="text-right font-bold">
                            {item.total.toLocaleString('pt-BR')}
                          </TableCell>
                          <TableCell className="text-right">
                            <Badge
                              variant={
                                item.taxaSucesso >= 95
                                  ? 'default'
                                  : item.taxaSucesso >= 80
                                    ? 'secondary'
                                    : 'destructive'
                              }
                            >
                              {item.taxaSucesso.toFixed(1)}%
                            </Badge>
                          </TableCell>
                        </TableRow>
                      ))
                    )}
                  </TableBody>
                </Table>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Resumo de Registros Únicos */}
        <Card className="border-muted">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Target className="h-5 w-5" />
              Resumo de Registros Únicos
            </CardTitle>
            <CardDescription>Total de registros únicos processados no período</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="grid gap-4 md:grid-cols-3">
              {productivity?.byPeriod && (
                <>
                  <div className="surface-brand text-center rounded-lg border p-4">
                    <p className="text-sm text-muted-foreground mb-2">CIOT Únicos</p>
                    <p className="metric-brand text-3xl font-bold">
                      {productivity.byPeriod
                        .reduce((sum, p) => sum + p.ciot.unicos, 0)
                        .toLocaleString('pt-BR')}
                    </p>
                    <p className="text-xs text-muted-foreground mt-1">
                      {productivity.byPeriod
                        .reduce((sum, p) => sum + p.ciot.total, 0)
                        .toLocaleString('pt-BR')}{' '}
                      eventos totais
                    </p>
                  </div>
                  <div className="surface-success text-center rounded-lg border p-4">
                    <p className="text-sm text-muted-foreground mb-2">NFSE Únicos</p>
                    <p className="text-3xl font-bold text-[var(--brand-success)]">
                      {productivity.byPeriod
                        .reduce((sum, p) => sum + p.nfse.unicos, 0)
                        .toLocaleString('pt-BR')}
                    </p>
                    <p className="text-xs text-muted-foreground mt-1">
                      {productivity.byPeriod
                        .reduce((sum, p) => sum + p.nfse.total, 0)
                        .toLocaleString('pt-BR')}{' '}
                      eventos totais
                    </p>
                  </div>
                  <div className="surface-info text-center rounded-lg border p-4">
                    <p className="text-sm text-muted-foreground mb-2">CT-e Únicos</p>
                    <p className="text-3xl font-bold text-[var(--brand-info)]">
                      {productivity.byPeriod
                        .reduce((sum, p) => sum + p.cte.unicos, 0)
                        .toLocaleString('pt-BR')}
                    </p>
                    <p className="text-xs text-muted-foreground mt-1">
                      {productivity.byPeriod
                        .reduce((sum, p) => sum + p.cte.total, 0)
                        .toLocaleString('pt-BR')}{' '}
                      eventos totais
                    </p>
                  </div>
                </>
              )}
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
