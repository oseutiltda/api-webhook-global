'use client';

import { useEffect, useState } from 'react';
import {
  Activity,
  AlertCircle,
  CheckCircle2,
  Clock,
  RefreshCw,
  TrendingUp,
  XCircle,
  Zap,
  Home as HomeIcon,
} from 'lucide-react';

import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Separator } from '@/components/ui/separator';
import { useRouter } from 'next/navigation';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { cn } from '@/lib/utils';
import { MetadataCell } from '@/components/ui/metadata-cell';
import { TruncatedCell } from '@/components/ui/truncated-cell';

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

interface WorkerStats {
  total: number;
  totalLast24h?: number; // Total de eventos nas últimas 24h
  pending: number;
  processing: number;
  processed: number;
  failed: number;
  failedLast24h?: number; // Falhas nas últimas 24h
  processedLast24h: number;
  successRate: number;
  eventsByType: Array<{ source: string; count: number }>;
}

interface WorkerEvent {
  id: string;
  source: string;
  receivedAt: string;
  status: string | null;
  processedAt: string | null;
  errorMessage: string | null;
  retryCount: number;
  metadata: string | null;
}

interface PerformanceMetrics {
  avgProcessingTimeMs: number;
  avgProcessingTimeSeconds: number;
  totalProcessed: number;
  hourlyStats: Array<{ hour: number; count: number }>;
}

function StatusBadge({ status }: { status: string | null }) {
  const statusMap: Record<string, { label: string; className: string }> = {
    pending: { label: 'Pendente', className: 'border-blue-200 bg-blue-500/10 text-blue-600' },
    processing: { label: 'Processando', className: 'border-sky-200 bg-sky-500/10 text-sky-600' },
    processed: {
      label: 'Processado',
      className: 'border-emerald-200 bg-emerald-500/10 text-emerald-600',
    },
    failed: { label: 'Falhou', className: 'border-rose-200 bg-rose-500/10 text-rose-600' },
  };

  const config = statusMap[status || 'pending'] || statusMap.pending;

  return (
    <Badge variant="outline" className={cn('capitalize', config.className)}>
      {config.label}
    </Badge>
  );
}

export default function WorkerDashboard() {
  const router = useRouter();
  const [stats, setStats] = useState<WorkerStats | null>(null);
  const [events, setEvents] = useState<WorkerEvent[]>([]);
  const [failures, setFailures] = useState<WorkerEvent[]>([]);
  const [performance, setPerformance] = useState<PerformanceMetrics | null>(null);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(1);
  const [statusFilter, setStatusFilter] = useState<string>('all');

  // Proteção de rota: exige login (auth_token no localStorage)
  useEffect(() => {
    if (typeof window === 'undefined') return;
    const token = localStorage.getItem('auth_token');
    if (!token) {
      // Usar router.push() ao invés de window.location.href para evitar erro de header inválido
      router.push('/login');
    }
  }, [router]);

  const fetchStats = async () => {
    try {
      const apiBase = getApiBase();
      const url = apiBase ? `${apiBase}/api/worker/stats` : '/api/worker/stats';
      const res = await fetch(url);
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}: ${res.statusText}`);
      }
      const data = await res.json();
      setStats(
        data || {
          total: 0,
          pending: 0,
          processing: 0,
          processed: 0,
          failed: 0,
          processedLast24h: 0,
          successRate: 0,
          eventsByType: [],
        },
      );
    } catch (error) {
      console.error('Erro ao buscar estatísticas:', error);
      setStats({
        total: 0,
        pending: 0,
        processing: 0,
        processed: 0,
        failed: 0,
        processedLast24h: 0,
        successRate: 0,
        eventsByType: [],
      });
    }
  };

  const fetchEvents = async () => {
    try {
      const params = new URLSearchParams({
        page: page.toString(),
        limit: '50',
      });
      if (statusFilter !== 'all') {
        params.append('status', statusFilter);
      }
      const apiBase = getApiBase();
      const url = apiBase
        ? `${apiBase}/api/worker/events?${params}`
        : `/api/worker/events?${params}`;
      const res = await fetch(url);
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}: ${res.statusText}`);
      }
      const data = await res.json();
      setEvents(data.events || []);
    } catch (error) {
      console.error('Erro ao buscar eventos:', error);
      setEvents([]);
    }
  };

  const fetchFailures = async () => {
    try {
      const apiBase = getApiBase();
      const url = apiBase ? `${apiBase}/api/worker/failures` : '/api/worker/failures';
      const res = await fetch(url);
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}: ${res.statusText}`);
      }
      const data = await res.json();
      setFailures(data || []);
    } catch (error) {
      console.error('Erro ao buscar falhas:', error);
      setFailures([]);
    }
  };

  const fetchPerformance = async () => {
    try {
      const apiBase = getApiBase();
      const url = apiBase ? `${apiBase}/api/worker/performance` : '/api/worker/performance';
      const res = await fetch(url);
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}: ${res.statusText}`);
      }
      const data = await res.json();
      setPerformance(
        data || {
          avgProcessingTimeSeconds: 0,
          avgProcessingTimeMs: 0,
          totalProcessed: 0,
          hourlyStats: [],
        },
      );
    } catch (error) {
      console.error('Erro ao buscar performance:', error);
      setPerformance({
        avgProcessingTimeSeconds: 0,
        avgProcessingTimeMs: 0,
        totalProcessed: 0,
        hourlyStats: [],
      });
    }
  };

  const loadAll = async () => {
    setLoading(true);
    await Promise.all([fetchStats(), fetchEvents(), fetchFailures(), fetchPerformance()]);
    setLoading(false);
  };

  useEffect(() => {
    loadAll();
    const interval = setInterval(loadAll, 10000); // Atualizar a cada 10s
    return () => clearInterval(interval);
  }, [page, statusFilter]);

  useEffect(() => {
    fetchEvents();
  }, [page, statusFilter]);

  const formatDate = (dateString: string | null) => {
    if (!dateString) return '-';
    return new Date(dateString).toLocaleString('pt-BR');
  };

  const getSourceLabel = (source: string) => {
    const labels: Record<string, string> = {
      '/webhooks/cte/autorizado': 'CT-e Autorizado',
      '/webhooks/cte/cancelado': 'CT-e Cancelado',
      '/webhooks/ctrb/ciot/parcelas': 'CIOT Parcelas',
      '/webhooks/faturas/pagar/criar': 'Fatura Pagar',
      '/webhooks/faturas/receber/criar': 'Fatura Receber',
      '/webhooks/nfse/autorizado': 'NFSe Autorizado',
      '/webhooks/pessoa/upsert': 'Pessoa Upsert',
    };

    // Eventos processados pelo worker (NFSe)
    if (source.startsWith('worker/nfse/')) {
      // Extrair número da NFSe do formato: "worker/nfse/{id} (NFSe: {numero})"
      const nfseMatch = source.match(/\(NFSe:\s*(\d+)\)/);
      if (nfseMatch && nfseMatch[1]) {
        return `NFSe Processada (Numero: ${nfseMatch[1]})`;
      }
      // Fallback: se não tiver o número, usar o ID
      const nfseId = source.split('/').pop()?.split(' ')[0] || 'N/A';
      return `NFSe Processada (ID: ${nfseId})`;
    }

    return labels[source] || source;
  };

  return (
    <div className="min-h-screen bg-muted/40 pb-12">
      <div className="mx-auto flex max-w-7xl flex-col gap-6 px-4 pb-8 pt-10 lg:px-0">
        <header className="flex flex-col gap-4 rounded-3xl border bg-card/70 p-6 shadow-sm backdrop-blur lg:flex-row lg:items-center lg:justify-between">
          <div>
            <h1 className="text-3xl font-semibold tracking-tight text-foreground">
              Dashboard do Worker
            </h1>
            <p className="text-sm text-muted-foreground">
              Monitoramento em tempo real do processamento de eventos
            </p>
          </div>
          <div className="flex gap-2">
            <Button onClick={() => router.push('/dashboard')} variant="outline" size="sm">
              <HomeIcon className="w-4 h-4 mr-2" />
              Voltar
            </Button>
            <Button onClick={loadAll} disabled={loading} variant="outline" size="sm">
              <RefreshCw className={cn('mr-2 h-4 w-4', loading && 'animate-spin')} />
              Atualizar
            </Button>
          </div>
        </header>

        {/* Cards de Estatísticas */}
        {stats && (
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">Total de Eventos</CardTitle>
                <Activity className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">
                  {((stats.totalLast24h ?? stats.total) || 0).toLocaleString()}
                </div>
                <p className="text-xs text-muted-foreground">Últimas 24 horas</p>
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">Pendentes</CardTitle>
                <Clock className="h-4 w-4 text-blue-500" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold text-blue-600">{stats.pending || 0}</div>
                <p className="text-xs text-muted-foreground">Aguardando processamento</p>
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">Processados (24h)</CardTitle>
                <CheckCircle2 className="h-4 w-4 text-emerald-500" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold text-emerald-600">
                  {(stats.processedLast24h || 0).toLocaleString()}
                </div>
                <p className="text-xs text-muted-foreground">
                  Taxa de sucesso: {stats.successRate || 0}%
                </p>
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">Falhas</CardTitle>
                <XCircle className="h-4 w-4 text-rose-500" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold text-rose-600">
                  {((stats.failedLast24h ?? stats.failed) || 0).toLocaleString()}
                </div>
                <p className="text-xs text-muted-foreground">
                  {failures.length} requerem atenção (últimas 24h)
                </p>
              </CardContent>
            </Card>
          </div>
        )}

        {/* Métricas de Performance */}
        {performance && (
          <Card>
            <CardHeader>
              <CardTitle>Performance</CardTitle>
              <CardDescription>Métricas de processamento (últimas 24h)</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="grid gap-4 md:grid-cols-3">
                <div>
                  <p className="text-sm text-muted-foreground">Tempo médio</p>
                  <p className="text-2xl font-bold">{performance.avgProcessingTimeSeconds || 0}s</p>
                  <p className="text-xs text-muted-foreground">
                    ({(performance.avgProcessingTimeMs || 0).toLocaleString()}ms)
                  </p>
                </div>
                <div>
                  <p className="text-sm text-muted-foreground">Total processado</p>
                  <p className="text-2xl font-bold">{performance.totalProcessed || 0}</p>
                  <p className="text-xs text-muted-foreground">Últimas 24 horas</p>
                </div>
                <div>
                  <p className="text-sm text-muted-foreground">Taxa de processamento</p>
                  <p className="text-2xl font-bold">
                    {(performance.totalProcessed || 0) > 0
                      ? Math.round(((performance.totalProcessed || 0) / 24) * 10) / 10
                      : 0}
                  </p>
                  <p className="text-xs text-muted-foreground">Eventos por hora</p>
                </div>
              </div>
            </CardContent>
          </Card>
        )}

        {/* Tabs com Eventos e Falhas */}
        <Tabs defaultValue="events" className="w-full">
          <TabsList>
            <TabsTrigger value="events">Eventos Recentes</TabsTrigger>
            <TabsTrigger value="failures">
              Falhas Críticas
              {failures.length > 0 && (
                <Badge variant="destructive" className="ml-2">
                  {failures.length}
                </Badge>
              )}
            </TabsTrigger>
            <TabsTrigger value="stats">Estatísticas por Tipo</TabsTrigger>
          </TabsList>

          <TabsContent value="events" className="space-y-4">
            <Card>
              <CardHeader>
                <div className="flex items-center justify-between">
                  <div>
                    <CardTitle>Eventos Recentes</CardTitle>
                    <CardDescription>Últimos eventos processados pelo worker</CardDescription>
                  </div>
                  <div className="flex gap-2">
                    <select
                      value={statusFilter}
                      onChange={(e) => {
                        setStatusFilter(e.target.value);
                        setPage(1);
                      }}
                      className="flex h-10 w-40 rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background file:border-0 file:bg-transparent file:text-sm file:font-medium placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50"
                    >
                      <option value="all">Todos os status</option>
                      <option value="pending">Pendente</option>
                      <option value="processing">Processando</option>
                      <option value="processed">Processado</option>
                      <option value="failed">Falhou</option>
                    </select>
                  </div>
                </div>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <ScrollArea className="h-[600px]">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead className="min-w-[120px]">ID</TableHead>
                          <TableHead className="min-w-[120px]">Tipo</TableHead>
                          <TableHead className="min-w-[100px]">Status</TableHead>
                          <TableHead className="min-w-[140px] hidden md:table-cell">
                            Recebido em
                          </TableHead>
                          <TableHead className="min-w-[140px] hidden lg:table-cell">
                            Processado em
                          </TableHead>
                          <TableHead className="min-w-[60px]">Retries</TableHead>
                          <TableHead className="min-w-[150px] hidden sm:table-cell">Erro</TableHead>
                          <TableHead className="min-w-[180px]">Metadados</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {events.length === 0 ? (
                          <TableRow>
                            <TableCell colSpan={8} className="text-center text-muted-foreground">
                              Nenhum evento encontrado
                            </TableCell>
                          </TableRow>
                        ) : (
                          events.map((event) => (
                            <TableRow key={event.id}>
                              <TableCell className="font-mono text-xs break-all">
                                {event.id}
                              </TableCell>
                              <TableCell className="text-xs sm:text-sm">
                                {getSourceLabel(event.source)}
                              </TableCell>
                              <TableCell>
                                <StatusBadge status={event.status} />
                              </TableCell>
                              <TableCell className="text-xs sm:text-sm hidden md:table-cell">
                                {formatDate(event.receivedAt)}
                              </TableCell>
                              <TableCell className="text-xs sm:text-sm hidden lg:table-cell">
                                {formatDate(event.processedAt)}
                              </TableCell>
                              <TableCell className="text-center">{event.retryCount}</TableCell>
                              <TableCell className="hidden sm:table-cell">
                                <TruncatedCell
                                  text={event.errorMessage || null}
                                  maxLength={50}
                                  title="Mensagem de erro completa"
                                  textClassName="text-muted-foreground"
                                />
                              </TableCell>
                              <TableCell className="relative">
                                <MetadataCell metadata={event.metadata} />
                              </TableCell>
                            </TableRow>
                          ))
                        )}
                      </TableBody>
                    </Table>
                  </ScrollArea>
                </div>
                <div className="mt-4 flex items-center justify-between">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => setPage((p) => Math.max(1, p - 1))}
                    disabled={page === 1}
                  >
                    Anterior
                  </Button>
                  <span className="text-sm text-muted-foreground">Página {page}</span>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => setPage((p) => p + 1)}
                    disabled={events.length < 50}
                  >
                    Próxima
                  </Button>
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="failures" className="space-y-4">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <AlertCircle className="h-5 w-5 text-rose-500" />
                  Falhas Críticas
                </CardTitle>
                <CardDescription>
                  Eventos que falharam após {stats?.failed || 0} tentativas
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <ScrollArea className="h-[600px]">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead className="min-w-[120px]">ID</TableHead>
                          <TableHead className="min-w-[120px]">Tipo</TableHead>
                          <TableHead className="min-w-[80px]">Retries</TableHead>
                          <TableHead className="min-w-[140px] hidden md:table-cell">
                            Recebido em
                          </TableHead>
                          <TableHead className="min-w-[200px]">Erro</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {failures.length === 0 ? (
                          <TableRow>
                            <TableCell colSpan={5} className="text-center text-muted-foreground">
                              Nenhuma falha crítica encontrada
                            </TableCell>
                          </TableRow>
                        ) : (
                          failures.map((event) => (
                            <TableRow key={event.id}>
                              <TableCell className="font-mono text-xs break-all">
                                {event.id}
                              </TableCell>
                              <TableCell className="text-xs sm:text-sm">
                                {getSourceLabel(event.source)}
                              </TableCell>
                              <TableCell>
                                <Badge variant="destructive">{event.retryCount}</Badge>
                              </TableCell>
                              <TableCell className="text-xs sm:text-sm hidden md:table-cell">
                                {formatDate(event.receivedAt)}
                              </TableCell>
                              <TableCell>
                                <TruncatedCell
                                  text={event.errorMessage || 'Erro desconhecido'}
                                  maxLength={50}
                                  title="Mensagem de erro completa"
                                  className="text-rose-600"
                                />
                              </TableCell>
                            </TableRow>
                          ))
                        )}
                      </TableBody>
                    </Table>
                  </ScrollArea>
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="stats" className="space-y-4">
            <Card>
              <CardHeader>
                <CardTitle>Eventos por Tipo (24h)</CardTitle>
                <CardDescription>Distribuição de eventos processados</CardDescription>
              </CardHeader>
              <CardContent>
                {stats && stats.eventsByType.length > 0 ? (
                  <div className="space-y-4">
                    {stats.eventsByType.map((item) => (
                      <div key={item.source} className="flex items-center justify-between">
                        <span className="text-sm">{item.source}</span>
                        <div className="flex items-center gap-2">
                          <div className="h-2 w-32 rounded-full bg-muted">
                            <div
                              className="h-2 rounded-full bg-primary"
                              style={{
                                width: `${(item.count / stats.processedLast24h) * 100}%`,
                              }}
                            />
                          </div>
                          <span className="text-sm font-medium">{item.count}</span>
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <p className="text-center text-muted-foreground">Nenhum dado disponível</p>
                )}
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}
