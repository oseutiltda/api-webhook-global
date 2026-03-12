'use client';

import { useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';
import { BarChart3, Activity, TrendingUp, ArrowRight, Zap, Shield, Clock } from 'lucide-react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import Image from 'next/image';

interface DashboardCard {
  id: string;
  title: string;
  description: string;
  icon: React.ReactNode;
  route: string;
  iconWrap: string;
  accent: string;
  stats?: string;
}

export default function DashboardSelection() {
  const router = useRouter();
  const [mounted, setMounted] = useState(false);
  const [userLogin, setUserLogin] = useState<string | null>(null);

  useEffect(() => {
    // Verificar autenticação
    const authToken = localStorage.getItem('auth_token');
    const login = localStorage.getItem('user_login');

    if (!authToken) {
      router.push('/login');
      return;
    }

    setUserLogin(login);
    setMounted(true);
  }, [router]);

  const dashboards: DashboardCard[] = [
    {
      id: 'main',
      title: 'Dashboard Principal',
      description:
        'Visão geral completa do sistema, estatísticas, eventos e monitoramento em tempo real',
      icon: <BarChart3 className="w-8 h-8" />,
      route: '/',
      iconWrap: 'surface-brand text-[var(--brand-primary)]',
      accent: 'bg-[linear-gradient(135deg,rgba(30,47,91,0.18),rgba(63,93,151,0.06))]',
      stats: 'Visão Geral',
    },
    {
      id: 'worker',
      title: 'Worker Dashboard',
      description:
        'Monitoramento detalhado dos workers, processamento de eventos e status de integração',
      icon: <Activity className="w-8 h-8" />,
      route: '/worker',
      iconWrap: 'surface-info text-[var(--brand-info)]',
      accent: 'bg-[linear-gradient(135deg,rgba(63,93,151,0.18),rgba(255,255,255,0.02))]',
      stats: 'Processamento',
    },
    {
      id: 'analytics',
      title: 'Worker Analytics',
      description: 'Análises avançadas, métricas de produtividade e relatórios de performance',
      icon: <TrendingUp className="w-8 h-8" />,
      route: '/worker1',
      iconWrap: 'surface-warning text-[var(--brand-warning)]',
      accent: 'bg-[linear-gradient(135deg,rgba(238,49,36,0.16),rgba(255,255,255,0.02))]',
      stats: 'Analytics',
    },
  ];

  const handleLogout = () => {
    localStorage.removeItem('auth_token');
    localStorage.removeItem('user_login');
    router.push('/login');
  };

  if (!mounted) {
    return (
      <div className="page-shell flex items-center justify-center">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-[var(--brand-primary)]"></div>
      </div>
    );
  }

  return (
    <div className="page-shell !pb-0">
      {/* Header */}
      <header className="sticky top-0 z-50 border-b border-[rgba(30,47,91,0.08)] bg-white/88 backdrop-blur-md">
        <div className="container mx-auto px-4 sm:px-6 lg:px-8 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <div className="relative h-11 w-11 rounded-2xl border border-[rgba(30,47,91,0.1)] bg-white p-2 shadow-[0_10px_24px_rgba(30,47,91,0.08)]">
                <Image
                  src="/logo-global-cima.png"
                  alt="Global"
                  fill
                  className="object-contain p-2"
                  priority
                />
              </div>
              <div>
                <h1 className="text-xl font-bold text-[var(--brand-primary)]">Global Integrador</h1>
                <p className="text-sm text-muted-foreground">Painel operacional de integrações</p>
              </div>
            </div>
            <div className="flex items-center gap-4">
              <div className="text-right">
                <p className="text-sm font-medium text-foreground">{userLogin || 'Usuário'}</p>
                <p className="text-xs text-muted-foreground">Conectado</p>
              </div>
              <Button onClick={handleLogout} variant="outline">
                Sair
              </Button>
            </div>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="container mx-auto px-4 sm:px-6 lg:px-8 py-12">
        {/* Welcome Section */}
        <div className="page-header mb-12 text-center">
          <Badge variant="default" className="mb-4">
            Ambiente operacional
          </Badge>
          <h2 className="mb-4 text-4xl font-bold tracking-tight text-[var(--brand-primary)]">
            Bem-vindo ao sistema
          </h2>
          <p className="mx-auto max-w-2xl text-base text-muted-foreground">
            Selecione um dashboard para acompanhar integrações, filas, produtividade e alertas com a
            mesma linguagem visual em todo o ambiente.
          </p>
        </div>

        {/* Dashboard Cards Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 max-w-7xl mx-auto">
          {dashboards.map((dashboard, index) => (
            <Card
              key={dashboard.id}
              className="group relative cursor-pointer overflow-hidden border border-[rgba(30,47,91,0.08)] bg-white/92 transition-all duration-300 hover:-translate-y-1 hover:border-[rgba(30,47,91,0.18)] hover:shadow-[0_24px_52px_rgba(30,47,91,0.08)]"
              onClick={() => router.push(dashboard.route)}
            >
              <div
                className={`absolute inset-0 opacity-0 transition-opacity duration-300 group-hover:opacity-100 ${dashboard.accent}`}
              />

              <CardHeader className="relative z-10">
                <div className="flex items-start justify-between mb-4">
                  <div
                    className={`rounded-2xl border p-3 shadow-sm transition-all duration-300 group-hover:scale-[1.03] ${dashboard.iconWrap}`}
                  >
                    {dashboard.icon}
                  </div>
                  <ArrowRight className="w-5 h-5 text-muted-foreground group-hover:text-[var(--brand-accent)] group-hover:translate-x-1 transition-all duration-300" />
                </div>
                <CardTitle className="text-2xl font-bold text-[var(--brand-primary)] transition-colors duration-300">
                  {dashboard.title}
                </CardTitle>
                <CardDescription className="mt-2 text-muted-foreground">
                  {dashboard.description}
                </CardDescription>
              </CardHeader>

              <CardContent className="relative z-10">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2 text-sm text-muted-foreground">
                    <Clock className="w-4 h-4" />
                    <span>Tempo real</span>
                  </div>
                  {dashboard.stats && (
                    <Badge
                      variant="default"
                      className="bg-[var(--brand-accent)] text-white hover:bg-[var(--brand-accent)]"
                    >
                      {dashboard.stats}
                    </Badge>
                  )}
                </div>
              </CardContent>
            </Card>
          ))}
        </div>

        {/* Quick Stats */}
        <div className="mt-12 grid grid-cols-1 md:grid-cols-3 gap-6 max-w-7xl mx-auto">
          <Card className="surface-brand transition-shadow duration-300">
            <CardContent className="pt-6">
              <div className="flex items-center gap-3">
                <div className="surface-info rounded-xl border p-2">
                  <Zap className="w-5 h-5 text-[var(--brand-info)]" />
                </div>
                <div>
                  <p className="text-sm text-muted-foreground">Sistema</p>
                  <p className="text-lg font-bold text-[var(--brand-primary)]">Operacional</p>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card className="surface-brand transition-shadow duration-300">
            <CardContent className="pt-6">
              <div className="flex items-center gap-3">
                <div className="surface-success rounded-xl border p-2">
                  <Shield className="w-5 h-5 text-[var(--brand-success)]" />
                </div>
                <div>
                  <p className="text-sm text-muted-foreground">Status</p>
                  <p className="text-lg font-bold text-[var(--brand-primary)]">Online</p>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card className="surface-brand transition-shadow duration-300">
            <CardContent className="pt-6">
              <div className="flex items-center gap-3">
                <div className="surface-warning rounded-xl border p-2">
                  <Activity className="w-5 h-5 text-[var(--brand-warning)]" />
                </div>
                <div>
                  <p className="text-sm text-muted-foreground">Monitoramento</p>
                  <p className="text-lg font-bold text-[var(--brand-primary)]">Ativo</p>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>
      </main>
    </div>
  );
}
