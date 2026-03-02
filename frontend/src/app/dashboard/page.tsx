'use client'

import { useEffect, useState } from 'react'
import { useRouter } from 'next/navigation'
import { 
  BarChart3, 
  Activity, 
  TrendingUp, 
  ArrowRight,
  Zap,
  Shield,
  Clock
} from 'lucide-react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import Image from 'next/image'

interface DashboardCard {
  id: string
  title: string
  description: string
  icon: React.ReactNode
  route: string
  color: string
  gradient: string
  stats?: string
}

export default function DashboardSelection() {
  const router = useRouter()
  const [mounted, setMounted] = useState(false)
  const [userLogin, setUserLogin] = useState<string | null>(null)

  useEffect(() => {
    // Verificar autenticação
    const authToken = localStorage.getItem('auth_token')
    const login = localStorage.getItem('user_login')
    
    if (!authToken) {
      router.push('/login')
      return
    }
    
    setUserLogin(login)
    setMounted(true)
  }, [router])

  const dashboards: DashboardCard[] = [
    {
      id: 'main',
      title: 'Dashboard Principal',
      description: 'Visão geral completa do sistema, estatísticas, eventos e monitoramento em tempo real',
      icon: <BarChart3 className="w-8 h-8" />,
      route: '/',
      color: 'from-blue-500 to-cyan-500',
      gradient: 'bg-gradient-to-br from-blue-500 to-cyan-500',
      stats: 'Visão Geral'
    },
    {
      id: 'worker',
      title: 'Worker Dashboard',
      description: 'Monitoramento detalhado dos workers, processamento de eventos e status de integração',
      icon: <Activity className="w-8 h-8" />,
      route: '/worker',
      color: 'from-purple-500 to-pink-500',
      gradient: 'bg-gradient-to-br from-purple-500 to-pink-500',
      stats: 'Processamento'
    },
    {
      id: 'analytics',
      title: 'Worker Analytics',
      description: 'Análises avançadas, métricas de produtividade e relatórios de performance',
      icon: <TrendingUp className="w-8 h-8" />,
      route: '/worker1',
      color: 'from-orange-500 to-red-500',
      gradient: 'bg-gradient-to-br from-orange-500 to-red-500',
      stats: 'Analytics'
    }
  ]

  const handleLogout = () => {
    localStorage.removeItem('auth_token')
    localStorage.removeItem('user_login')
    router.push('/login')
  }

  if (!mounted) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-gray-50 to-gray-100 flex items-center justify-center">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-[#e9c440]"></div>
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-50 via-white to-gray-100">
      {/* Header */}
      <header className="bg-white/80 backdrop-blur-md border-b border-gray-200 shadow-sm sticky top-0 z-50">
        <div className="container mx-auto px-4 sm:px-6 lg:px-8 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <div className="relative w-10 h-10">
                <Image
                  src="/logo-bmx-cima.png"
                  alt="BMX Serviços"
                  fill
                  className="object-contain"
                  priority
                />
              </div>
              <div>
                <h1 className="text-xl font-bold text-gray-900">BMX Integrador</h1>
                <p className="text-sm text-gray-500">Sistema de Integração</p>
              </div>
            </div>
            <div className="flex items-center gap-4">
              <div className="text-right">
                <p className="text-sm font-medium text-gray-900">{userLogin || 'Usuário'}</p>
                <p className="text-xs text-gray-500">Conectado</p>
              </div>
              <Button
                onClick={handleLogout}
                variant="outline"
                className="border-gray-300 hover:bg-gray-50"
              >
                Sair
              </Button>
            </div>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="container mx-auto px-4 sm:px-6 lg:px-8 py-12">
        {/* Welcome Section */}
        <div className="text-center mb-12">
          <h2 className="text-4xl font-bold text-gray-900 mb-4">
            Bem-vindo ao Sistema
          </h2>
          <p className="text-lg text-gray-600 max-w-2xl mx-auto">
            Selecione um dashboard para visualizar informações detalhadas e monitorar o sistema
          </p>
        </div>

        {/* Dashboard Cards Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 max-w-7xl mx-auto">
          {dashboards.map((dashboard, index) => (
            <Card
              key={dashboard.id}
              className="group relative overflow-hidden border-2 border-gray-200 hover:border-[#e9c440] transition-all duration-300 hover:shadow-2xl cursor-pointer transform hover:-translate-y-2"
              onClick={() => router.push(dashboard.route)}
            >
              {/* Gradient Background Effect */}
              <div className={`absolute inset-0 ${dashboard.gradient} opacity-0 group-hover:opacity-5 transition-opacity duration-300`} />
              
              {/* Animated Border */}
              <div className={`absolute inset-0 ${dashboard.gradient} opacity-0 group-hover:opacity-20 blur-xl transition-opacity duration-300`} />
              
              <CardHeader className="relative z-10">
                <div className="flex items-start justify-between mb-4">
                  <div className={`p-3 rounded-xl ${dashboard.gradient} text-white shadow-lg transform group-hover:scale-110 group-hover:rotate-3 transition-all duration-300`}>
                    {dashboard.icon}
                  </div>
                  <ArrowRight className="w-5 h-5 text-gray-400 group-hover:text-[#e9c440] group-hover:translate-x-1 transition-all duration-300" />
                </div>
                <CardTitle className="text-2xl font-bold text-gray-900 group-hover:text-[#e9c440] transition-colors duration-300">
                  {dashboard.title}
                </CardTitle>
                <CardDescription className="text-gray-600 mt-2">
                  {dashboard.description}
                </CardDescription>
              </CardHeader>
              
              <CardContent className="relative z-10">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2 text-sm text-gray-500">
                    <Clock className="w-4 h-4" />
                    <span>Tempo real</span>
                  </div>
                  {dashboard.stats && (
                    <Badge variant="secondary" className="bg-gray-100 text-gray-700">
                      {dashboard.stats}
                    </Badge>
                  )}
                </div>
              </CardContent>

              {/* Hover Effect Overlay */}
              <div className="absolute inset-0 bg-gradient-to-br from-[#e9c440]/0 to-[#e9c440]/0 group-hover:from-[#e9c440]/5 group-hover:to-transparent transition-all duration-300 pointer-events-none" />
            </Card>
          ))}
        </div>

        {/* Quick Stats */}
        <div className="mt-12 grid grid-cols-1 md:grid-cols-3 gap-6 max-w-7xl mx-auto">
          <Card className="border-gray-200 hover:shadow-lg transition-shadow duration-300">
            <CardContent className="pt-6">
              <div className="flex items-center gap-3">
                <div className="p-2 bg-blue-100 rounded-lg">
                  <Zap className="w-5 h-5 text-blue-600" />
                </div>
                <div>
                  <p className="text-sm text-gray-600">Sistema</p>
                  <p className="text-lg font-bold text-gray-900">Operacional</p>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card className="border-gray-200 hover:shadow-lg transition-shadow duration-300">
            <CardContent className="pt-6">
              <div className="flex items-center gap-3">
                <div className="p-2 bg-green-100 rounded-lg">
                  <Shield className="w-5 h-5 text-green-600" />
                </div>
                <div>
                  <p className="text-sm text-gray-600">Status</p>
                  <p className="text-lg font-bold text-gray-900">Online</p>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card className="border-gray-200 hover:shadow-lg transition-shadow duration-300">
            <CardContent className="pt-6">
              <div className="flex items-center gap-3">
                <div className="p-2 bg-purple-100 rounded-lg">
                  <Activity className="w-5 h-5 text-purple-600" />
                </div>
                <div>
                  <p className="text-sm text-gray-600">Monitoramento</p>
                  <p className="text-lg font-bold text-gray-900">Ativo</p>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>
      </main>
    </div>
  )
}

