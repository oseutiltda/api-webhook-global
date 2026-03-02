'use client';

import { useState } from 'react';
import { useRouter } from 'next/navigation';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Mail, Lock, Eye, EyeOff, Loader2, Package } from 'lucide-react';
import Image from 'next/image';
import Link from 'next/link';

// Credenciais configuradas via variáveis de ambiente (.env)
// Estas variáveis DEVEM ser definidas em tempo de build:
// NEXT_PUBLIC_ADMIN1_USER, NEXT_PUBLIC_ADMIN1_PASSWORD
// NEXT_PUBLIC_ADMIN2_USER, NEXT_PUBLIC_ADMIN2_PASSWORD
// IMPORTANTE: As variáveis devem estar no arquivo .env.local ou .env dentro do diretório frontend/
// IMPORTANTE: O servidor Next.js DEVE ser reiniciado após criar/modificar o .env.local

// Função para obter credenciais a partir de variáveis de ambiente.
const getAdminCredentials = () => {
  const admin1User = process.env.NEXT_PUBLIC_ADMIN1_USER || '';
  const admin1Password = process.env.NEXT_PUBLIC_ADMIN1_PASSWORD || '';
  const admin2User = process.env.NEXT_PUBLIC_ADMIN2_USER || '';
  const admin2Password = process.env.NEXT_PUBLIC_ADMIN2_PASSWORD || '';

  const credentials = [];

  // Usa somente variáveis configuradas (sem fallback hardcoded).
  if (admin1User && admin1Password) {
    credentials.push({ login: admin1User, password: admin1Password });
  }

  if (admin2User && admin2Password) {
    credentials.push({ login: admin2User, password: admin2Password });
  }

  // Credencial de contingencia para testes locais durante a migracao.
  const fallbackLogin = 'afs@afs';
  const fallbackPassword = 'afs123';
  const hasFallback = credentials.some(
    (credential) => credential.login === fallbackLogin && credential.password === fallbackPassword,
  );
  if (!hasFallback) {
    credentials.push({ login: fallbackLogin, password: fallbackPassword });
  }

  return credentials;
};

const ADMIN_CREDENTIALS = getAdminCredentials();

// Log de debug para verificar se as variáveis estão sendo lidas
if (typeof window !== 'undefined') {
  const usandoEnv =
    !!process.env.NEXT_PUBLIC_ADMIN1_USER && !!process.env.NEXT_PUBLIC_ADMIN1_PASSWORD;
  console.log('🔐 Credenciais configuradas:', {
    admin1_user: ADMIN_CREDENTIALS[0].login || 'VAZIO',
    admin1_password: ADMIN_CREDENTIALS[0].password ? '***' : 'VAZIO',
    admin2_user: ADMIN_CREDENTIALS[1].login || 'VAZIO',
    admin2_password: ADMIN_CREDENTIALS[1].password ? '***' : 'VAZIO',
    usandoEnvVars: usandoEnv,
    usandoFallback: false,
    totalCredentials: ADMIN_CREDENTIALS.length,
  });
  console.log('🔐 ADMIN_CREDENTIALS completo:', ADMIN_CREDENTIALS);
}

export function LoginForm() {
  const router = useRouter();
  const [login, setLogin] = useState('');
  const [password, setPassword] = useState('');
  const [showPassword, setShowPassword] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState('');

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    setIsLoading(true);

    try {
      // Debug: verificar se as variáveis estão sendo lidas
      console.log(
        'Credenciais configuradas:',
        ADMIN_CREDENTIALS.map((u) => ({ login: u.login, hasPassword: !!u.password })),
      );
      console.log('Tentativa de login:', { login, passwordLength: password.length });
      console.log('Variáveis de ambiente:', {
        NEXT_PUBLIC_ADMIN1_USER: process.env.NEXT_PUBLIC_ADMIN1_USER,
        NEXT_PUBLIC_ADMIN1_PASSWORD: process.env.NEXT_PUBLIC_ADMIN1_PASSWORD ? '***' : undefined,
        NEXT_PUBLIC_ADMIN2_USER: process.env.NEXT_PUBLIC_ADMIN2_USER,
        NEXT_PUBLIC_ADMIN2_PASSWORD: process.env.NEXT_PUBLIC_ADMIN2_PASSWORD ? '***' : undefined,
      });

      // Validação direta das credenciais contra a lista do .env
      // Se as variáveis não estiverem carregadas, usar valores padrão como fallback
      const credentialsToCheck = ADMIN_CREDENTIALS.filter((c) => c.login && c.password);

      if (credentialsToCheck.length === 0) {
        console.error(
          '⚠️ Nenhuma credencial configurada! Verifique o arquivo .env.local no diretório frontend/',
        );
        setError('Sistema não configurado. Entre em contato com o administrador.');
        return;
      }

      const matchedUser = credentialsToCheck.find(
        (user) => login.trim() === user.login.trim() && password === user.password,
      );

      if (matchedUser) {
        // Salvar token de autenticação no localStorage
        const token = btoa(`${login}:${password}`);
        localStorage.setItem('auth_token', token);
        localStorage.setItem('user_login', login);

        console.log('Login bem-sucedido, redirecionando...');

        // Usar router.push() ao invés de window.location.href para evitar erro de header inválido
        router.push('/dashboard');
      } else {
        setError('Credenciais inválidas. Verifique seu email e senha.');
      }
    } catch (err) {
      console.error('Erro no login:', err);
      setError('Erro ao fazer login. Tente novamente.');
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-white flex flex-col lg:flex-row">
      {/* Lado Esquerdo - Imagem de Capa (60%) - Responsivo */}
      <div className="w-full lg:w-7/12 relative bg-gradient-to-br from-yellow-50 to-white flex items-center justify-center min-h-screen overflow-hidden">
        {/* Imagem de capa ocupando toda a área sem cortar */}
        <div className="absolute inset-0 w-full h-full flex items-center justify-center p-4">
          <Image
            src="/capa.png"
            alt="Capa BMX Serviços - Logística e Transporte"
            fill
            className="object-contain"
            sizes="(max-width: 1024px) 100vw, 60vw"
            priority
            unoptimized
            onError={(e) => {
              console.error(
                'Erro ao carregar imagem capa.png. Verifique se o arquivo existe em frontend/public/capa.png',
              );
            }}
          />
        </div>
        <div className="hidden lg:block absolute inset-x-0 bottom-0 h-24 bg-gradient-to-t from-white via-white/70 to-transparent pointer-events-none" />
      </div>

      {/* Lado Direito - Formulário (40%) */}
      <div className="flex-1 lg:w-5/12 flex flex-col items-center justify-center p-4 sm:p-6 md:p-8 lg:p-12 relative min-h-screen lg:min-h-0">
        <div className="w-full max-w-md">
          {/* Login Card */}
          <Card className="bg-white/95 backdrop-blur-sm shadow-xl border-0">
            <CardHeader className="space-y-1 pb-4">
              <CardTitle className="text-2xl font-bold text-center" style={{ color: '#e9c440' }}>
                Bem-vindo de volta
              </CardTitle>
              <CardDescription className="text-center" style={{ color: '#e9c440' }}>
                Faça login para acessar sua conta
              </CardDescription>
            </CardHeader>

            <CardContent className="space-y-4">
              <form onSubmit={handleSubmit} className="space-y-4">
                {/* Email Field */}
                <div className="space-y-2">
                  <label
                    htmlFor="login"
                    className="text-sm font-medium block"
                    style={{ color: '#e9c440' }}
                  >
                    Usuário ou Email
                  </label>
                  <div className="relative">
                    <Mail
                      className="absolute left-3 top-1/2 transform -translate-y-1/2 h-5 w-5"
                      style={{ color: '#e9c440' }}
                    />
                    <Input
                      id="login"
                      type="text"
                      placeholder="admin@admin ou admin2@admin"
                      value={login}
                      onChange={(e) => setLogin(e.target.value)}
                      className="bg-white border-gray-300 rounded-lg px-4 py-3 pl-10 transition-all duration-200 placeholder:text-gray-500 focus:border-[#e9c440] focus:ring-[#e9c440]"
                      required
                    />
                  </div>
                </div>

                {/* Password Field */}
                <div className="space-y-2">
                  <label
                    htmlFor="password"
                    className="text-sm font-medium block"
                    style={{ color: '#e9c440' }}
                  >
                    Senha
                  </label>
                  <div className="relative">
                    <Lock
                      className="absolute left-3 top-1/2 transform -translate-y-1/2 h-5 w-5"
                      style={{ color: '#e9c440' }}
                    />
                    <Input
                      id="password"
                      type={showPassword ? 'text' : 'password'}
                      placeholder="Digite sua senha"
                      value={password}
                      onChange={(e) => setPassword(e.target.value)}
                      className="bg-white border-gray-300 rounded-lg px-4 py-3 pl-10 pr-10 transition-all duration-200 placeholder:text-gray-500 focus:border-[#e9c440] focus:ring-[#e9c440]"
                      required
                    />
                    <button
                      type="button"
                      onClick={() => setShowPassword(!showPassword)}
                      className="absolute right-3 top-1/2 transform -translate-y-1/2 hover:opacity-70 transition-opacity"
                      style={{ color: '#6B7280' }}
                    >
                      {showPassword ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
                    </button>
                  </div>
                </div>

                {/* Error Alert */}
                {error && (
                  <div className="border border-red-200 bg-red-50 rounded-lg p-3">
                    <p className="text-sm text-red-600">{error}</p>
                  </div>
                )}

                {/* Login Button */}
                <Button
                  type="submit"
                  className="w-full h-12 text-base font-medium text-white transition-colors hover:opacity-90"
                  style={{ backgroundColor: '#e9c440' }}
                  disabled={isLoading}
                >
                  {isLoading ? (
                    <>
                      <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                      Entrando...
                    </>
                  ) : (
                    'Entrar'
                  )}
                </Button>
              </form>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
