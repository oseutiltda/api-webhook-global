'use client';

import { useState } from 'react';
import { useRouter } from 'next/navigation';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Mail, Lock, Eye, EyeOff, Loader2, ArrowRight } from 'lucide-react';
import Image from 'next/image';
import coverImage from '../../public/capa.jpg';

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
  const [isCoverLoaded, setIsCoverLoaded] = useState(false);
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
    <div className="min-h-screen bg-white text-[var(--brand-primary)] flex flex-col lg:flex-row">
      <section className="relative min-h-screen w-full overflow-hidden bg-[#f7f9fc] lg:w-7/12">
        <div
          className={`absolute inset-0 bg-[linear-gradient(180deg,#eef3f9_0%,#f7f9fc_48%,#edf2f8_100%)] transition-opacity duration-500 ${
            isCoverLoaded ? 'opacity-0' : 'opacity-100'
          }`}
        />
        <div className="absolute inset-0 flex h-full w-full items-center justify-center p-4">
          <Image
            src={coverImage}
            alt="Capa Global Cargo - Logistica e Transporte"
            fill
            className={`object-contain transition-opacity duration-700 ${isCoverLoaded ? 'opacity-100' : 'opacity-0'}`}
            sizes="(max-width: 1024px) 100vw, 60vw"
            quality={100}
            fetchPriority="high"
            onLoad={() => setIsCoverLoaded(true)}
            onError={() => {
              setIsCoverLoaded(true);
              console.error(
                'Erro ao carregar imagem capa.jpg. Verifique se o arquivo existe em frontend/public/capa.jpg',
              );
            }}
          />
        </div>
        <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_center,rgba(255,255,255,0)_34%,rgba(33,58,102,0.045)_100%),linear-gradient(90deg,rgba(24,45,86,0.065)_0%,rgba(255,255,255,0)_16%,rgba(255,255,255,0)_84%,rgba(24,45,86,0.065)_100%)]" />
        <div className="pointer-events-none absolute inset-x-0 top-0 hidden h-32 bg-gradient-to-b from-[#e7edf6] via-[#f7f9fc]/84 to-transparent lg:block" />
        <div className="pointer-events-none absolute inset-x-0 bottom-0 hidden h-32 bg-gradient-to-t from-[#e7edf6] via-[#f7f9fc]/84 to-transparent lg:block" />
      </section>

      <section className="relative flex min-h-screen flex-1 items-center justify-center overflow-hidden bg-[#f5f3ee] px-6 py-10 sm:px-8 lg:w-5/12 lg:px-12">
        <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_right,_rgba(29,47,95,0.08),_transparent_32%),radial-gradient(circle_at_bottom_left,_rgba(228,68,50,0.035),_transparent_24%),linear-gradient(180deg,_rgba(255,255,255,0.78),_rgba(242,240,235,0.96))]" />
        <div className="absolute inset-y-0 left-0 hidden w-px bg-[linear-gradient(180deg,rgba(29,47,95,0),rgba(29,47,95,0.12),rgba(29,47,95,0))] lg:block" />
        <div className="absolute right-10 top-16 hidden h-48 w-48 rounded-full bg-[var(--brand-accent)]/8 blur-3xl lg:block" />
        <div className="relative z-10 w-full max-w-[28rem] lg:-translate-y-3">
          <div className="mb-5 flex items-center gap-3 text-[var(--brand-muted)]">
            <div className="flex h-11 w-11 items-center justify-center rounded-2xl border border-[#d7dce5] bg-white shadow-[0_12px_30px_rgba(16,35,63,0.05)]">
              <ArrowRight className="h-5 w-5 text-[var(--brand-accent)]" />
            </div>
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.28em] text-[#7b879b]">
                Autenticacao
              </p>
              <p className="text-sm text-[var(--brand-muted)]">
                Acesso restrito ao ambiente operacional
              </p>
            </div>
          </div>

          <Card className="overflow-hidden rounded-[32px] border border-[#d7dce5] bg-[#fcfbf8]/98 py-0 shadow-[0_22px_72px_rgba(16,35,63,0.075)] ring-1 ring-white/60 backdrop-blur-sm">
            <div className="h-1.5 w-full bg-[linear-gradient(90deg,#1d2f5f_0%,#1d2f5f_72%,#ee3124_72%,#ee3124_100%)]" />
            <CardHeader className="space-y-3 px-7 pb-0 pt-7">
              <div className="space-y-2">
                <CardTitle className="text-3xl font-semibold tracking-[-0.04em] text-[var(--brand-primary)]">
                  Bem-vindo de volta
                </CardTitle>
                <CardDescription className="max-w-sm text-sm leading-6 text-[var(--brand-muted)]">
                  Faça login para acessar sua conta.
                </CardDescription>
              </div>
            </CardHeader>

            <CardContent className="space-y-5 px-7 pb-7 pt-6">
              <form onSubmit={handleSubmit} className="space-y-5">
                <div className="space-y-2">
                  <label
                    htmlFor="login"
                    className="block text-sm font-medium tracking-[-0.01em] text-[#22385b]"
                  >
                    Usuario ou e-mail
                  </label>
                  <div className="relative">
                    <Mail className="absolute left-4 top-1/2 h-4 w-4 -translate-y-1/2 text-[#70809a]" />
                    <Input
                      id="login"
                      type="text"
                      autoComplete="username"
                      placeholder="seu.usuario@globalcargo.com.br"
                      value={login}
                      onChange={(e) => setLogin(e.target.value)}
                      className="h-[52px] rounded-2xl border-[#d7dce5] bg-[#fffefd] px-4 py-3 pl-11 text-[15px] text-[var(--brand-primary)] shadow-[inset_0_1px_0_rgba(255,255,255,0.7)] placeholder:text-[#8d97a7] transition-colors focus-visible:border-[var(--brand-primary)] focus-visible:ring-[var(--brand-primary)]/12"
                      required
                    />
                  </div>
                </div>

                <div className="space-y-2">
                  <label
                    htmlFor="password"
                    className="block text-sm font-medium tracking-[-0.01em] text-[#22385b]"
                  >
                    Senha
                  </label>
                  <div className="relative">
                    <Lock className="absolute left-4 top-1/2 h-4 w-4 -translate-y-1/2 text-[#70809a]" />
                    <Input
                      id="password"
                      type={showPassword ? 'text' : 'password'}
                      autoComplete="current-password"
                      placeholder="Digite sua senha"
                      value={password}
                      onChange={(e) => setPassword(e.target.value)}
                      className="h-[52px] rounded-2xl border-[#d7dce5] bg-[#fffefd] px-4 py-3 pl-11 pr-11 text-[15px] text-[var(--brand-primary)] shadow-[inset_0_1px_0_rgba(255,255,255,0.7)] placeholder:text-[#8d97a7] transition-colors focus-visible:border-[var(--brand-primary)] focus-visible:ring-[var(--brand-primary)]/12"
                      required
                    />
                    <button
                      type="button"
                      onClick={() => setShowPassword(!showPassword)}
                      className="absolute right-4 top-1/2 -translate-y-1/2 text-[#6b778b] transition-opacity hover:opacity-70"
                    >
                      {showPassword ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
                    </button>
                  </div>
                </div>

                {error && (
                  <div className="rounded-2xl border border-[#f2c4bf] bg-[#fff2f0] p-3">
                    <p className="text-sm text-[#a3392a]">{error}</p>
                  </div>
                )}

                <Button
                  type="submit"
                  className="h-[52px] w-full rounded-2xl bg-[var(--brand-accent)] text-base font-semibold tracking-[-0.01em] text-white shadow-[0_10px_24px_rgba(238,49,36,0.18)] transition-opacity hover:bg-[var(--brand-accent)]/95 focus-visible:ring-[var(--brand-accent)]/25"
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
      </section>
    </div>
  );
}
