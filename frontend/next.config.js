/** @type {import('next').NextConfig} */
const nextConfig = {
  /* config options here */
  // Desabilitar verificações de arquivos .env em produção
  env: {
    // Variáveis de ambiente serão carregadas apenas via process.env
    // Não tentar carregar arquivos .env automaticamente
  },
  // Suprimir avisos de arquivos .env não encontrados
  onDemandEntries: {
    maxInactiveAge: 25 * 1000,
    pagesBufferLength: 2,
  },
  // Configurações de produção
  productionBrowserSourceMaps: false,
  compress: true,
};

module.exports = nextConfig;

