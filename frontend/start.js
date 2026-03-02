#!/usr/bin/env node
/**
 * Script de inicialização que silencia erros de verificação de arquivos .env
 * Intercepta e filtra mensagens de erro relacionadas a arquivos .env não encontrados
 */

const { spawn } = require('child_process');
const path = require('path');

// Padrões de erro a serem filtrados
const errorPatterns = [
  /Command failed: test -f/,
  /\/proc\/.*\/environ/,
  /\.env/,
  /status: 1/,
  /signal: null/,
];

// Função para verificar se uma linha deve ser filtrada
function shouldFilterLine(line) {
  const lineStr = line.toString();
  return errorPatterns.some(pattern => pattern.test(lineStr));
}

// Iniciar o Next.js
const nextProcess = spawn('node', ['node_modules/.bin/next', 'start'], {
  cwd: __dirname,
  stdio: ['inherit', 'pipe', 'pipe'],
  env: {
    ...process.env,
    NODE_ENV: 'production',
  },
});

// Interceptar stdout
nextProcess.stdout.on('data', (data) => {
  const lines = data.toString().split('\n');
  lines.forEach(line => {
    if (line.trim() && !shouldFilterLine(line)) {
      process.stdout.write(line + '\n');
    }
  });
});

// Interceptar stderr
nextProcess.stderr.on('data', (data) => {
  const lines = data.toString().split('\n');
  lines.forEach(line => {
    if (line.trim() && !shouldFilterLine(line)) {
      process.stderr.write(line + '\n');
    }
  });
});

// Gerenciar saída do processo
nextProcess.on('close', (code) => {
  process.exit(code);
});

nextProcess.on('error', (error) => {
  console.error('Erro ao iniciar Next.js:', error);
  process.exit(1);
});

