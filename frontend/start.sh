#!/bin/sh
# Script de inicialização que silencia erros de verificação de arquivos .env

# Filtrar erros de verificação de .env e iniciar o Next.js
node_modules/.bin/next start 2>&1 | grep -v -E "(Command failed: test -f|/proc/.*/environ|\.env)" || node_modules/.bin/next start

