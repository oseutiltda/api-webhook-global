#!/bin/bash
# Parar container frontend comprometido e remover arquivos maliciosos

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

set -e

echo "=========================================="
echo "PARADA DE EMERGÊNCIA - CONTAINER FRONTEND"
echo "=========================================="
echo ""

# 1. Matar processo node start.js órfão
echo "1. Matando processo node start.js (PID 9022)..."
if [ -d "/proc/9022" ]; then
    kill -9 9022 2>/dev/null || true
    echo "✓ Processo 9022 morto"
    sleep 1
else
    echo "Processo 9022 não existe mais"
fi
echo ""

# 2. Matar todos os processos maliciosos
echo "2. Matando processos maliciosos..."
ps aux | grep -E "(33zdXf1Z|QmjPwS5N|FdJ7qB|J6RtzFm)" | grep -v grep | grep -v "defunct" | awk '{print $2}' | xargs -r kill -9 2>/dev/null || true
sleep 1
echo ""

# 3. Parar container frontend
echo "3. Parando container bmx-frontend..."
docker stop bmx-frontend 2>/dev/null || echo "Container já está parado"
echo ""

# 4. Tentar remover arquivos maliciosos do container (antes de remover)
echo "4. Tentando remover arquivos maliciosos do container..."
docker exec bmx-frontend rm -f /33zdXf1Z /3auLc /QmjPwS5N 2>/dev/null || echo "Container não está acessível ou arquivos já foram removidos"
echo ""

# 5. Remover container
echo "5. Removendo container bmx-frontend..."
docker rm -f bmx-frontend 2>/dev/null || echo "Container já foi removido"
echo ""

# 6. Remover imagem comprometida
echo "6. Removendo imagem comprometida..."
docker rmi api-webhook-bmx-frontend 2>/dev/null || echo "Imagem já foi removida"
echo ""

# 7. Verificar processos maliciosos restantes
echo "7. Verificando processos maliciosos restantes..."
REMAINING=$(ps aux | grep -E "(33zdXf1Z|QmjPwS5N|FdJ7qB|J6RtzFm)" | grep -v grep | grep -v "defunct" | wc -l)
if [ "$REMAINING" -gt 0 ]; then
    echo "⚠️  Ainda há processos maliciosos:"
    ps aux | grep -E "(33zdXf1Z|QmjPwS5N|FdJ7qB|J6RtzFm)" | grep -v grep | grep -v "defunct"
else
    echo "✓ Nenhum processo malicioso encontrado"
fi
echo ""

echo "=========================================="
echo "CONTAINER FRONTEND PARADO E REMOVIDO"
echo "=========================================="
echo ""
echo "Próximos passos:"
echo "1. Execute: ./scripts/ops/complete_cleanup.sh para reconstruir tudo do zero"
echo "2. OU execute: ./scripts/ops/fix_frontend_container.sh para reconstruir apenas o frontend"
