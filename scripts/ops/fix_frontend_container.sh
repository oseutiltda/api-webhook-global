#!/bin/bash
# Script para investigar e corrigir o container frontend comprometido

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

set -e

echo "=========================================="
echo "CORREÇÃO DO CONTAINER FRONTEND"
echo "=========================================="
echo ""

# 1. Matar processos maliciosos no host primeiro
echo "1. Matando processos maliciosos no host..."
ps aux | grep -E "(qQeRFXxm|SF9tZc|GKZ|qQeRFXxm)" | grep -v grep | awk '{print $2}' | xargs -r kill -9 2>/dev/null || true
sleep 2
echo ""

# 2. Parar containers
echo "2. Parando containers..."
docker-compose down
echo ""

# 3. Remover containers e imagens comprometidos
echo "3. Removendo containers e imagens comprometidos..."
docker ps -a | grep -E "(bmx-frontend|8ff96b452d95)" | awk '{print $1}' | xargs -r docker rm -f 2>/dev/null || true
docker images | grep -E "(api-webhook-bmx-frontend|frontend)" | awk '{print $3}' | xargs -r docker rmi -f 2>/dev/null || true
echo ""

# 4. Limpar cache do Docker completamente
echo "4. Limpando cache do Docker..."
docker builder prune -af
docker system prune -af
echo ""

# 5. Verificar se há volumes comprometidos
echo "5. Verificando volumes Docker..."
docker volume ls | grep -E "(frontend|bmx)" || echo "Nenhum volume encontrado"
echo ""

# 6. Verificar Dockerfile do frontend
echo "6. Verificando Dockerfile do frontend..."
if [ -f "frontend/Dockerfile" ]; then
    echo "✓ Dockerfile encontrado"
    SUSPICIOUS=$(grep -iE "(wget|curl.*http|bash.*<|sh.*<|base64|eval|exec.*curl)" frontend/Dockerfile || true)
    if [ -n "$SUSPICIOUS" ]; then
        echo "⚠️  Comandos suspeitos encontrados:"
        echo "$SUSPICIOUS"
    else
        echo "✓ Nenhum comando suspeito encontrado"
    fi
else
    echo "✗ Dockerfile não encontrado"
fi
echo ""

# 7. Verificar arquivos no diretório frontend
echo "7. Verificando arquivos suspeitos no diretório frontend..."
find frontend -type f -name "*[a-zA-Z0-9]{6,}*" 2>/dev/null | head -10 || echo "Nenhum arquivo suspeito encontrado"
find frontend -type f -executable -name "*[a-zA-Z0-9]{6,}*" 2>/dev/null | head -10 || echo "Nenhum executável suspeito encontrado"
echo ""

# 8. Recriar imagem do frontend do zero (SEM CACHE)
echo "8. Recriando imagem do frontend do zero (SEM CACHE)..."
echo "Isso pode levar alguns minutos..."
docker-compose build --no-cache --pull frontend
echo ""

# 9. Subir apenas o frontend
echo "9. Subindo apenas o frontend..."
docker-compose up -d frontend
echo ""

# 10. Aguardar inicialização
echo "10. Aguardando 20 segundos para inicialização..."
sleep 20
echo ""

# 11. Verificar processos dentro do container
echo "11. Verificando processos dentro do container frontend..."
FRONTEND_CONTAINER=$(docker ps -q --filter name=bmx-frontend)
if [ -n "$FRONTEND_CONTAINER" ]; then
    echo "Container ID: $FRONTEND_CONTAINER"
    echo ""
    echo "--- Processos no container ---"
    docker exec $FRONTEND_CONTAINER ps aux 2>/dev/null || echo "Erro ao listar processos"
    echo ""
    echo "--- Arquivos na raiz do container ---"
    docker exec $FRONTEND_CONTAINER ls -la / 2>/dev/null | grep -E "(qQeRFXxm|SF9tZc|GKZ|[a-zA-Z0-9]{8,})" || echo "✓ Nenhum arquivo suspeito encontrado"
    echo ""
    echo "--- Verificando se há processos maliciosos ---"
    docker exec $FRONTEND_CONTAINER ps aux 2>/dev/null | grep -E "(qQeRFXxm|SF9tZc|GKZ)" | grep -v grep || echo "✓ Nenhum processo malicioso encontrado no container"
else
    echo "✗ Container frontend não está rodando"
fi
echo ""

# 12. Verificar processos maliciosos no host
echo "12. Verificando processos maliciosos no host..."
MALICIOUS=$(ps aux | grep -E "(qQeRFXxm|SF9tZc|GKZ)" | grep -v grep || true)
if [ -n "$MALICIOUS" ]; then
    echo "⚠️  PROCESSOS MALICIOSOS AINDA RODANDO NO HOST!"
    echo "$MALICIOUS"
else
    echo "✓ Nenhum processo malicioso encontrado no host"
fi
echo ""

# 13. Monitorar por 30 segundos
echo "13. Monitorando por 30 segundos para verificar se o malware retorna..."
for i in {1..6}; do
    sleep 5
    MALICIOUS=$(ps aux | grep -E "(qQeRFXxm|SF9tZc|GKZ)" | grep -v grep || true)
    if [ -n "$MALICIOUS" ]; then
        echo "⚠️  MALWARE RETORNOU após $((i*5)) segundos!"
        echo "$MALICIOUS"
        break
    else
        echo "  [$i/6] ✓ Nenhum malware detectado..."
    fi
done
echo ""

echo "=========================================="
echo "CORREÇÃO CONCLUÍDA"
echo "=========================================="
echo ""
echo "Próximos passos:"
echo "1. Se o malware retornou, investigue:"
echo "   - Volumes Docker montados"
echo "   - Scripts de inicialização"
echo "   - Dependências npm comprometidas"
echo "2. Se não retornou, monitore por alguns minutos"
echo "3. Considere adicionar monitoramento contínuo"
