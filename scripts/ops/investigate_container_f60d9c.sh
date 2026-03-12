#!/bin/bash
# Investigar o container específico que está executando o malware

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

CONTAINER_ID="f60d9c057dc7545806801fb6ede613a9279f34a658c84cb9e29c1139d97164e4"
CONTAINER_SHORT="f60d9c057dc7"

echo "=========================================="
echo "INVESTIGAÇÃO DO CONTAINER COMPROMETIDO"
echo "=========================================="
echo ""

# 1. Verificar se o container ainda existe
echo "1. Verificando container..."
docker ps -a | grep $CONTAINER_SHORT || echo "Container não encontrado na lista"
echo ""

# 2. Tentar identificar o container pelo ID completo
echo "2. Procurando container pelo ID completo..."
docker inspect $CONTAINER_ID 2>/dev/null | grep -E "(Name|Image|Status|Created)" || echo "Container não encontrado"
echo ""

# 3. Listar todos os containers
echo "3. Listando todos os containers (rodando e parados)..."
docker ps -a
echo ""

# 4. Verificar processos dentro do container (se ainda existir)
echo "4. Verificando processos dentro do container..."
if docker ps -a | grep -q $CONTAINER_SHORT; then
    CONTAINER_NAME=$(docker ps -a | grep $CONTAINER_SHORT | awk '{print $NF}')
    echo "Nome do container: $CONTAINER_NAME"
    echo ""
    
    echo "Todos os processos no container:"
    docker exec $CONTAINER_NAME ps aux 2>/dev/null || echo "Container não está rodando ou não pode ser acessado"
    echo ""
    
    echo "Processo node start.js:"
    docker exec $CONTAINER_NAME ps aux 2>/dev/null | grep "node start.js" | grep -v grep || echo "Não encontrado"
    echo ""
    
    echo "Processos filhos do node:"
    NODE_PID=$(docker exec $CONTAINER_NAME ps aux 2>/dev/null | grep "node start.js" | grep -v grep | awk '{print $2}' || echo "")
    if [ -n "$NODE_PID" ]; then
        echo "PID do node: $NODE_PID"
        docker exec $CONTAINER_NAME ps aux 2>/dev/null | awk -v ppid="$NODE_PID" '$3 == ppid' || echo "Nenhum processo filho encontrado"
    fi
    echo ""
    
    echo "Arquivo start.js no container:"
    docker exec $CONTAINER_NAME cat /app/start.js 2>/dev/null || docker exec $CONTAINER_NAME cat start.js 2>/dev/null || echo "Arquivo não encontrado"
    echo ""
    
    echo "Arquivos na raiz do container:"
    docker exec $CONTAINER_NAME ls -la / 2>/dev/null | head -20 || echo "Não acessível"
    echo ""
    
    echo "Arquivos em /app:"
    docker exec $CONTAINER_NAME ls -la /app 2>/dev/null | head -30 || echo "Não acessível"
    echo ""
    
    echo "Arquivos suspeitos (nomes aleatórios):"
    docker exec $CONTAINER_NAME find / -maxdepth 2 -type f -name "*[a-zA-Z0-9]{8,}*" 2>/dev/null | head -20 || echo "Nenhum encontrado"
    echo ""
    
else
    echo "⚠️  Container não existe mais, mas o processo ainda está rodando!"
    echo "Isso indica que o container foi removido mas o processo ficou órfão."
    echo ""
    echo "Processos órfãos relacionados:"
    ps aux | grep $CONTAINER_SHORT | grep -v grep || echo "Nenhum encontrado"
    echo ""
fi

# 5. Verificar qual imagem foi usada para criar esse container
echo "5. Verificando imagem do container..."
if docker ps -a | grep -q $CONTAINER_SHORT; then
    docker inspect $CONTAINER_SHORT 2>/dev/null | grep -E "(Image|Config)" | head -10 || echo "Não acessível"
else
    echo "Container não existe, não é possível verificar a imagem"
fi
echo ""

# 6. Verificar processos maliciosos no host relacionados a esse container
echo "6. Verificando processos maliciosos no host relacionados..."
ps aux | grep -E "(FdJ7qB|J6RtzFm|33zdXf1Z|QmjPwS5N)" | grep -v grep | grep -v "defunct" || echo "Nenhum processo malicioso ativo"
echo ""

# 7. Verificar processos filhos do PID 9022
echo "7. Verificando processos filhos do PID 9022 (node start.js)..."
if [ -d "/proc/9022" ]; then
    echo "Processos filhos:"
    ps --ppid 9022 -o pid,cmd 2>/dev/null || echo "Nenhum processo filho encontrado ou processo não existe mais"
else
    echo "Processo 9022 não existe mais"
fi
echo ""

echo "=========================================="
echo "INVESTIGAÇÃO CONCLUÍDA"
echo "=========================================="
echo ""
echo "Próximos passos:"
echo "1. Se o container ainda existe, pare e remova: docker stop $CONTAINER_SHORT && docker rm $CONTAINER_SHORT"
echo "2. Se o processo está órfão, mate-o: kill -9 9022"
echo "3. Reconstrua todos os containers: ./scripts/ops/fix_frontend_container.sh"
