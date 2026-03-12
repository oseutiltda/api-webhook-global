#!/bin/bash
# Investigar o processo node start.js que está iniciando o malware

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

echo "=========================================="
echo "INVESTIGAÇÃO: node start.js (PID 9022)"
echo "=========================================="
echo ""

# 1. Matar novos processos maliciosos primeiro
echo "1. Matando novos processos maliciosos..."
kill -9 10253 10266 2>/dev/null || true
sleep 1
echo ""

# 2. Investigar PID 9022
echo "2. Investigando processo pai (PID 9022)..."
if [ -d "/proc/9022" ]; then
    echo "✓ Processo ainda existe"
    echo ""
    
    echo "Comando completo:"
    cat /proc/9022/cmdline 2>/dev/null | tr '\0' ' ' || echo "Não acessível"
    echo ""
    echo ""
    
    echo "Arquivo executável:"
    readlink -f /proc/9022/exe 2>/dev/null || echo "Não acessível"
    echo ""
    
    echo "Diretório de trabalho:"
    readlink -f /proc/9022/cwd 2>/dev/null || echo "Não acessível"
    echo ""
    
    echo "Está em container Docker?"
    cat /proc/9022/cgroup 2>/dev/null | grep docker || echo "Não está em container"
    echo ""
    
    echo "Container ID (se estiver em container):"
    cat /proc/9022/cgroup 2>/dev/null | grep docker | head -1 | cut -d'/' -f3 || echo "Não está em container"
    echo ""
    
    echo "Variáveis de ambiente (todas):"
    cat /proc/9022/environ 2>/dev/null | tr '\0' '\n' | sort || echo "Não acessível"
    echo ""
    
    echo "Processo pai do 9022:"
    PARENT_9022=$(cat /proc/9022/stat 2>/dev/null | awk '{print $4}' || echo "")
    if [ -n "$PARENT_9022" ] && [ "$PARENT_9022" != "0" ]; then
        echo "PPID: $PARENT_9022"
        if [ -d "/proc/$PARENT_9022" ]; then
            echo "Comando do processo pai:"
            cat /proc/$PARENT_9022/cmdline 2>/dev/null | tr '\0' ' ' || echo "Não acessível"
            echo ""
            echo "Arquivo do processo pai:"
            readlink -f /proc/$PARENT_9022/exe 2>/dev/null || echo "Não acessível"
        fi
    fi
    echo ""
else
    echo "✗ Processo 9022 não existe mais"
    echo "Procurando por outros processos 'node start.js'..."
    ps aux | grep "node start.js" | grep -v grep
fi
echo ""

# 3. Verificar qual container está executando node start.js
echo "3. Verificando containers Docker que executam 'node start.js'..."
echo ""

for CONTAINER in $(docker ps --format "{{.Names}}" 2>/dev/null); do
    echo "--- Container: $CONTAINER ---"
    
    # Verificar se há processo node start.js
    NODE_PROCESS=$(docker exec $CONTAINER ps aux 2>/dev/null | grep "node start.js" | grep -v grep || true)
    
    if [ -n "$NODE_PROCESS" ]; then
        echo "⚠️  PROCESSO 'node start.js' ENCONTRADO:"
        echo "$NODE_PROCESS"
        echo ""
        
        # Verificar arquivo start.js no container
        echo "Conteúdo do arquivo start.js:"
        docker exec $CONTAINER cat /app/start.js 2>/dev/null || docker exec $CONTAINER cat start.js 2>/dev/null || echo "Arquivo não encontrado"
        echo ""
        
        # Verificar processos filhos
        NODE_PID=$(echo "$NODE_PROCESS" | awk '{print $2}')
        echo "Processos filhos do node (PID $NODE_PID):"
        docker exec $CONTAINER ps aux 2>/dev/null | awk -v ppid="$NODE_PID" '$3 == ppid || $2 == ppid' || echo "Nenhum processo filho encontrado"
        echo ""
        
        # Verificar arquivos suspeitos na raiz do container
        echo "Arquivos na raiz do container (procurando malware):"
        docker exec $CONTAINER ls -la / 2>/dev/null | grep -E "^-.*[a-zA-Z0-9]{8,}" | head -10 || echo "Nenhum arquivo suspeito encontrado"
        echo ""
    else
        echo "✓ Nenhum processo 'node start.js' encontrado"
    fi
    echo ""
done

# 4. Verificar arquivo start.js no host (se existir)
echo "4. Verificando se há start.js no host..."
if [ -f "$PROJECT_ROOT/frontend/start.js" ]; then
    echo "Arquivo encontrado: $PROJECT_ROOT/frontend/start.js"
    echo "Primeiras 50 linhas:"
    head -50 "$PROJECT_ROOT/frontend/start.js"
    echo ""
    echo "Últimas 20 linhas:"
    tail -20 "$PROJECT_ROOT/frontend/start.js"
else
    echo "Arquivo não encontrado no host"
fi
echo ""

# 5. Verificar processos maliciosos ativos agora
echo "5. Verificando processos maliciosos ativos..."
ACTIVE=$(ps aux | grep -E "(33zdXf1Z|QmjPwS5N|FdJ7qB|J6RtzFm)" | grep -v grep | grep -v "defunct\|systemd\|docker\|sshd\|containerd\|kthreadd\|rpcbind\|saslauthd\|rsyslogd\|gssproxy\|dbus\|nscd\|udev\|journald\|crond\|init\|node\|next" || true)

if [ -n "$ACTIVE" ]; then
    echo "⚠️  PROCESSOS MALICIOSOS AINDA ATIVOS:"
    echo "$ACTIVE"
else
    echo "✓ Nenhum processo malicioso ativo"
fi
echo ""

echo "=========================================="
echo "INVESTIGAÇÃO CONCLUÍDA"
echo "=========================================="
