#!/bin/bash
# Investigar processos maliciosos que foram mortos (via /proc)

echo "=========================================="
echo "INVESTIGAÇÃO DE PROCESSOS MALICIOSOS MORTOS"
echo "=========================================="
echo ""

# PIDs conhecidos que foram mortos
PIDS="9235 9224 875 10205 10206"

for PID in $PIDS; do
    echo "=========================================="
    echo "INVESTIGANDO PID $PID"
    echo "=========================================="
    
    if [ -d "/proc/$PID" ]; then
        echo "✓ Informações ainda disponíveis em /proc/$PID"
        echo ""
        
        echo "1. Comando executado:"
        cat /proc/$PID/cmdline 2>/dev/null | tr '\0' ' ' || echo "Não acessível"
        echo ""
        echo ""
        
        echo "2. Arquivo executável:"
        readlink -f /proc/$PID/exe 2>/dev/null || echo "Não acessível"
        ls -la /proc/$PID/exe 2>/dev/null | head -1 || echo "Não acessível"
        echo ""
        
        echo "3. Processo pai (PPID):"
        PARENT_PID=$(cat /proc/$PID/stat 2>/dev/null | awk '{print $4}' || echo "")
        if [ -n "$PARENT_PID" ] && [ "$PARENT_PID" != "0" ]; then
            echo "PPID: $PARENT_PID"
            if [ -d "/proc/$PARENT_PID" ]; then
                echo "Comando do processo pai:"
                cat /proc/$PARENT_PID/cmdline 2>/dev/null | tr '\0' ' ' || echo "Não acessível"
                echo ""
                echo "Arquivo do processo pai:"
                readlink -f /proc/$PARENT_PID/exe 2>/dev/null || echo "Não acessível"
            else
                echo "Processo pai não existe mais"
            fi
        else
            echo "PPID não disponível ou é 0 (init)"
        fi
        echo ""
        
        echo "4. Container Docker?"
        cat /proc/$PID/cgroup 2>/dev/null | grep docker || echo "Não está em container Docker"
        echo ""
        
        echo "5. Diretório de trabalho:"
        readlink -f /proc/$PID/cwd 2>/dev/null || echo "Não acessível"
        echo ""
        
        echo "6. Variáveis de ambiente (primeiras 10):"
        cat /proc/$PID/environ 2>/dev/null | tr '\0' '\n' | head -10 || echo "Não acessível"
        echo ""
        
        echo "7. Quando foi iniciado:"
        START_TIME=$(stat -c %Y /proc/$PID 2>/dev/null || echo "")
        if [ -n "$START_TIME" ]; then
            date -d "@$START_TIME" 2>/dev/null || echo "Não disponível"
        fi
        echo ""
        
    else
        echo "✗ Processo não existe mais (informações em /proc já foram limpas)"
    fi
    
    echo ""
done

# Verificar processos maliciosos ativos agora
echo "=========================================="
echo "VERIFICANDO PROCESSOS MALICIOSOS ATIVOS"
echo "=========================================="
echo ""

ACTIVE_MALWARE=$(ps aux | grep -E "(FdJ7qB|J6RtzFm|qQeRFXxm|SF9tZc|GKZ|/[a-zA-Z0-9]{8,})" | grep -v grep | grep -v "systemd\|docker\|sshd\|containerd\|kthreadd\|rpcbind\|saslauthd\|rsyslogd\|gssproxy\|dbus\|nscd\|udev\|journald\|crond\|init\|node\|next" || true)

if [ -n "$ACTIVE_MALWARE" ]; then
    echo "⚠️  PROCESSOS MALICIOSOS AINDA ATIVOS:"
    echo "$ACTIVE_MALWARE"
else
    echo "✓ Nenhum processo malicioso ativo no momento"
fi
echo ""

# Verificar containers
echo "=========================================="
echo "VERIFICANDO CONTAINERS DOCKER"
echo "=========================================="
echo ""

for CONTAINER in $(docker ps --format "{{.Names}}" 2>/dev/null); do
    echo "Container: $CONTAINER"
    CONTAINER_MALWARE=$(docker exec $CONTAINER ps aux 2>/dev/null | grep -E "(FdJ7qB|J6RtzFm|qQeRFXxm|SF9tZc|GKZ|/[a-zA-Z0-9]{8,})" | grep -v grep | grep -v "PID\|ps aux\|node\|next" || true)
    
    if [ -n "$CONTAINER_MALWARE" ]; then
        echo "⚠️  MALWARE ENCONTRADO:"
        echo "$CONTAINER_MALWARE"
    else
        echo "✓ Nenhum malware encontrado"
    fi
    echo ""
done

echo "=========================================="
echo "INVESTIGAÇÃO CONCLUÍDA"
echo "=========================================="

