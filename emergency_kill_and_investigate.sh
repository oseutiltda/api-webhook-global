#!/bin/bash
# Script de emergência para matar malware e investigar origem

set -e

echo "=========================================="
echo "AÇÃO DE EMERGÊNCIA - MATAR E INVESTIGAR"
echo "=========================================="
echo ""

# 1. Matar processos maliciosos
echo "1. Matando processos maliciosos..."
MALWARE_PIDS=$(ps aux | grep -E "(FdJ7qB|J6RtzFm|qQeRFXxm|SF9tZc|GKZ|/[a-zA-Z0-9]{6,})" | grep -v grep | grep -v "systemd\|docker\|sshd\|containerd\|kthreadd\|rpcbind\|saslauthd\|rsyslogd\|gssproxy\|dbus\|nscd\|udev\|journald\|crond\|init\|node\|next" | awk '{print $2}')

if [ -n "$MALWARE_PIDS" ]; then
    echo "Processos encontrados: $MALWARE_PIDS"
    for PID in $MALWARE_PIDS; do
        echo "  Matando PID $PID..."
        kill -9 $PID 2>/dev/null || true
    done
    sleep 2
    echo "✓ Processos mortos"
else
    echo "Nenhum processo malicioso encontrado"
fi
echo ""

# 2. Investigar cada processo malicioso
echo "2. Investigando origem dos processos..."
for PID in 9235 9224; do
    if ps -p $PID > /dev/null 2>&1; then
        echo "--- Investigando PID $PID ---"
        echo "Comando:"
        ps -p $PID -o cmd= 2>/dev/null || echo "Processo não encontrado"
        echo ""
        
        echo "Arquivo executável:"
        readlink -f /proc/$PID/exe 2>/dev/null || echo "Não acessível"
        ls -la /proc/$PID/exe 2>/dev/null || echo "Não acessível"
        echo ""
        
        echo "Processo pai (PPID):"
        PARENT_PID=$(ps -o ppid= -p $PID 2>/dev/null | tr -d ' ')
        if [ -n "$PARENT_PID" ]; then
            echo "PPID: $PARENT_PID"
            ps -p $PARENT_PID -o cmd= 2>/dev/null || echo "Processo pai não encontrado"
        fi
        echo ""
        
        echo "Está em container Docker?"
        cat /proc/$PID/cgroup 2>/dev/null | grep docker || echo "Não está em container"
        echo ""
        
        echo "Diretório de trabalho:"
        readlink -f /proc/$PID/cwd 2>/dev/null || echo "Não acessível"
        echo ""
        
        echo "Variáveis de ambiente (primeiras 5):"
        cat /proc/$PID/environ 2>/dev/null | tr '\0' '\n' | head -5 || echo "Não acessível"
        echo ""
        echo "---"
        echo ""
    fi
done

# 3. Verificar containers Docker
echo "3. Verificando processos dentro dos containers..."
for CONTAINER in $(docker ps --format "{{.Names}}"); do
    echo "Container: $CONTAINER"
    docker exec $CONTAINER ps aux 2>/dev/null | grep -E "(FdJ7qB|J6RtzFm|qQeRFXxm|SF9tZc|GKZ|/[a-zA-Z0-9]{6,})" | grep -v grep | grep -v "PID\|ps aux\|node\|next" || echo "  Nenhum processo malicioso encontrado"
    echo ""
done

# 4. Verificar arquivos maliciosos no sistema
echo "4. Procurando arquivos maliciosos no sistema..."
echo "Procurando na raiz (/):"
find / -maxdepth 1 -type f -name "*[a-zA-Z0-9]{6,}*" 2>/dev/null | head -10 || echo "Nenhum encontrado"
echo ""

echo "Procurando em /tmp:"
find /tmp -type f -name "*[a-zA-Z0-9]{6,}*" 2>/dev/null | head -10 || echo "Nenhum encontrado"
echo ""

# 5. Verificar crontabs
echo "5. Verificando crontabs..."
echo "Crontab do root:"
crontab -l 2>/dev/null | grep -v "^#" | grep -v "^$" || echo "Nenhum crontab encontrado"
echo ""

echo "Crontabs do sistema:"
cat /etc/crontab 2>/dev/null | grep -v "^#" | grep -v "^$" || echo "Nenhum encontrado"
echo ""

# 6. Verificar systemd services suspeitos
echo "6. Verificando systemd services..."
systemctl list-units --type=service --state=running | grep -E "(FdJ7qB|J6RtzFm|qQeRFXxm|SF9tZc)" || echo "Nenhum service suspeito encontrado"
echo ""

# 7. Verificar processos órfãos (defunct)
echo "7. Verificando processos defunct (zombies)..."
ps aux | grep "\[.*\] <defunct>" | head -10 || echo "Nenhum processo defunct encontrado"
echo ""

echo "=========================================="
echo "INVESTIGAÇÃO CONCLUÍDA"
echo "=========================================="
echo ""
echo "Próximos passos:"
echo "1. Se os processos estão em containers, execute: ./fix_frontend_container.sh"
echo "2. Se estão no host, verifique:"
echo "   - Crontabs"
echo "   - Systemd services"
echo "   - Scripts de inicialização"
echo "   - Arquivos em /tmp ou /"

