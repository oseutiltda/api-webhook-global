#!/bin/bash
# Script de investigação profunda para encontrar origem do malware

echo "=========================================="
echo "INVESTIGAÇÃO PROFUNDA - ORIGEM DO MALWARE"
echo "=========================================="
echo ""

# Encontrar PID do processo malicioso (procurar especificamente pelos nomes conhecidos)
MALWARE_PID=$(ps aux | grep -E "(SF9tZc|GKZ|qQeRFXxm|6a06VGn|ouhH6us|szMx3|x56A2NNve|jSKhrxE|QVibT|iyLoZ1PFq|v2dL5n|BXMvVzc)" | grep -v grep | grep -v "systemd\|docker\|sshd\|containerd\|kthreadd\|rpcbind\|saslauthd\|rsyslogd\|gssproxy\|dbus\|nscd\|udev\|journald\|crond\|init" | awk '{print $2}' | head -1)

if [ -z "$MALWARE_PID" ]; then
    echo "Nenhum processo malicioso encontrado no momento"
    echo "Procurando por processos com alto uso de CPU..."
    ps aux --sort=-%cpu | head -15
    exit 0
fi

echo "Processo malicioso encontrado: PID $MALWARE_PID"
echo ""

# 1. Informações do processo
echo "1. INFORMAÇÕES DO PROCESSO:"
ps aux | grep $MALWARE_PID | grep -v grep
echo ""

# 2. Arquivo executável real
echo "2. ARQUIVO EXECUTÁVEL REAL:"
if [ -L /proc/$MALWARE_PID/exe ] || [ -f /proc/$MALWARE_PID/exe ]; then
    echo "Link simbólico:"
    ls -la /proc/$MALWARE_PID/exe 2>/dev/null
    echo ""
    echo "Caminho real:"
    readlink -f /proc/$MALWARE_PID/exe 2>/dev/null
    REAL_PATH=$(readlink -f /proc/$MALWARE_PID/exe 2>/dev/null)
    echo ""
    if [ ! -z "$REAL_PATH" ] && [ -f "$REAL_PATH" ]; then
        echo "Informações do arquivo:"
        file "$REAL_PATH" 2>/dev/null
        ls -lah "$REAL_PATH" 2>/dev/null
        echo ""
        echo "Primeiros bytes (header):"
        head -c 100 "$REAL_PATH" 2>/dev/null | od -An -tx1 | head -5
    fi
else
    echo "Não foi possível acessar /proc/$MALWARE_PID/exe"
fi
echo ""

# 3. Processo pai (PPID)
echo "3. PROCESSO PAI (quem iniciou):"
PARENT_PID=$(ps -o ppid= -p $MALWARE_PID 2>/dev/null | tr -d ' ')
if [ ! -z "$PARENT_PID" ]; then
    echo "PPID: $PARENT_PID"
    ps aux | grep "^[^ ]* *$PARENT_PID " | grep -v grep
    echo ""
    echo "Comando do processo pai:"
    ps -p $PARENT_PID -o cmd= 2>/dev/null
    echo ""
    echo "Árvore de processos:"
    pstree -p $PARENT_PID 2>/dev/null || ps -ef | grep $PARENT_PID | grep -v grep
fi
echo ""

# 4. Diretório de trabalho
echo "4. DIRETÓRIO DE TRABALHO (cwd):"
ls -la /proc/$MALWARE_PID/cwd 2>/dev/null
readlink -f /proc/$MALWARE_PID/cwd 2>/dev/null
echo ""

# 5. Variáveis de ambiente
echo "5. VARIÁVEIS DE AMBIENTE (primeiras 20):"
cat /proc/$MALWARE_PID/environ 2>/dev/null | tr '\0' '\n' | head -20
echo ""

# 6. Argumentos da linha de comando
echo "6. ARGUMENTOS DA LINHA DE COMANDO:"
cat /proc/$MALWARE_PID/cmdline 2>/dev/null | tr '\0' ' ' | echo
echo ""
echo ""

# 7. Arquivos abertos
echo "7. ARQUIVOS ABERTOS (primeiros 20):"
lsof -p $MALWARE_PID 2>/dev/null | head -20
echo ""

# 8. Conexões de rede
echo "8. CONEXÕES DE REDE:"
netstat -tulpn 2>/dev/null | grep $MALWARE_PID
ss -tulpn 2>/dev/null | grep "pid=$MALWARE_PID"
echo ""

# 9. Verificar se está em container
echo "9. VERIFICANDO SE ESTÁ EM CONTAINER:"
CONTAINER_ID=$(cat /proc/$MALWARE_PID/cgroup 2>/dev/null | grep -oE 'docker/[a-f0-9]{64}' | head -1 | cut -d/ -f2)
if [ ! -z "$CONTAINER_ID" ]; then
    echo "⚠️  PROCESSO ESTÁ DENTRO DE UM CONTAINER DOCKER!"
    echo "Container ID: $CONTAINER_ID"
    echo ""
    echo "Informações do container:"
    docker ps -a --filter id=$CONTAINER_ID --format "table {{.ID}}\t{{.Names}}\t{{.Image}}\t{{.Status}}"
    echo ""
    echo "Nome do container:"
    docker ps -a --filter id=$CONTAINER_ID --format "{{.Names}}"
else
    echo "Processo está rodando no HOST (não em container)"
fi
echo ""

# 10. Verificar quando foi iniciado
echo "10. QUANDO FOI INICIADO:"
ps -o lstart= -p $MALWARE_PID 2>/dev/null
echo ""

# 11. Verificar arquivos relacionados
echo "11. PROCURANDO ARQUIVOS RELACIONADOS:"
if [ ! -z "$REAL_PATH" ]; then
    echo "Arquivo executável: $REAL_PATH"
    find / -samefile "$REAL_PATH" 2>/dev/null | head -10
fi
echo "Procurando por arquivos com nomes similares:"
find /tmp /var/tmp /opt /root -name "*SF9tZc*" -o -name "*GKZ*" 2>/dev/null | head -10
echo ""

# 12. Verificar histórico de comandos
echo "12. ÚLTIMOS COMANDOS EXECUTADOS (bash_history):"
tail -50 /root/.bash_history 2>/dev/null | grep -E "(wget|curl|bash|sh|chmod|/tmp|/var/tmp)" | tail -10
echo ""

# 13. Verificar processos relacionados
echo "13. PROCESSOS RELACIONADOS (mesmo PPID ou grupo):"
PGID=$(ps -o pgid= -p $MALWARE_PID 2>/dev/null | tr -d ' ')
if [ ! -z "$PGID" ]; then
    ps aux | awk -v pgid=$PGID '$5 == pgid {print $0}'
fi
echo ""

echo "=========================================="
echo "INVESTIGAÇÃO CONCLUÍDA"
echo "=========================================="
echo ""
echo "RESUMO:"
echo "- PID: $MALWARE_PID"
echo "- PPID: $PARENT_PID"
if [ ! -z "$CONTAINER_ID" ]; then
    echo "- Container: $CONTAINER_ID"
    echo "- ⚠️  ORIGEM: Container Docker"
else
    echo "- ⚠️  ORIGEM: Sistema Host"
fi
if [ ! -z "$REAL_PATH" ]; then
    echo "- Arquivo: $REAL_PATH"
fi
echo ""

