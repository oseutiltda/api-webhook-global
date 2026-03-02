#!/bin/bash
# Limpeza completa: matar processos órfãos e reconstruir containers

# Não parar em erros - continuar mesmo se alguns comandos falharem
set +e

echo "=========================================="
echo "LIMPEZA COMPLETA DO SISTEMA"
echo "=========================================="
echo ""

# 1. Matar todos os processos maliciosos
echo "1. Matando processos maliciosos..."
MALWARE_PIDS=$(ps aux 2>/dev/null | grep -E "(FdJ7qB|J6RtzFm|33zdXf1Z|QmjPwS5N|qQeRFXxm|SF9tZc|GKZ|/[a-zA-Z0-9]{8,})" | grep -v grep | grep -v "systemd\|docker\|sshd\|containerd\|kthreadd\|rpcbind\|saslauthd\|rsyslogd\|gssproxy\|dbus\|nscd\|udev\|journald\|crond\|init\|node\|next" | awk '{print $2}' || true)
if [ -n "$MALWARE_PIDS" ]; then
    echo "$MALWARE_PIDS" | xargs -r kill -9 2>/dev/null || true
    echo "Processos maliciosos mortos"
else
    echo "Nenhum processo malicioso encontrado"
fi
sleep 2
echo ""

# 2. Matar processo node start.js órfão (PID 9022)
echo "2. Matando processo órfão node start.js (PID 9022)..."
if [ -d "/proc/9022" ]; then
    kill -9 9022 2>/dev/null || true
    echo "✓ Processo 9022 morto"
else
    echo "Processo 9022 não existe mais"
fi
echo ""

# 3. Parar todos os containers
echo "3. Parando todos os containers..."
docker-compose down 2>/dev/null || echo "Erro ao parar containers (pode não existir docker-compose.yml)"
echo ""

# 4. Remover containers órfãos
echo "4. Removendo containers órfãos..."
ORPHAN_CONTAINERS=$(docker ps -a 2>/dev/null | grep -E "(f60d9c057dc7|Exited|Dead)" | awk '{print $1}' || true)
if [ -n "$ORPHAN_CONTAINERS" ]; then
    echo "$ORPHAN_CONTAINERS" | xargs -r docker rm -f 2>/dev/null || true
    echo "Containers órfãos removidos"
else
    echo "Nenhum container órfão encontrado"
fi
echo ""

# 5. Remover todas as imagens
echo "5. Removendo imagens Docker..."
IMAGES=$(docker images 2>/dev/null | grep -E "(api-webhook-bmx|frontend|backend|worker)" | awk '{print $3}' || true)
if [ -n "$IMAGES" ]; then
    echo "$IMAGES" | xargs -r docker rmi -f 2>/dev/null || true
    echo "Imagens removidas"
else
    echo "Nenhuma imagem encontrada"
fi
echo ""

# 6. Limpar cache do Docker
echo "6. Limpando cache do Docker..."
docker builder prune -af
docker system prune -af
echo ""

# 7. Verificar processos maliciosos restantes
echo "7. Verificando processos maliciosos restantes..."
REMAINING=$(ps aux | grep -E "(FdJ7qB|J6RtzFm|33zdXf1Z|QmjPwS5N)" | grep -v grep | grep -v "defunct" | wc -l)
if [ "$REMAINING" -gt 0 ]; then
    echo "⚠️  Ainda há processos maliciosos:"
    ps aux | grep -E "(FdJ7qB|J6RtzFm|33zdXf1Z|QmjPwS5N)" | grep -v grep | grep -v "defunct"
else
    echo "✓ Nenhum processo malicioso encontrado"
fi
echo ""

# 8. Reconstruir containers do zero
echo "8. Reconstruindo containers do zero (sem cache)..."
echo "Isso pode levar vários minutos..."
echo "Aguarde..."
docker-compose build --no-cache --pull 2>&1 | tee /tmp/docker-build.log || {
    echo "⚠️  Erro durante a reconstrução. Verificando logs..."
    tail -50 /tmp/docker-build.log 2>/dev/null || true
    echo "Continuando mesmo com erros..."
}
echo ""

# 9. Subir containers
echo "9. Subindo containers..."
docker-compose up -d
echo ""

# 10. Aguardar inicialização
echo "10. Aguardando 30 segundos para inicialização..."
sleep 30
echo ""

# 11. Verificar processos maliciosos
echo "11. Verificando processos maliciosos após reconstrução..."
MALWARE=$(ps aux | grep -E "(FdJ7qB|J6RtzFm|33zdXf1Z|QmjPwS5N|qQeRFXxm|SF9tZc)" | grep -v grep | grep -v "defunct\|systemd\|docker\|sshd\|containerd\|kthreadd\|rpcbind\|saslauthd\|rsyslogd\|gssproxy\|dbus\|nscd\|udev\|journald\|crond\|init\|node\|next" || true)

if [ -n "$MALWARE" ]; then
    echo "⚠️  MALWARE RETORNOU!"
    echo "$MALWARE"
    echo ""
    echo "O malware está sendo injetado durante a inicialização."
    echo "Investigue:"
    echo "  - Dependências npm comprometidas"
    echo "  - Scripts de inicialização"
    echo "  - Volumes Docker montados"
else
    echo "✓ Nenhum malware detectado"
fi
echo ""

# 12. Verificar containers
echo "12. Verificando containers..."
docker ps
echo ""

echo "=========================================="
echo "LIMPEZA CONCLUÍDA"
echo "=========================================="

