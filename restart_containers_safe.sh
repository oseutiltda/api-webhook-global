#!/bin/bash
# Script para reiniciar containers de forma segura

echo "=========================================="
echo "REINICIANDO CONTAINERS DE FORMA SEGURA"
echo "=========================================="
echo ""

cd /opt/API-WEBHOOK-BMX

# 1. Verificar status atual
echo "1. Status atual dos containers:"
docker-compose ps
echo ""

# 2. Parar containers (se estiverem rodando)
echo "2. Parando containers (se estiverem rodando)..."
docker-compose down
echo ""

# 3. Limpar imagens antigas (opcional - descomente se quiser forçar rebuild completo)
# echo "3. Removendo imagens antigas..."
# docker-compose rm -f
# docker rmi api-webhook-bmx-backend api-webhook-bmx-frontend api-webhook-bmx-worker 2>/dev/null
# echo ""

# 4. Recriar e subir containers
echo "3. Recriando e subindo containers..."
docker-compose up -d --build
echo ""

# 5. Aguardar inicialização
echo "4. Aguardando containers iniciarem (10 segundos)..."
sleep 10
echo ""

# 6. Verificar status
echo "5. Status dos containers:"
docker-compose ps
echo ""

# 7. Verificar logs (primeiras linhas)
echo "6. Verificando logs (primeiras 20 linhas de cada container):"
echo "--- Backend ---"
docker-compose logs --tail=20 backend 2>/dev/null
echo ""
echo "--- Worker ---"
docker-compose logs --tail=20 worker 2>/dev/null
echo ""
echo "--- Frontend ---"
docker-compose logs --tail=20 frontend 2>/dev/null
echo ""

# 8. Verificar processos suspeitos
echo "7. Verificando processos no sistema host..."
ps aux | grep -E "(x56A2NNve|jSKhrxE|QVibT|iyLoZ1PFq|v2dL5n|BXMvVzc|/[a-zA-Z0-9]{6,})" | grep -v grep
if [ $? -eq 0 ]; then
    echo "⚠️  ATENÇÃO: Processos suspeitos detectados após subir containers!"
else
    echo "✓ Nenhum processo suspeito encontrado"
fi
echo ""

echo "=========================================="
echo "CONTAINERS REINICIADOS"
echo "=========================================="
echo ""
echo "IMPORTANTE:"
echo "- Monitore os processos por alguns minutos:"
echo "  watch -n 5 'ps aux | grep -E \"[a-zA-Z0-9]{6,}\" | grep -v grep'"
echo ""
echo "- Se processos suspeitos aparecerem, pare os containers:"
echo "  docker-compose down"
echo ""

