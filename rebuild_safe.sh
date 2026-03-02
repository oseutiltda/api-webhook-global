#!/bin/bash
# Script para recriar containers de forma segura

echo "=========================================="
echo "RECONSTRUÇÃO SEGURA DOS CONTAINERS"
echo "=========================================="
echo ""

cd /opt/API-WEBHOOK-BMX

# 1. Parar e remover tudo
echo "1. Parando e removendo containers..."
docker-compose down -v
echo ""

# 2. Remover imagens antigas
echo "2. Removendo imagens antigas..."
docker rmi api-webhook-bmx-backend api-webhook-bmx-frontend api-webhook-bmx-worker 2>/dev/null
docker image prune -f
echo ""

# 3. Limpar cache do Docker
echo "3. Limpando cache do Docker..."
docker builder prune -af
echo ""

# 4. Verificar se processos maliciosos desapareceram
echo "4. Verificando processos maliciosos..."
ps aux | grep -E "(6a06VGn|ouhH6us|szMx3)" | grep -v grep
if [ $? -eq 0 ]; then
    echo "⚠️  PROCESSOS AINDA RODANDO - matando..."
    pkill -9 -f "6a06VGn|ouhH6us|szMx3"
    sleep 2
else
    echo "✓ Nenhum processo malicioso encontrado"
fi
echo ""

# 5. Recriar imagens do zero (sem cache)
echo "5. Recriando imagens do zero (isso pode demorar)..."
docker-compose build --no-cache --pull
echo ""

# 6. Subir containers
echo "6. Subindo containers..."
docker-compose up -d
echo ""

# 7. Aguardar inicialização
echo "7. Aguardando containers iniciarem (15 segundos)..."
sleep 15
echo ""

# 8. Verificar status
echo "8. Status dos containers:"
docker-compose ps
echo ""

# 9. Monitorar processos (aguardar 30 segundos e verificar)
echo "9. Monitorando processos por 30 segundos..."
for i in {1..6}; do
    sleep 5
    # Procurar apenas processos maliciosos conhecidos ou executáveis na raiz com nomes aleatórios
    SUSPICIOUS=$(ps aux | grep -E "(6a06VGn|ouhH6us|szMx3|x56A2NNve|jSKhrxE|QVibT|iyLoZ1PFq|v2dL5n|BXMvVzc|/x56A2NNve|/jSKhrxE|/QVibT|/iyLoZ1PFq|/v2dL5n|/BXMvVzc|/6a06VGn|/ouhH6us|/szMx3)" | grep -v grep)
    
    # Também procurar por processos executando arquivos na raiz com nomes aleatórios (não caminhos normais)
    ROOT_SUSPICIOUS=$(ps aux | awk '{print $11}' | grep -E "^/[a-zA-Z0-9]{6,}$" | grep -vE "^/(usr|var|opt|etc|proc|sys|dev|tmp|root|home|bin|sbin|lib)" | sort -u)
    if [ ! -z "$SUSPICIOUS" ] || [ ! -z "$ROOT_SUSPICIOUS" ]; then
        echo "⚠️  ALERTA: Processos suspeitos detectados após $((i*5)) segundos!"
        if [ ! -z "$SUSPICIOUS" ]; then
            echo "Processos conhecidos:"
            echo "$SUSPICIOUS"
        fi
        if [ ! -z "$ROOT_SUSPICIOUS" ]; then
            echo "Executáveis suspeitos na raiz:"
            echo "$ROOT_SUSPICIOUS"
        fi
        echo ""
        echo "PARANDO CONTAINERS IMEDIATAMENTE..."
        docker-compose down
        exit 1
    fi
    echo "  Verificação $i/6: OK"
done
echo ""

echo "=========================================="
echo "RECONSTRUÇÃO CONCLUÍDA"
echo "=========================================="
echo ""
echo "✓ Containers recriados e rodando"
echo "✓ Nenhum processo malicioso detectado após 30 segundos"
echo ""
echo "CONTINUE MONITORANDO:"
echo "  watch -n 5 'ps aux | grep -E \"[a-zA-Z0-9]{6,}\" | grep -v grep'"
echo ""

