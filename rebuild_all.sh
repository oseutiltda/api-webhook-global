#!/bin/bash
# Script simplificado para reconstruir tudo do zero

echo "=========================================="
echo "RECONSTRUÇÃO COMPLETA DO SISTEMA"
echo "=========================================="
echo ""

# 1. Matar processos maliciosos (versão mais segura)
echo "1. Matando processos maliciosos..."
for pid in $(ps aux | grep -E "(FdJ7qB|J6RtzFm|33zdXf1Z|QmjPwS5N|qQeRFXxm|SF9tZc)" | grep -v grep | grep -v "defunct" | awk '{print $2}'); do
    kill -9 $pid 2>/dev/null || true
done
echo "✓ Processos maliciosos mortos"
echo ""

# 2. Parar containers
echo "2. Parando containers..."
docker-compose down 2>/dev/null || true
echo "✓ Containers parados"
echo ""

# 3. Remover containers
echo "3. Removendo containers..."
docker ps -aq | xargs docker rm -f 2>/dev/null || true
echo "✓ Containers removidos"
echo ""

# 4. Remover imagens
echo "4. Removendo imagens..."
docker images -q | xargs docker rmi -f 2>/dev/null || true
echo "✓ Imagens removidas"
echo ""

# 5. Limpar cache
echo "5. Limpando cache Docker..."
docker builder prune -af 2>/dev/null || true
docker system prune -af 2>/dev/null || true
echo "✓ Cache limpo"
echo ""

# 6. Reconstruir
echo "6. Reconstruindo containers (isso pode levar vários minutos)..."
echo "   Por favor, aguarde..."
docker-compose build --no-cache 2>&1
BUILD_EXIT=$?

if [ $BUILD_EXIT -eq 0 ]; then
    echo "✓ Reconstrução concluída com sucesso"
else
    echo "⚠️  Reconstrução teve erros, mas continuando..."
fi
echo ""

# 7. Subir containers
echo "7. Subindo containers..."
docker-compose up -d 2>&1
echo "✓ Containers subindo"
echo ""

# 8. Aguardar
echo "8. Aguardando 30 segundos para inicialização..."
sleep 30
echo ""

# 9. Verificar malware
echo "9. Verificando processos maliciosos..."
MALWARE=$(ps aux | grep -E "(FdJ7qB|J6RtzFm|33zdXf1Z|QmjPwS5N)" | grep -v grep | grep -v "defunct" || true)

if [ -z "$MALWARE" ]; then
    echo "✓ Nenhum malware detectado!"
else
    echo "⚠️  MALWARE DETECTADO:"
    echo "$MALWARE"
fi
echo ""

# 10. Status dos containers
echo "10. Status dos containers:"
docker ps
echo ""

echo "=========================================="
echo "RECONSTRUÇÃO CONCLUÍDA"
echo "=========================================="

