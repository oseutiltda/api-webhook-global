FROM node:20 AS builder

WORKDIR /app

COPY package*.json ./
COPY tsconfig.json ./

RUN npm install

# Copiar scripts de inicialização antes do build
COPY start.sh ./start.sh
COPY start.js ./start.js
RUN chmod +x ./start.sh ./start.js

COPY . .

ARG NEXT_PUBLIC_API_BASE_URL
ENV NEXT_PUBLIC_API_BASE_URL=${NEXT_PUBLIC_API_BASE_URL}

# Limitar uso de memória durante o build para evitar OOM
ENV NODE_OPTIONS="--max-old-space-size=3072"

RUN npm run build

# Remover dependências de desenvolvimento após o build
RUN npm prune --omit=dev || true

FROM node:20-slim AS runner

WORKDIR /app

ENV NODE_ENV=production
ENV PORT=3001

COPY --from=builder /app/package*.json ./
COPY --from=builder /app/tsconfig.json ./tsconfig.json
COPY --from=builder /app/next.config.js ./next.config.js
COPY --from=builder /app/.next ./.next
COPY --from=builder /app/public ./public
COPY --from=builder /app/node_modules ./node_modules

# Copiar scripts de inicialização
COPY --from=builder /app/start.sh ./start.sh
COPY --from=builder /app/start.js ./start.js
RUN chmod +x ./start.sh ./start.js

EXPOSE 3001

# Usar script Node.js que silencia erros de .env (mais robusto)
CMD ["node", "start.js"]

