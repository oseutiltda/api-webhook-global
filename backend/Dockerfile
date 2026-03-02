FROM node:20 AS builder

# Node 20 completo já inclui OpenSSL - não precisa instalar

WORKDIR /app

COPY package*.json tsconfig.json ./
COPY prisma ./prisma

# Instalar dependências (dividido para evitar problemas com runc)
RUN npm install --no-audit --no-fund

COPY src ./src

# Gerar Prisma e build (dividido para evitar problemas com runc)
RUN npm run prisma:generate
RUN npm run build

# Remove dependências de desenvolvimento após o build
RUN npm prune --omit=dev

FROM node:20 AS runner

# node:20 já inclui OpenSSL, não precisa instalar
# Usar node:20 ao invés de node:20-slim para evitar problemas com runc/containerd

ENV NODE_ENV=production
WORKDIR /app

COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/package*.json ./
COPY --from=builder /app/prisma ./prisma

EXPOSE 3000

CMD ["sh", "-c", "node prisma/seed.js && node dist/server.js"]
