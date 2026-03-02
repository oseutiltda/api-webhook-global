const path = require('path');
const dotenv = require('dotenv');
const { PrismaClient } = require('@prisma/client');

// Suporta execucao do seed tanto em /backend quanto em containers.
dotenv.config();
dotenv.config({ path: path.resolve(__dirname, '../../.env') });

const prisma = new PrismaClient();

async function waitForDatabase(maxAttempts = 30, delayMs = 2000) {
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      await prisma.$queryRawUnsafe('SELECT 1');
      return;
    } catch (error) {
      if (attempt === maxAttempts) {
        throw error;
      }
      // eslint-disable-next-line no-console
      console.warn(
        `[seed] banco indisponivel (tentativa ${attempt}/${maxAttempts}), aguardando ${delayMs}ms...`,
      );
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
}

async function main() {
  const login =
    process.env.ADMIN_SEED_LOGIN || process.env.NEXT_PUBLIC_ADMIN1_USER || 'admin@admin';
  const password =
    process.env.ADMIN_SEED_PASSWORD || process.env.NEXT_PUBLIC_ADMIN1_PASSWORD || 'admin#change-me';
  const role = process.env.ADMIN_SEED_ROLE || 'ADMIN';

  await waitForDatabase();

  await prisma.$executeRawUnsafe(`
    CREATE TABLE IF NOT EXISTS admin_users (
      id SERIAL PRIMARY KEY,
      login VARCHAR(120) NOT NULL UNIQUE,
      password VARCHAR(255) NOT NULL,
      role VARCHAR(30) NOT NULL DEFAULT 'ADMIN',
      is_active BOOLEAN NOT NULL DEFAULT true,
      created_at TIMESTAMP NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMP NOT NULL DEFAULT NOW()
    );
  `);

  await prisma.$executeRawUnsafe(
    `
      INSERT INTO admin_users (login, password, role, is_active, created_at, updated_at)
      VALUES ($1, $2, $3, true, NOW(), NOW())
      ON CONFLICT (login) DO UPDATE
      SET password = EXCLUDED.password,
          role = EXCLUDED.role,
          is_active = true,
          updated_at = NOW();
    `,
    login,
    password,
    role,
  );

  // eslint-disable-next-line no-console
  console.log(`[seed] admin pronto: ${login}`);
}

main()
  .catch((error) => {
    // eslint-disable-next-line no-console
    console.error('[seed] erro ao executar seed', error);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
