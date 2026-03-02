const { PrismaClient } = require('@prisma/client');

const prisma = new PrismaClient();
const NFSE_SOURCE_DATABASE = process.env.NFSE_SOURCE_DATABASE || 'AFS_INTEGRADOR';

async function main() {
  const ids = process.argv.slice(2).map((id) => Number(id)).filter(Boolean);

  if (ids.length === 0) {
    console.error('Informe pelo menos um ID de NFSe para resetar');
    process.exit(1);
  }

  const idList = ids.join(',');
  const tableName = `[${NFSE_SOURCE_DATABASE}].[dbo].[nfse]`;

  await prisma.$executeRawUnsafe(`
    UPDATE ${tableName}
    SET processed = 0,
        status = 'received',
        error_message = NULL
    WHERE id IN (${idList})
  `);

  console.log(`NFSe resetadas: ${idList}`);
}

main()
  .catch((error) => {
    console.error(error);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });

