const path = require('path');
const dotenv = require('dotenv');
const { PrismaClient } = require('@prisma/client');

dotenv.config();
dotenv.config({ path: path.resolve(__dirname, '../../.env') });

const prisma = new PrismaClient();

const CTE_IDS = [990001, 990002, 990003, 990004, 990005, 990006];

async function main() {
  await prisma.cte.deleteMany({
    where: {
      external_id: {
        in: CTE_IDS,
      },
    },
  });

  await prisma.cte.createMany({
    data: [
      {
        external_id: 990001,
        authorization_number: 700001,
        status: 'AUTORIZADO',
        xml: '<cte><id>990001</id><status>AUTORIZADO</status></cte>',
        event_xml: null,
        processed: false,
      },
      {
        external_id: 990002,
        authorization_number: 700002,
        status: 'AUTORIZADO',
        xml: '<cte><id>990002</id><status>AUTORIZADO</status></cte>',
        event_xml: null,
        processed: true,
      },
      {
        external_id: 990003,
        authorization_number: 700003,
        status: 'CANCELADO',
        xml: '<cte><id>990003</id><status>CANCELADO</status></cte>',
        event_xml: '<evento><tipo>CANCELAMENTO</tipo></evento>',
        processed: false,
      },
      {
        external_id: 990004,
        authorization_number: 700004,
        status: 'CANCELADO',
        xml: '<cte><id>990004</id><status>CANCELADO</status></cte>',
        event_xml: '<evento><tipo>CANCELAMENTO</tipo></evento>',
        processed: true,
      },
      {
        external_id: 990005,
        authorization_number: 700005,
        status: 'PROCESSANDO',
        xml: '<cte><id>990005</id><status>PROCESSANDO</status></cte>',
        event_xml: null,
        processed: false,
      },
      {
        external_id: 990006,
        authorization_number: 700006,
        status: 'ERRO',
        xml: '<cte><id>990006</id><status>ERRO</status></cte>',
        event_xml: '<erro><mensagem>Falha simulada</mensagem></erro>',
        processed: false,
      },
    ],
  });

  // eslint-disable-next-line no-console
  console.log('[seed:ctes] 6 CT-es de teste inseridos com sucesso.');
}

main()
  .catch((error) => {
    // eslint-disable-next-line no-console
    console.error('[seed:ctes] erro ao executar seed', error);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
