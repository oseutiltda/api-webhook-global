import { createServer } from 'http';
import app from './app';
import { logger } from './utils/logger';

const port = process.env.PORT ? Number(process.env.PORT) : 3000;

const server = createServer(app);

server.listen(port, () => {
  logger.info({ port }, 'Servidor iniciado');
});

process.on('unhandledRejection', (reason, promise) => {
  const errorDetails: any = {
    promise: promise?.toString(),
  };
  
  if (reason instanceof Error) {
    errorDetails.message = reason.message;
    errorDetails.stack = reason.stack;
    errorDetails.name = reason.name;
  } else if (typeof reason === 'object' && reason !== null) {
    errorDetails.reason = JSON.stringify(reason, Object.getOwnPropertyNames(reason));
  } else {
    errorDetails.reason = String(reason);
  }
  
  logger.error(errorDetails, 'Unhandled Rejection');
});

process.on('uncaughtException', (error) => {
  logger.error({ error }, 'Uncaught Exception');
  process.exit(1);
});


