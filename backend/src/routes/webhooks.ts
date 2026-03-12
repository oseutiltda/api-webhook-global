import { Router } from 'express';
import { verifyWebhookSecret, ensureIdempotency } from '../middleware/auth';
import * as controller from '../controllers/webhookController';

const router = Router();

router.post('/cte/autorizado', verifyWebhookSecret, ensureIdempotency, controller.cteAutorizado);
router.post('/cte/cancelado', verifyWebhookSecret, ensureIdempotency, controller.cteCancelado);
router.post('/ctrb/ciot/base', verifyWebhookSecret, ensureIdempotency, controller.ciotBase);
router.post('/ctrb/ciot/parcelas', verifyWebhookSecret, ensureIdempotency, controller.ciotParcelas);
router.post(
  '/faturas/pagar/criar',
  verifyWebhookSecret,
  ensureIdempotency,
  controller.faturaPagarCriar,
);
router.post(
  '/faturas/pagar/baixar',
  verifyWebhookSecret,
  ensureIdempotency,
  controller.faturaPagarBaixar,
);
router.post(
  '/faturas/pagar/cancelar',
  verifyWebhookSecret,
  ensureIdempotency,
  controller.faturaPagarCancelar,
);
router.post(
  '/faturas/receber/criar',
  verifyWebhookSecret,
  ensureIdempotency,
  controller.faturaReceberCriar,
);
router.post(
  '/faturas/receber/baixar',
  verifyWebhookSecret,
  ensureIdempotency,
  controller.faturaReceberBaixar,
);
router.post('/nfse/autorizado', verifyWebhookSecret, ensureIdempotency, controller.nfseAutorizado);
router.post('/pessoa/upsert', verifyWebhookSecret, ensureIdempotency, controller.pessoaUpsert);

export default router;
