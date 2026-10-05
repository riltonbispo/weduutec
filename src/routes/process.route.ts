import type { FastifyInstance } from 'fastify';

import { processBodySchema } from '../schemas/process.schema.js';
import type { ProcessMessageHandler } from '../services/process-message.service.js';
import { ProcessMessageUnavailableError } from '../services/process-message.service.js';

export function createProcessRoute(
  processMessageService: ProcessMessageHandler,
): (app: FastifyInstance) => Promise<void> {
  return function registerProcessRoute(app: FastifyInstance): Promise<void> {
    app.post('/process', async (request, reply) => {
      const parsedBody = processBodySchema.safeParse(request.body);

      if (!parsedBody.success) {
        return reply.status(400).send({ error: 'invalid_payload' });
      }

      const startedAt = performance.now();
      const logContext = {
        run_id: parsedBody.data.run_id,
        seq: parsedBody.data.seq,
      };

      try {
        await processMessageService.process(parsedBody.data);
        request.log.debug(
          { ...logContext, duration_ms: performance.now() - startedAt },
          'process message accepted',
        );

        return await reply.status(200).send({ ok: true });
      } catch (error) {
        if (error instanceof ProcessMessageUnavailableError) {
          request.log.warn(
            { ...logContext, duration_ms: performance.now() - startedAt },
            'process message storage unavailable',
          );

          return reply.status(503).send({ error: 'service_unavailable' });
        }

        throw error;
      }
    });

    return Promise.resolve();
  };
}
