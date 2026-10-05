import type { FastifyInstance } from 'fastify';

import { checkBodySchema } from '../schemas/check.schema.js';

export function registerCheckRoute(app: FastifyInstance): Promise<void> {
  app.post('/check', async (request, reply) => {
    const result = checkBodySchema.safeParse(request.body);

    if (!result.success) {
      return reply.status(400).send({ error: 'invalid_payload' });
    }

    return reply.status(200).send({ token: result.data.token });
  });

  return Promise.resolve();
}
