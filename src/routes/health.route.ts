import type { FastifyInstance } from 'fastify';

export function registerHealthRoute(app: FastifyInstance): Promise<void> {
  app.get('/health', () => ({ ok: true }));

  return Promise.resolve();
}
