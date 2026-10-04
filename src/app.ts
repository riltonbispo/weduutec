import Fastify from 'fastify';

import { registerHealthRoute } from './http/routes/health.route.js';

export function buildApp() {
  const app = Fastify({
    logger: true,
  });

  void app.register(registerHealthRoute);

  return app;
}
