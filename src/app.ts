import Fastify from 'fastify';

import { registerHealthRoute } from './http/routes/health.route.js';

interface BuildAppOptions {
  logger?: boolean;
  logLevel?: string;
}

export function buildApp(options: BuildAppOptions = {}) {
  const app = Fastify({
    logger: options.logger === false ? false : { level: options.logLevel ?? 'info' },
  });

  void app.register(registerHealthRoute);

  return app;
}
