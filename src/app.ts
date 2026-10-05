import Fastify, { LogController } from 'fastify';
import type { Redis } from 'ioredis';

import type { SkuQueue } from './queues/sku.queue.js';
import { ItemRepository } from './repositories/item.repository.js';
import { registerCheckRoute } from './routes/check.route.js';
import { registerHealthRoute } from './routes/health.route.js';
import { createProcessRoute } from './routes/process.route.js';
import { ProcessMessageService } from './services/process-message.service.js';

interface BuildAppOptions {
  redis: Redis;
  queue: SkuQueue;
  logLevel: string;
}

export function buildApp({ redis, queue, logLevel }: BuildAppOptions) {
  const app = Fastify({
    logger: { level: logLevel },
    logController: new LogController({ disableRequestLogging: true }),
  });

  redis.on('error', (error) => {
    app.log.error({ err: error }, 'redis connection error');
  });
  queue.on('error', (error) => {
    app.log.error({ err: error }, 'sku queue error');
  });

  const processMessageService = new ProcessMessageService(new ItemRepository(redis), queue);

  void app.register(registerHealthRoute);
  void app.register(registerCheckRoute);
  void app.register(createProcessRoute(processMessageService));

  return app;
}
