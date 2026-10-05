import type { Redis } from 'ioredis';

import { WeduuClient } from './clients/weduu.client.js';
import { loadWorkerEnv } from './config/env.js';
import { createWorkerRedisConnection } from './lib/redis.js';
import { createSkuQueue, setSkuQueueGlobalConcurrency } from './queues/sku.queue.js';
import type { SkuQueue } from './queues/sku.queue.js';
import { ItemRepository } from './repositories/item.repository.js';
import type { EnrichLogger } from './services/enrich-sku.service.js';
import { createSkuWorker } from './workers/sku.worker.js';
import type { SkuWorker } from './workers/sku.worker.js';

const logger: EnrichLogger & {
  error(context: Record<string, unknown>, message: string): void;
} = {
  info(context, message) {
    console.log(JSON.stringify({ level: 'info', message, ...context }));
  },
  error(context, message) {
    console.error(JSON.stringify({ level: 'error', message, ...context }));
  },
};

let worker: SkuWorker | undefined;
let queue: SkuQueue | undefined;
let workerRedis: Redis | undefined;
let repositoryRedis: Redis | undefined;
let queueRedis: Redis | undefined;
let shutdownPromise: Promise<void> | undefined;

function closeRedis(redis: Redis | undefined): Promise<unknown> | undefined {
  if (redis === undefined) {
    return undefined;
  }

  return redis.quit().catch((error: unknown) => {
    redis.disconnect();
    logger.error({ err: error }, 'failed to close Redis connection gracefully');
  });
}

function shutdown(signal: NodeJS.Signals | 'startup_error'): Promise<void> {
  shutdownPromise ??= (async () => {
    logger.info({ signal }, 'shutting down enrich worker');

    await Promise.allSettled(
      [worker?.close(), queue?.close()].filter(
        (operation): operation is Promise<void> => operation !== undefined,
      ),
    );
    await Promise.allSettled(
      [closeRedis(workerRedis), closeRedis(repositoryRedis), closeRedis(queueRedis)].filter(
        (operation): operation is Promise<unknown> => operation !== undefined,
      ),
    );
  })();

  return shutdownPromise;
}

try {
  const env = loadWorkerEnv();
  workerRedis = createWorkerRedisConnection(env.REDIS_URL);
  repositoryRedis = createWorkerRedisConnection(env.REDIS_URL);
  queueRedis = createWorkerRedisConnection(env.REDIS_URL);
  queue = createSkuQueue(queueRedis, { maxAttempts: env.ENRICH_MAX_ATTEMPTS });
  await setSkuQueueGlobalConcurrency(queue);

  const client = new WeduuClient({
    baseUrl: env.WEDUU_BASE_URL,
    cid: env.WEDUU_CID,
    token: env.WEDUU_TOKEN,
    timeoutMs: env.ENRICH_TIMEOUT_MS,
    maxInFlight: 3,
  });
  worker = createSkuWorker({
    connection: workerRedis,
    client,
    itemRepository: new ItemRepository(repositoryRedis),
    policy: {
      maxAttempts: env.ENRICH_MAX_ATTEMPTS,
      maxRateLimitWaits: env.ENRICH_RATE_LIMIT_MAX_WAITS,
      baseMs: env.ENRICH_BACKOFF_BASE_MS,
      maxMs: env.ENRICH_BACKOFF_MAX_MS,
    },
    random: Math.random,
    logger,
    lockDurationMs: Math.max(30_000, env.ENRICH_TIMEOUT_MS * 2),
  });
  worker.on('error', (error) => {
    logger.error({ err: error }, 'enrich worker error');
  });
  await worker.waitUntilReady();
  logger.info({ concurrency: 3 }, 'enrich worker ready');

  process.once('SIGTERM', () => {
    void shutdown('SIGTERM');
  });
  process.once('SIGINT', () => {
    void shutdown('SIGINT');
  });
} catch (error) {
  logger.error({ err: error }, 'failed to start enrich worker');
  process.exitCode = 1;
  await shutdown('startup_error');
}
