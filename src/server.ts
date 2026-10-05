import { buildApp } from './app.js';
import { loadEnv } from './config/env.js';
import { createRedisConnection } from './lib/redis.js';
import { createSkuQueue } from './queues/sku.queue.js';

try {
  const env = loadEnv();
  const redis = createRedisConnection(env.REDIS_URL, {
    commandTimeoutMs: env.REDIS_COMMAND_TIMEOUT_MS,
    connectTimeoutMs: env.REDIS_CONNECT_TIMEOUT_MS,
  });
  const queue = createSkuQueue(redis, { maxAttempts: env.ENRICH_MAX_ATTEMPTS });
  const app = buildApp({ redis, queue, logLevel: env.LOG_LEVEL });
  let shutdownPromise: Promise<void> | undefined;

  function shutdown(signal: NodeJS.Signals | 'startup_error'): Promise<void> {
    shutdownPromise ??= (async () => {
      app.log.info({ signal }, 'shutting down');
      let failed = false;

      try {
        await app.close();
      } catch (error) {
        failed = true;
        app.log.error({ err: error }, 'failed to close HTTP server');
      }

      try {
        await queue.close();
      } catch (error) {
        failed = true;
        app.log.error({ err: error }, 'failed to close SKU queue');
      }

      try {
        await redis.quit();
      } catch (error) {
        failed = true;
        redis.disconnect();
        app.log.error({ err: error }, 'failed to close Redis connection gracefully');
      }

      if (failed) {
        process.exitCode = 1;
      }
    })();

    return shutdownPromise;
  }

  process.once('SIGTERM', () => {
    void shutdown('SIGTERM');
  });
  process.once('SIGINT', () => {
    void shutdown('SIGINT');
  });

  try {
    await app.listen({ port: env.PORT, host: '0.0.0.0' });
  } catch (error) {
    app.log.error({ err: error }, 'failed to start server');
    process.exitCode = 1;
    await shutdown('startup_error');
  }
} catch (error) {
  const message = error instanceof Error ? error.message : 'Unknown startup error';
  console.error(`Failed to start server: ${message}`);
  process.exitCode = 1;
}
