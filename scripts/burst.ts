import { z } from 'zod';

import { WeduuClient } from '../src/clients/weduu.client.js';
import { loadWorkerEnv } from '../src/config/env.js';
import { createWorkerRedisConnection } from '../src/lib/redis.js';
import { ItemRepository } from '../src/repositories/item.repository.js';
import { RunRepository } from '../src/repositories/run.repository.js';
import { CallbackService } from '../src/services/callback.service.js';
import { RunFinalizer } from '../src/services/run-finalizer.service.js';

const burstSchema = z.object({
  run_id: z.string().min(1),
  total: z.number().int().positive(),
  started_at: z.string().min(1),
});

const logger = {
  info(context: Record<string, unknown>, message: string) {
    console.log(JSON.stringify({ level: 'info', message, ...context }));
  },
  warn(context: Record<string, unknown>, message: string) {
    console.warn(JSON.stringify({ level: 'warn', message, ...context }));
  },
  error(context: Record<string, unknown>, message: string) {
    console.error(JSON.stringify({ level: 'error', message, ...context }));
  },
};

const env = loadWorkerEnv();
const baseUrl = env.WEDUU_BASE_URL.endsWith('/') ? env.WEDUU_BASE_URL : `${env.WEDUU_BASE_URL}/`;
const response = await fetch(new URL(`burst/${encodeURIComponent(env.WEDUU_CID)}`, baseUrl), {
  method: 'POST',
  headers: { 'x-token': env.WEDUU_TOKEN },
  signal: AbortSignal.timeout(10_000),
});
if (!response.ok) throw new Error(`Burst failed with HTTP ${String(response.status)}`);
const burst = burstSchema.parse(await response.json());
console.log(JSON.stringify(burst));

const redis = createWorkerRedisConnection(env.REDIS_URL);
try {
  const runs = new RunRepository(redis, env.CALLBACK_MAX_ATTEMPTS);
  const items = new ItemRepository(redis);
  const registration = await runs.registerRun(burst.run_id, burst.total, Date.now());
  if (registration.conflict) {
    logger.warn(
      { run_id: burst.run_id, retained_total: registration.total, received_total: burst.total },
      'run total conflict; retained first value',
    );
  }
  const client = new WeduuClient({
    baseUrl: env.WEDUU_BASE_URL,
    cid: env.WEDUU_CID,
    token: env.WEDUU_TOKEN,
    timeoutMs: env.ENRICH_TIMEOUT_MS,
    callbackTimeoutMs: env.CALLBACK_TIMEOUT_MS,
  });
  const callbackService = new CallbackService(runs, items, logger);
  const finalizer = new RunFinalizer({
    runs,
    callbackService,
    client,
    leaseMs: env.CALLBACK_LEASE_MS,
    backoffBaseMs: env.CALLBACK_BACKOFF_BASE_MS,
    random: Math.random,
    logger,
  });
  await finalizer.finalize(burst.run_id);
} finally {
  await redis.quit();
}
