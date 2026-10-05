import { z } from 'zod';

import { loadEnv } from '../src/config/env.js';
import { createWorkerRedisConnection } from '../src/lib/redis.js';
import { ItemRepository } from '../src/repositories/item.repository.js';
import { RunRepository } from '../src/repositories/run.repository.js';

const runId = z.string().min(1, 'run_id is required').parse(process.argv[2]);
const env = loadEnv();
const redis = createWorkerRedisConnection(env.REDIS_URL);

try {
  const runs = new RunRepository(redis, env.CALLBACK_MAX_ATTEMPTS);
  const items = new ItemRepository(redis);
  const run = await runs.getRun(runId);
  if (run === null) throw new Error(`Run not found: ${runId}`);

  const resolution = await runs.getResolution(runId, run.total ?? 0);
  const itemSeqs = await items.listRunItemSeqs(runId);
  const records = await items.getItems(runId, itemSeqs);
  const failed = records.flatMap((item) =>
    item?.status === 'failed'
      ? [
          {
            seq: item.seq,
            sku: item.sku,
            kind: item.errorKind,
            error: item.error,
            attempts: item.attempts,
            status_code: item.statusCode,
          },
        ]
      : [],
  );

  console.log(
    JSON.stringify(
      {
        run_id: run.runId,
        total: run.total ?? null,
        status: run.status,
        callback_attempts: run.callbackAttempts,
        resolved: {
          count: resolution.resolvedSeqs.length,
          seqs: resolution.resolvedSeqs,
        },
        failed,
      },
      null,
      2,
    ),
  );
} finally {
  await redis.quit();
}
