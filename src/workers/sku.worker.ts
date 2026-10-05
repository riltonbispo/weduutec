import { Worker } from 'bullmq';
import type { Redis } from 'ioredis';

import { computeBackoffMs } from '../lib/retry.js';
import type { RetryPolicy } from '../lib/retry.js';
import { SKU_QUEUE_NAME } from '../queues/sku.queue.js';
import type { SkuJobData } from '../queues/sku.queue.js';
import { processSkuJob } from '../services/enrich-sku.service.js';
import type {
  EnrichClientPort,
  EnrichLogger,
  ItemRepositoryPort,
} from '../services/enrich-sku.service.js';

export type SkuWorker = Worker<SkuJobData, void>;

export interface CreateSkuWorkerOptions {
  connection: Redis;
  client: EnrichClientPort;
  itemRepository: ItemRepositoryPort;
  policy: Omit<RetryPolicy, 'random'>;
  random: () => number;
  logger: EnrichLogger;
  lockDurationMs: number;
  finalizeRun: (runId: string) => Promise<unknown>;
}

export function createSkuWorker(options: CreateSkuWorkerOptions): SkuWorker {
  const worker = new Worker<SkuJobData, void>(
    SKU_QUEUE_NAME,
    async (job) => {
      await processSkuJob(job, {
        client: options.client,
        itemRepository: options.itemRepository,
        policy: options.policy,
        random: options.random,
        rateLimit: async (delayMs) => {
          // Required by BullMQ's manual rate-limit protocol; RateLimitError avoids attemptsMade.
          // eslint-disable-next-line @typescript-eslint/no-deprecated
          await worker.rateLimit(delayMs);
        },
        logger: options.logger,
        finalizeRun: options.finalizeRun,
      });
    },
    {
      connection: options.connection,
      concurrency: 3,
      lockDuration: options.lockDurationMs,
      settings: {
        backoffStrategy: (attemptsMade) =>
          computeBackoffMs(attemptsMade, {
            baseMs: options.policy.baseMs ?? 500,
            maxMs: options.policy.maxMs ?? 8_000,
            random: options.random,
          }),
      },
    },
  );

  return worker;
}
