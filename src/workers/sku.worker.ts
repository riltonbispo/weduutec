import { RATE_LIMIT_ERROR, Worker } from 'bullmq';
import type { Redis } from 'ioredis';

import { EnrichError } from '../domain/errors.js';
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

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function isTerminalStatus(status: string): boolean {
  return status === 'completed' || status === 'failed';
}

export function createSkuWorker(options: CreateSkuWorkerOptions): SkuWorker {
  const worker = new Worker<SkuJobData, void>(
    SKU_QUEUE_NAME,
    async (job) => {
      try {
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
      } catch (error) {
        if (
          error instanceof EnrichError ||
          (error instanceof Error && error.message === RATE_LIMIT_ERROR)
        ) {
          throw error;
        }

        const attempt = job.attemptsMade + 1;
        const maxAttempts = job.opts.attempts ?? 1;
        if (attempt < maxAttempts) {
          throw error;
        }

        const failed = await options.itemRepository.failItem(job.data.runId, job.data.seq, {
          kind: 'transient',
          message: `internal_error: ${errorMessage(error)}`,
          attempts: attempt,
        });
        if (!failed) {
          const item = await options.itemRepository.getItem(job.data.runId, job.data.seq);
          if (item === null || !isTerminalStatus(item.status)) {
            throw error;
          }
          return;
        }

        options.logger.info(
          {
            run_id: job.data.runId,
            seq: job.data.seq,
            sku: job.data.sku,
            attempt,
            kind: 'internal',
            action: 'fail',
          },
          'SKU internal failure exhausted retries',
        );
        await options.finalizeRun(job.data.runId);
      }
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
