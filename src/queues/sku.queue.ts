import { Queue } from 'bullmq';
import type { Redis } from 'ioredis';

export const SKU_QUEUE_NAME = 'sku-enrich';
const SKU_JOB_NAME = 'enrich-sku';

export interface SkuJobData {
  runId: string;
  seq: number;
  sku: string;
}

export type SkuQueue = Queue<SkuJobData, void, typeof SKU_JOB_NAME>;

export interface SkuQueueWriter {
  add(name: typeof SKU_JOB_NAME, data: SkuJobData, options: { jobId: string }): Promise<unknown>;
}

export interface SkuQueueOptions {
  maxAttempts?: number;
}

export function createSkuQueue(connection: Redis, options: SkuQueueOptions = {}): SkuQueue {
  return new Queue<SkuJobData, void, typeof SKU_JOB_NAME>(SKU_QUEUE_NAME, {
    connection,
    skipWaitingForReady: true,
    defaultJobOptions: {
      attempts: options.maxAttempts ?? 5,
      backoff: { type: 'custom' },
      // Item state remains durable in its Redis hash after completed jobs are pruned.
      removeOnComplete: { age: 3600, count: 1000 },
      removeOnFail: false,
    },
  });
}

export async function setSkuQueueGlobalConcurrency(queue: SkuQueue): Promise<void> {
  await queue.setGlobalConcurrency(3);
}

export async function enqueueSku(queue: SkuQueueWriter, data: SkuJobData): Promise<void> {
  await queue.add(SKU_JOB_NAME, data, {
    jobId: `${data.runId}_${String(data.seq)}`,
  });
}
