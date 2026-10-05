import { Worker } from 'bullmq';
import type { Redis } from 'ioredis';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

import { createWorkerRedisConnection } from '../../src/lib/redis.js';
import { enqueueSku, createSkuQueue, SKU_QUEUE_NAME } from '../../src/queues/sku.queue.js';
import type { SkuJobData, SkuQueue } from '../../src/queues/sku.queue.js';
import { ItemRepository } from '../../src/repositories/item.repository.js';
import type { ItemRecord } from '../../src/repositories/item.repository.js';
import { ProcessMessageService } from '../../src/services/process-message.service.js';
import type { ProcessItemRepository } from '../../src/services/process-message.service.js';
import { createSkuWorker } from '../../src/workers/sku.worker.js';
import type { SkuWorker } from '../../src/workers/sku.worker.js';
import type { ItemRepositoryPort } from '../../src/services/enrich-sku.service.js';
import { createTestRedisConnection } from '../helpers/redis.js';

const TEST_REDIS_URL = process.env.TEST_ENQUEUE_RACE_REDIS_URL ?? 'redis://127.0.0.1:6379/11';

interface WorkerHarness {
  worker: SkuWorker;
  workerRedis: Redis;
  repositoryRedis: Redis;
}

let redis: Redis;
let queue: SkuQueue;
let items: ItemRepository;
let workers: WorkerHarness[] = [];
let rawWorkers: { worker: Worker<SkuJobData, void>; redis: Redis }[] = [];

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function waitForItem(runId: string, seq: number, status: ItemRecord['status']) {
  const deadline = Date.now() + 5_000;

  while (Date.now() < deadline) {
    const item = await items.getItem(runId, seq);
    if (item?.status === status) return item;
    await delay(10);
  }

  throw new Error(`Timed out waiting for ${runId}:${String(seq)} to become ${status}`);
}

async function startWorker(itemRepository?: ItemRepositoryPort): Promise<WorkerHarness> {
  const workerRedis = createWorkerRedisConnection(TEST_REDIS_URL);
  const repositoryRedis = createWorkerRedisConnection(TEST_REDIS_URL);
  const worker = createSkuWorker({
    connection: workerRedis,
    client: {
      enrich: (sku) => Promise.resolve({ sku, price: 10, stock: 1 }),
    },
    itemRepository: itemRepository ?? new ItemRepository(repositoryRedis),
    policy: { maxAttempts: 5, maxRateLimitWaits: 20, baseMs: 1, maxMs: 5 },
    random: () => 0,
    logger: { info: vi.fn() },
    lockDurationMs: 10_000,
    finalizeRun: () => Promise.resolve(),
  });
  await worker.waitUntilReady();

  const harness = { worker, workerRedis, repositoryRedis };
  workers.push(harness);
  return harness;
}

beforeAll(async () => {
  redis = createTestRedisConnection(TEST_REDIS_URL);
  await redis.connect();
  queue = createSkuQueue(redis, { maxAttempts: 5 });
  items = new ItemRepository(redis);
  await queue.waitUntilReady();
});

beforeEach(async () => {
  await redis.flushdb();
  workers = [];
  rawWorkers = [];
});

afterEach(async () => {
  await Promise.all(workers.map(({ worker }) => worker.close()));
  await Promise.all(
    workers.flatMap(({ workerRedis, repositoryRedis }) => [
      workerRedis.quit(),
      repositoryRedis.quit(),
    ]),
  );
  await Promise.all(rawWorkers.map(({ worker }) => worker.close()));
  await Promise.all(rawWorkers.map(({ redis: workerRedis }) => workerRedis.quit()));
});

afterAll(async () => {
  await queue.close();
  await redis.quit();
});

describe('enqueue and item-state race recovery', () => {
  it('completes without retry when the worker consumes before markQueued finishes', async () => {
    const runId = 'run-paused-mark-queued';
    let releaseMarkQueued: (() => void) | undefined;
    let markQueuedStarted: (() => void) | undefined;
    const markQueuedGate = new Promise<void>((resolve) => {
      releaseMarkQueued = resolve;
    });
    const markQueuedHasStarted = new Promise<void>((resolve) => {
      markQueuedStarted = resolve;
    });
    const pausedRepository: ProcessItemRepository = {
      receiveItem: (currentRunId, seq, sku) => items.receiveItem(currentRunId, seq, sku),
      async markQueued(currentRunId, seq) {
        markQueuedStarted?.();
        await markQueuedGate;
        return items.markQueued(currentRunId, seq);
      },
    };
    const service = new ProcessMessageService(pausedRepository, queue);
    await startWorker();

    const processing = service.process({ run_id: runId, seq: 0, sku: 'sku-001' });
    await markQueuedHasStarted;
    const completed = await waitForItem(runId, 0, 'completed');
    releaseMarkQueued?.();
    await processing;

    expect(completed.attempts).toBe(1);
    await expect(items.markQueued(runId, 0)).resolves.toBe(false);
  });

  it("completes a job left with its item in 'received' after a simulated crash", async () => {
    const runId = 'run-crash-after-enqueue';
    await items.receiveItem(runId, 0, 'sku-001');
    await enqueueSku(queue, { runId, seq: 0, sku: 'sku-001' });

    await startWorker();

    await expect(waitForItem(runId, 0, 'completed')).resolves.toMatchObject({ attempts: 1 });
  });

  it.each(['received', 'queued', 'processing'] as const)(
    "retries a failed job when a duplicate finds its item in '%s'",
    async (status) => {
      const runId = `run-failed-job-recovery-${status}`;
      const jobData = { runId, seq: 0, sku: 'sku-001' };
      await items.receiveItem(runId, 0, jobData.sku);
      if (status === 'queued' || status === 'processing') {
        await items.markQueued(runId, 0);
      }
      if (status === 'processing') {
        await items.markProcessing(runId, 0);
      }
      await queue.add('enrich-sku', jobData, { jobId: `${runId}_0`, attempts: 1 });

      const failingRedis = createWorkerRedisConnection(TEST_REDIS_URL);
      const failingWorker = new Worker<SkuJobData, void>(
        SKU_QUEUE_NAME,
        () => Promise.reject(new Error('simulated worker crash')),
        { connection: failingRedis },
      );
      rawWorkers.push({ worker: failingWorker, redis: failingRedis });
      await failingWorker.waitUntilReady();
      const failedJob = await queue.getJob(`${runId}_0`);
      expect(failedJob).toBeDefined();
      const deadline = Date.now() + 5_000;
      while ((await failedJob?.getState()) !== 'failed' && Date.now() < deadline) {
        await delay(10);
      }
      expect(await failedJob?.getState()).toBe('failed');
      await failingWorker.close();
      await failingRedis.quit();
      rawWorkers = [];

      const service = new ProcessMessageService(items, queue);
      await service.process({ run_id: runId, seq: 0, sku: jobData.sku });
      await startWorker();

      await expect(waitForItem(runId, 0, 'completed')).resolves.toMatchObject({
        attempts: status === 'processing' ? 2 : 1,
      });
    },
  );

  it('persists an internal error when the worker exhausts its attempts', async () => {
    const runId = 'run-internal-error';
    const jobData = { runId, seq: 0, sku: 'sku-001' };
    await items.receiveItem(runId, 0, jobData.sku);
    await items.markQueued(runId, 0);
    await queue.add('enrich-sku', jobData, { jobId: `${runId}_0`, attempts: 2 });
    const failingRepository: ItemRepositoryPort = {
      getItem: (currentRunId, seq) => items.getItem(currentRunId, seq),
      markProcessing: (currentRunId, seq) => items.markProcessing(currentRunId, seq),
      completeItem: () => Promise.reject(new Error('simulated persistence failure')),
      failItem: (currentRunId, seq, data) => items.failItem(currentRunId, seq, data),
      recordRetry: (currentRunId, seq, data) => items.recordRetry(currentRunId, seq, data),
    };

    await startWorker(failingRepository);

    await expect(waitForItem(runId, 0, 'failed')).resolves.toMatchObject({
      attempts: 2,
      errorKind: 'transient',
      error: 'internal_error: simulated persistence failure',
    });
    const completedJob = await queue.getJob(`${runId}_0`);
    await expect(completedJob?.getState()).resolves.toBe('completed');
  });
});
