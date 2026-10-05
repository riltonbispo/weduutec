import type { FastifyInstance } from 'fastify';
import type { Redis } from 'ioredis';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod';

import { createMockPlatform, expectedEnrichment, skuForSeq } from '../../scripts/mock-platform.js';
import type { MockPlatform } from '../../scripts/mock-platform.js';
import { buildApp } from '../../src/app.js';
import { WeduuClient } from '../../src/clients/weduu.client.js';
import { createWorkerRedisConnection } from '../../src/lib/redis.js';
import { createSkuQueue, setSkuQueueGlobalConcurrency } from '../../src/queues/sku.queue.js';
import type { SkuQueue } from '../../src/queues/sku.queue.js';
import { ItemRepository } from '../../src/repositories/item.repository.js';
import type { ItemRecord } from '../../src/repositories/item.repository.js';
import { RunRepository } from '../../src/repositories/run.repository.js';
import { CallbackService } from '../../src/services/callback.service.js';
import { RunFinalizer } from '../../src/services/run-finalizer.service.js';
import { createSkuWorker } from '../../src/workers/sku.worker.js';
import type { SkuWorker } from '../../src/workers/sku.worker.js';
import { createTestRedisConnection } from '../helpers/redis.js';

const TEST_REDIS_URL = process.env.TEST_WORKER_REDIS_URL ?? 'redis://127.0.0.1:6379/10';
const TOTAL = 20;
const credentialsSchema = z.object({ cid: z.string(), token: z.string() });
const runSchema = z.object({ run_id: z.string(), total: z.number().int() });

interface WorkerHarness {
  worker: SkuWorker;
  workerRedis: Redis;
  repositoryRedis: Redis;
  closed: boolean;
}

interface BurstResult {
  platform: MockPlatform;
  platformUrl: string;
  cid: string;
  token: string;
  runId: string;
}

let apiRedis: Redis;
let queue: SkuQueue;
let app: FastifyInstance;
let appUrl: string;
let itemRepository: ItemRepository;
let platforms: MockPlatform[] = [];
let workers: WorkerHarness[] = [];

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

beforeAll(async () => {
  apiRedis = createTestRedisConnection(TEST_REDIS_URL);
  await apiRedis.connect();
  queue = createSkuQueue(apiRedis, { maxAttempts: 5 });
  itemRepository = new ItemRepository(apiRedis);
  app = buildApp({ redis: apiRedis, queue, logLevel: 'silent' });
  await app.listen({ port: 0, host: '127.0.0.1' });
  const address = app.server.address();
  if (address === null || typeof address === 'string') {
    throw new Error('Worker integration API did not bind to a TCP port');
  }
  appUrl = `http://127.0.0.1:${String(address.port)}`;
});

beforeEach(async () => {
  await apiRedis.flushdb();
  platforms = [];
  workers = [];
});

async function closeWorker(harness: WorkerHarness): Promise<void> {
  if (harness.closed) {
    return;
  }
  harness.closed = true;
  await harness.worker.close();
  await Promise.all([harness.workerRedis.quit(), harness.repositoryRedis.quit()]);
}

afterEach(async () => {
  await Promise.all(
    workers.map(async (harness) => {
      await closeWorker(harness);
    }),
  );
  await Promise.all(
    platforms.map(async (platform) => {
      await platform.stop();
    }),
  );
});

afterAll(async () => {
  await app.close();
  await queue.close();
  await apiRedis.quit();
});

async function dispatchBurst(options: {
  errorRate: number;
  invalidSkuSeqs?: Iterable<number>;
}): Promise<BurstResult> {
  const platform = createMockPlatform({
    port: 0,
    total: TOTAL,
    dupRate: 0,
    errorRate: options.errorRate,
    invalidSkuSeqs: options.invalidSkuSeqs,
    seed: 42,
    printReports: false,
  });
  platforms.push(platform);
  const platformUrl = await platform.start();
  const registrationResponse = await fetch(`${platformUrl}/register`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ name: 'Worker integration', webhook: appUrl }),
  });
  const credentials = credentialsSchema.parse(await registrationResponse.json());
  const burstResponse = await fetch(`${platformUrl}/burst/${credentials.cid}`, {
    method: 'POST',
    headers: { 'x-token': credentials.token },
  });
  const run = runSchema.parse(await burstResponse.json());
  await platform.waitForDispatch(run.run_id);

  expect(registrationResponse.status).toBe(200);
  expect(burstResponse.status).toBe(200);
  expect(run.total).toBe(TOTAL);

  return {
    platform,
    platformUrl,
    cid: credentials.cid,
    token: credentials.token,
    runId: run.run_id,
  };
}

async function startWorker(burst: BurstResult): Promise<WorkerHarness> {
  await setSkuQueueGlobalConcurrency(queue);
  const workerRedis = createWorkerRedisConnection(TEST_REDIS_URL);
  const repositoryRedis = createWorkerRedisConnection(TEST_REDIS_URL);
  const worker = createSkuWorker({
    connection: workerRedis,
    client: new WeduuClient({
      baseUrl: burst.platformUrl,
      cid: burst.cid,
      token: burst.token,
      timeoutMs: 2_000,
      maxInFlight: 3,
    }),
    itemRepository: new ItemRepository(repositoryRedis),
    policy: { maxAttempts: 5, maxRateLimitWaits: 20, baseMs: 1, maxMs: 5 },
    random: () => 0,
    logger: { info: vi.fn() },
    lockDurationMs: 10_000,
    finalizeRun: () => Promise.resolve(),
  });
  const errors: unknown[] = [];
  worker.on('error', (error) => {
    errors.push(error);
  });
  await worker.waitUntilReady();

  const harness = { worker, workerRedis, repositoryRedis, closed: false };
  workers.push(harness);
  return harness;
}

async function startFinalizingWorker(options: {
  platformUrl: string;
  cid: string;
  token: string;
}): Promise<{ harness: WorkerHarness; runs: RunRepository; finalizer: RunFinalizer }> {
  await setSkuQueueGlobalConcurrency(queue);
  const workerRedis = createWorkerRedisConnection(TEST_REDIS_URL);
  const repositoryRedis = createWorkerRedisConnection(TEST_REDIS_URL);
  const items = new ItemRepository(repositoryRedis);
  const runs = new RunRepository(repositoryRedis, 5);
  const client = new WeduuClient({
    baseUrl: options.platformUrl,
    cid: options.cid,
    token: options.token,
    timeoutMs: 2_000,
    callbackTimeoutMs: 2_000,
    maxInFlight: 3,
  });
  const callbackLogger = { info: vi.fn(), warn: vi.fn(), error: vi.fn() };
  const callbackService = new CallbackService(runs, items, callbackLogger);
  const finalizer = new RunFinalizer({
    runs,
    callbackService,
    client,
    leaseMs: 5_000,
    backoffBaseMs: 5,
    random: () => 0,
    logger: callbackLogger,
  });
  const worker = createSkuWorker({
    connection: workerRedis,
    client,
    itemRepository: items,
    policy: { maxAttempts: 5, maxRateLimitWaits: 20, baseMs: 1, maxMs: 5 },
    random: () => 0,
    logger: { info: vi.fn() },
    lockDurationMs: 10_000,
    finalizeRun: (runId) => finalizer.finalize(runId),
  });
  await worker.waitUntilReady();
  const harness = { worker, workerRedis, repositoryRedis, closed: false };
  workers.push(harness);
  return { harness, runs, finalizer };
}

async function waitForReport(platform: MockPlatform, runId: string): Promise<void> {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    const report = platform.reports.find(
      (candidate) => candidate.run_id === runId && candidate.callbacks_for_run > 0,
    );
    if (report !== undefined) return;
    await delay(50);
  }
  throw new Error(`Timed out waiting for callback report from run ${runId}`);
}

async function waitForItems(
  runId: string,
  predicate: (items: ItemRecord[]) => boolean,
  timeoutMs = 30_000,
): Promise<ItemRecord[]> {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    const items = await Promise.all(
      Array.from({ length: TOTAL }, async (_, seq) => await itemRepository.getItem(runId, seq)),
    );
    if (items.every((item): item is ItemRecord => item !== null) && predicate(items)) {
      return items;
    }
    await delay(50);
  }

  throw new Error(`Timed out waiting for items from run ${runId}`);
}

describe('real SKU worker', () => {
  it('retries transient failures and completes all deterministic enrichments', async () => {
    const burst = await dispatchBurst({ errorRate: 0.3 });
    await startWorker(burst);

    const items = await waitForItems(burst.runId, (currentItems) =>
      currentItems.every((item) => item.status === 'completed'),
    );

    for (const item of items) {
      const expected = expectedEnrichment(skuForSeq(item.seq));
      expect(expected).not.toBeNull();
      expect(item).toMatchObject({
        status: 'completed',
        sku: skuForSeq(item.seq),
        price: expected?.price,
        stock: expected?.stock,
      });
    }
    expect(items.some((item) => item.attempts > 1)).toBe(true);
    expect(burst.platform.metrics.enrichByStatus[500]).toBeGreaterThan(0);
    expect(burst.platform.metrics.enrichByStatus[429]).toBe(0);
    expect(burst.platform.metrics.peakEnrichInFlight).toBeLessThanOrEqual(3);
  }, 30_000);

  it('persists an invalid SKU as a permanent 404 failure', async () => {
    const burst = await dispatchBurst({ errorRate: 0, invalidSkuSeqs: [3] });
    await startWorker(burst);

    const items = await waitForItems(burst.runId, (currentItems) =>
      currentItems.every((item) => item.status === 'completed' || item.status === 'failed'),
    );

    expect(items[3]).toMatchObject({
      status: 'failed',
      errorKind: 'permanent',
      statusCode: 404,
      attempts: 1,
    });
    expect(items.filter((item) => item.status === 'completed')).toHaveLength(TOTAL - 1);
  }, 30_000);

  it('resumes queued jobs with a new worker without duplicating enrich effects', async () => {
    const burst = await dispatchBurst({ errorRate: 0 });
    const firstWorker = await startWorker(burst);
    await waitForItems(
      burst.runId,
      (items) => items.some((item) => item.status === 'completed'),
      10_000,
    );
    await closeWorker(firstWorker);

    const itemsAfterStop = await Promise.all(
      Array.from(
        { length: TOTAL },
        async (_, seq) => await itemRepository.getItem(burst.runId, seq),
      ),
    );
    expect(itemsAfterStop.filter((item) => item?.status === 'completed').length).toBeLessThan(
      TOTAL,
    );

    await startWorker(burst);
    const completedItems = await waitForItems(burst.runId, (items) =>
      items.every((item) => item.status === 'completed'),
    );

    expect(completedItems.every((item) => item.attempts === 1)).toBe(true);
    expect(burst.platform.metrics.enrichByStatus[200]).toBe(TOTAL);
  }, 30_000);
});

describe.each([false, true])('end-to-end callback (early dispatch: %s)', (earlyDispatch) => {
  it('produces a passing platform report', async () => {
    const platform = createMockPlatform({
      port: 0,
      total: TOTAL,
      dupRate: 0.25,
      errorRate: 0.1,
      seed: 42,
      earlyDispatch,
      printReports: false,
    });
    platforms.push(platform);
    const platformUrl = await platform.start();
    const registrationResponse = await fetch(`${platformUrl}/register`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ name: 'End-to-end integration', webhook: appUrl }),
    });
    const credentials = credentialsSchema.parse(await registrationResponse.json());
    const runtime = await startFinalizingWorker({ platformUrl, ...credentials });
    const burstResponse = await fetch(`${platformUrl}/burst/${credentials.cid}`, {
      method: 'POST',
      headers: { 'x-token': credentials.token },
    });
    const run = runSchema.parse(await burstResponse.json());
    const registered = await runtime.runs.registerRun(run.run_id, run.total, Date.now());
    expect(registered.conflict).toBe(false);
    await runtime.finalizer.finalize(run.run_id);
    await platform.waitForDispatch(run.run_id);
    await waitForReport(platform, run.run_id);

    const report = [...platform.reports]
      .reverse()
      .find((candidate) => candidate.run_id === run.run_id && candidate.callbacks_for_run > 0);
    expect(report?.verdict).toEqual({ status: 'PASS', reasons: [] });
    expect(report?.callback.items_received).toBe(TOTAL);
    expect(report?.enrich.peak_in_flight).toBeLessThanOrEqual(3);
    expect(report?.ack.above_600_ms).toBe(0);
  }, 30_000);
});
