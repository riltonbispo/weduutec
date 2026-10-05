import type { Redis } from 'ioredis';
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

import type { CallbackPayload } from '../../src/clients/weduu.client.js';
import { EnrichError } from '../../src/domain/errors.js';
import { ItemRepository } from '../../src/repositories/item.repository.js';
import { RunRepository } from '../../src/repositories/run.repository.js';
import { CallbackService } from '../../src/services/callback.service.js';
import { RunFinalizer } from '../../src/services/run-finalizer.service.js';
import { createRunSweeper } from '../../src/workers/run-sweeper.js';
import { createTestRedisConnection } from '../helpers/redis.js';

const REDIS_URL = process.env.TEST_RUN_REDIS_URL ?? 'redis://127.0.0.1:6379/7';
const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() };
let redis: Redis;
let items: ItemRepository;
let runs: RunRepository;

beforeAll(async () => {
  redis = createTestRedisConnection(REDIS_URL);
  await redis.connect();
  items = new ItemRepository(redis);
  runs = new RunRepository(redis, 5);
});

beforeEach(async () => {
  await redis.flushdb();
  vi.clearAllMocks();
});

afterAll(async () => {
  await redis.quit();
});

async function complete(runId: string, seq: number): Promise<void> {
  await items.receiveItem(runId, seq, `sku-${String(seq + 1).padStart(3, '0')}`);
  await items.markQueued(runId, seq);
  await items.markProcessing(runId, seq);
  await items.completeItem(runId, seq, { price: 10 + seq, stock: seq });
}

function finalizer(
  sendCallback: (payload: CallbackPayload) => Promise<void>,
  now: () => number = Date.now,
): RunFinalizer {
  return new RunFinalizer({
    runs,
    callbackService: new CallbackService(runs, items, logger),
    client: { sendCallback },
    leaseMs: 100,
    backoffBaseMs: 10,
    random: () => 0,
    now,
    logger,
  });
}

describe('run finalization with Redis', () => {
  it('keeps the first registered total and reports later conflicts', async () => {
    await expect(runs.registerRun('run-conflict', 2, 1_000)).resolves.toEqual({
      conflict: false,
      total: 2,
    });
    await expect(runs.registerRun('run-conflict', 3, 2_000)).resolves.toEqual({
      conflict: true,
      total: 2,
    });
    await expect(runs.getRun('run-conflict')).resolves.toMatchObject({
      total: 2,
      registeredAt: 1_000,
    });
  });

  it('allows only one callback claim across ten concurrent finalizers', async () => {
    await runs.registerRun('run-race', 2, Date.now());
    await complete('run-race', 0);
    await complete('run-race', 1);
    const send = vi.fn(() => Promise.resolve());
    await Promise.all(Array.from({ length: 10 }, () => finalizer(send).finalize('run-race')));
    expect(send).toHaveBeenCalledOnce();
    await expect(runs.getRun('run-race')).resolves.toMatchObject({
      status: 'completed',
      callbackAttempts: 1,
    });
  });

  it('finalizes immediately when total is registered after all items completed', async () => {
    await complete('run-late', 0);
    await complete('run-late', 1);
    await expect(runs.getRun('run-late')).resolves.toMatchObject({ total: undefined });
    await runs.registerRun('run-late', 2, Date.now());
    const send = vi.fn(() => Promise.resolve());
    await expect(finalizer(send).finalize('run-late')).resolves.toBe('sent');
    expect(send).toHaveBeenCalledOnce();
  });

  it('retries a transient callback and completes on the second attempt', async () => {
    let currentTime = 1_000;
    await runs.registerRun('run-retry', 1, currentTime);
    await complete('run-retry', 0);
    const send = vi
      .fn<(payload: CallbackPayload) => Promise<void>>()
      .mockRejectedValueOnce(
        new EnrichError({ kind: 'transient', statusCode: 500, message: '500' }),
      )
      .mockResolvedValueOnce();
    const service = finalizer(send, () => currentTime);
    await expect(service.finalize('run-retry')).resolves.toBe('retry_scheduled');
    currentTime += 1;
    await expect(service.finalize('run-retry')).resolves.toBe('sent');
    expect(send).toHaveBeenCalledTimes(2);
    await expect(runs.getRun('run-retry')).resolves.toMatchObject({ callbackAttempts: 2 });
  });

  it('recovers an expired callback lease after a simulated crash', async () => {
    let currentTime = 5_000;
    await runs.registerRun('run-lease', 1, currentTime);
    await complete('run-lease', 0);
    await expect(runs.claimCallback('run-lease', currentTime, 100)).resolves.toMatchObject({
      claimed: true,
    });
    const send = vi.fn(() => Promise.resolve());
    await expect(finalizer(send, () => currentTime).finalize('run-lease')).resolves.toBe('busy');
    currentTime += 101;
    await expect(finalizer(send, () => currentTime).finalize('run-lease')).resolves.toBe('sent');
    expect(send).toHaveBeenCalledOnce();
  });

  it('marks a missing run stalled once, then a late item reopens and completes it', async () => {
    const registeredAt = Date.now();
    await runs.registerRun('run-stalled', 2, registeredAt);
    await complete('run-stalled', 0);
    const send = vi.fn(() => Promise.resolve());
    const sweeper = createRunSweeper({
      runs,
      finalizeRun: (runId) => finalizer(send).finalize(runId),
      intervalMs: 10_000,
      stallTimeoutMs: 1,
      now: () => Date.now() + 10_000,
      logger,
    });
    await sweeper.tick();
    await sweeper.tick();
    await expect(runs.getRun('run-stalled')).resolves.toMatchObject({ status: 'stalled' });
    expect(logger.error).toHaveBeenCalledTimes(1);
    expect(logger.error).toHaveBeenCalledWith(
      expect.objectContaining({ run_id: 'run-stalled', missing_seqs: [1] }),
      'run stalled with missing items',
    );
    await complete('run-stalled', 1);
    await expect(runs.getRun('run-stalled')).resolves.toMatchObject({ status: 'open' });
    await expect(finalizer(send).finalize('run-stalled')).resolves.toBe('sent');
  });

  it('omits a failed item while retaining its terminal diagnostics', async () => {
    await runs.registerRun('run-failed-item', 2, Date.now());
    await complete('run-failed-item', 0);
    await items.receiveItem('run-failed-item', 1, 'sku-002');
    await items.markQueued('run-failed-item', 1);
    await items.markProcessing('run-failed-item', 1);
    await items.failItem('run-failed-item', 1, {
      kind: 'permanent',
      statusCode: 404,
      message: 'not found',
      attempts: 1,
    });
    const send = vi.fn(() => Promise.resolve());
    await finalizer(send).finalize('run-failed-item');
    expect(send).toHaveBeenCalledWith({
      runId: 'run-failed-item',
      result: [{ seq: 0, sku: 'sku-001', price: 10, stock: 0 }],
    });
    await expect(items.getItem('run-failed-item', 1)).resolves.toMatchObject({
      status: 'failed',
      errorKind: 'permanent',
      statusCode: 404,
    });
  });
});
