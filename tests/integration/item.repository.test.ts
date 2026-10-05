import type { Redis } from 'ioredis';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import { ItemRepository } from '../../src/repositories/item.repository.js';
import { createTestRedisConnection } from '../helpers/redis.js';

const TEST_REDIS_URL =
  process.env.TEST_ITEM_REDIS_URL ?? 'redis://127.0.0.1:6379/9';

let redis: Redis;
let repository: ItemRepository;

beforeAll(async () => {
  redis = createTestRedisConnection(TEST_REDIS_URL);
  await redis.connect();
  repository = new ItemRepository(redis);
});

beforeEach(async () => {
  await redis.flushdb();
});

afterAll(async () => {
  await redis.quit();
});

describe('ItemRepository worker transitions', () => {
  it('records retries and completes an item idempotently', async () => {
    await repository.receiveItem('run-complete', 0, 'sku-001');
    await repository.markQueued('run-complete', 0);

    await expect(repository.markProcessing('run-complete', 0)).resolves.toBe(true);
    await expect(
      repository.recordRetry('run-complete', 0, {
        attempts: 1,
        rateLimitWaits: 0,
        lastError: { kind: 'transient', statusCode: 500, message: 'upstream_error' },
      }),
    ).resolves.toBe(true);
    await expect(repository.markProcessing('run-complete', 0)).resolves.toBe(true);
    await expect(
      repository.completeItem('run-complete', 0, { price: 12.5, stock: 7 }),
    ).resolves.toBe(true);
    await expect(
      repository.completeItem('run-complete', 0, { price: 999, stock: 999 }),
    ).resolves.toBe(true);

    await expect(repository.getItem('run-complete', 0)).resolves.toMatchObject({
      status: 'completed',
      price: 12.5,
      stock: 7,
      attempts: 2,
      rateLimitWaits: 0,
      lastError: { kind: 'transient', statusCode: 500, message: 'upstream_error' },
    });
    await expect(repository.markProcessing('run-complete', 0)).resolves.toBe(false);
    await expect(
      repository.failItem('run-complete', 0, {
        kind: 'permanent',
        message: 'must not overwrite completion',
        attempts: 3,
      }),
    ).resolves.toBe(false);
  });

  it('persists a permanent failure and protects the terminal state', async () => {
    await repository.receiveItem('run-failed', 3, 'sku-004');
    await repository.markQueued('run-failed', 3);
    await repository.markProcessing('run-failed', 3);

    await expect(
      repository.failItem('run-failed', 3, {
        kind: 'permanent',
        statusCode: 404,
        message: 'permanent_http_error',
        attempts: 1,
      }),
    ).resolves.toBe(true);

    await expect(repository.getItem('run-failed', 3)).resolves.toMatchObject({
      status: 'failed',
      error: 'permanent_http_error',
      errorKind: 'permanent',
      statusCode: 404,
      attempts: 1,
    });
    await expect(repository.markProcessing('run-failed', 3)).resolves.toBe(false);
    await expect(
      repository.recordRetry('run-failed', 3, {
        attempts: 2,
        rateLimitWaits: 0,
        lastError: { kind: 'transient', message: 'must not overwrite failure' },
      }),
    ).resolves.toBe(false);
  });
});
