import type { FastifyInstance } from 'fastify';
import type { Redis } from 'ioredis';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import { buildApp } from '../../src/app.js';
import { createRedisConnection } from '../../src/lib/redis.js';
import { createSkuQueue } from '../../src/queues/sku.queue.js';
import type { SkuQueue } from '../../src/queues/sku.queue.js';
import { ItemRepository } from '../../src/repositories/item.repository.js';

const TEST_REDIS_URL = process.env.TEST_REDIS_URL ?? 'redis://127.0.0.1:6379/15';

let app: FastifyInstance;
let redis: Redis;
let queue: SkuQueue;
let itemRepository: ItemRepository;

beforeAll(async () => {
  redis = createRedisConnection(TEST_REDIS_URL);
  await redis.connect();
  queue = createSkuQueue(redis);
  itemRepository = new ItemRepository(redis);
  app = buildApp({
    redis,
    queue,
    logLevel: 'silent',
  });
  await app.ready();
});

beforeEach(async () => {
  await redis.flushdb();
});

afterAll(async () => {
  await app.close();
  await queue.close();
  await redis.quit();
});

describe('POST /process', () => {
  it('accepts a valid payload', async () => {
    const response = await app.inject({
      method: 'POST',
      url: '/process',
      payload: { run_id: 'run-valid', seq: 0, sku: 'sku-001' },
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ ok: true });
    await expect(itemRepository.getStatus('run-valid', 0)).resolves.toBe('queued');
  });

  it.each([
    ['empty body', {}],
    ['empty run_id', { run_id: '', seq: 0, sku: 'sku-001' }],
    ['negative seq', { run_id: 'run-invalid', seq: -1, sku: 'sku-001' }],
    ['non-integer seq', { run_id: 'run-invalid', seq: 1.5, sku: 'sku-001' }],
    ['empty sku', { run_id: 'run-invalid', seq: 0, sku: '' }],
  ])('rejects an invalid payload: %s', async (_case, payload) => {
    const response = await app.inject({ method: 'POST', url: '/process', payload });

    expect(response.statusCode).toBe(400);
    expect(response.json()).toEqual({ error: 'invalid_payload' });
  });

  it('creates one item and one job for 10 concurrent duplicate messages', async () => {
    const request = {
      method: 'POST' as const,
      url: '/process',
      payload: { run_id: 'run-duplicate', seq: 4, sku: 'sku-004' },
    };

    const responses = await Promise.all(
      Array.from({ length: 10 }, async () => await app.inject(request)),
    );

    expect(responses.map((response) => response.statusCode)).toEqual(Array<number>(10).fill(200));
    await expect(itemRepository.getStatus('run-duplicate', 4)).resolves.toBe('queued');
    await expect(redis.keys('item:run-duplicate:*')).resolves.toHaveLength(1);
    await expect(queue.getJobCounts('waiting')).resolves.toMatchObject({ waiting: 1 });
  });

  it('creates 20 items and jobs for 20 sequences with 3 duplicates each', async () => {
    const requests = Array.from({ length: 20 }, (_, seq) =>
      Array.from(
        { length: 3 },
        async () =>
          await app.inject({
            method: 'POST',
            url: '/process',
            payload: { run_id: 'run-concurrent', seq, sku: `sku-${String(seq)}` },
          }),
      ),
    ).flat();

    const responses = await Promise.all(requests);

    expect(responses).toHaveLength(60);
    expect(responses.every((response) => response.statusCode === 200)).toBe(true);
    await expect(redis.keys('item:run-concurrent:*')).resolves.toHaveLength(20);
    await expect(queue.getJobCounts('waiting')).resolves.toMatchObject({ waiting: 20 });
  });

  it('accepts messages out of order', async () => {
    const responses = [];

    for (const seq of [2, 0, 1]) {
      responses.push(
        await app.inject({
          method: 'POST',
          url: '/process',
          payload: { run_id: 'run-unordered', seq, sku: `sku-00${String(seq)}` },
        }),
      );
    }

    expect(responses.map((response) => response.statusCode)).toEqual([200, 200, 200]);
    await expect(queue.getJobCounts('waiting')).resolves.toMatchObject({ waiting: 3 });
  });

  it("re-enqueues a duplicate left in 'received' state", async () => {
    const received = await itemRepository.receiveItem('run-recovery', 7, 'sku-007');
    expect(received).toEqual({ created: true, status: 'received' });

    const response = await app.inject({
      method: 'POST',
      url: '/process',
      payload: { run_id: 'run-recovery', seq: 7, sku: 'sku-007' },
    });

    expect(response.statusCode).toBe(200);
    await expect(itemRepository.getStatus('run-recovery', 7)).resolves.toBe('queued');
    await expect(queue.getJobCounts('waiting')).resolves.toMatchObject({ waiting: 1 });
  });

  it('returns 503 quickly when Redis is unavailable', async () => {
    const unavailableRedis = createRedisConnection('redis://127.0.0.1:1');
    const unavailableQueue = createSkuQueue(unavailableRedis);
    const unavailableApp = buildApp({
      redis: unavailableRedis,
      queue: unavailableQueue,
      logLevel: 'silent',
    });
    const startedAt = performance.now();

    const response = await unavailableApp.inject({
      method: 'POST',
      url: '/process',
      payload: { run_id: 'run-unavailable', seq: 0, sku: 'sku-001' },
    });
    const durationMs = performance.now() - startedAt;

    expect(response.statusCode).toBe(503);
    expect(response.json()).toEqual({ error: 'service_unavailable' });
    expect(durationMs).toBeLessThan(200);
    await unavailableApp.close();
    unavailableRedis.disconnect();
  });

  it('keeps the sequential ACK p95 below 100 ms locally', async () => {
    const durations: number[] = [];

    for (let seq = 0; seq < 50; seq += 1) {
      const startedAt = performance.now();
      const response = await app.inject({
        method: 'POST',
        url: '/process',
        payload: { run_id: 'run-latency', seq, sku: `sku-${String(seq)}` },
      });

      durations.push(performance.now() - startedAt);
      expect(response.statusCode).toBe(200);
    }

    durations.sort((left, right) => left - right);
    const p95Index = Math.ceil(durations.length * 0.95) - 1;
    const p95 = durations[p95Index];

    expect(p95).toBeDefined();
    expect(p95).toBeLessThan(100);
  });
});
