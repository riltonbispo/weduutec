import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { buildApp } from '../../src/app.js';
import { createSkuQueue } from '../../src/queues/sku.queue.js';
import { createTestRedisConnection } from '../helpers/redis.js';

const redis = createTestRedisConnection(
  process.env.TEST_HEALTH_REDIS_URL ?? 'redis://127.0.0.1:6379/13',
);
const queue = createSkuQueue(redis);
const app = buildApp({ redis, queue, logLevel: 'silent' });

beforeAll(async () => {
  await queue.waitUntilReady();
});

afterAll(async () => {
  await app.close();
  await queue.close();
  await redis.quit();
});

describe('GET /health', () => {
  it('returns 200 when the server is healthy', async () => {
    const response = await app.inject({ method: 'GET', url: '/health' });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ ok: true });
  });
});
