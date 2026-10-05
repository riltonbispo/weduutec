import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { buildApp } from '../../src/app.js';
import { createRedisConnection } from '../../src/lib/redis.js';
import { createSkuQueue } from '../../src/queues/sku.queue.js';

const redis = createRedisConnection('redis://127.0.0.1:6379/15');
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
