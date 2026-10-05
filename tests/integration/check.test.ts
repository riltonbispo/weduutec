import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { buildApp } from '../../src/app.js';
import { createSkuQueue } from '../../src/queues/sku.queue.js';
import { createTestRedisConnection } from '../helpers/redis.js';

const redis = createTestRedisConnection(
  process.env.TEST_CHECK_REDIS_URL ?? 'redis://127.0.0.1:6379/12',
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

describe('POST /check', () => {
  it('echoes the received token', async () => {
    const response = await app.inject({
      method: 'POST',
      url: '/check',
      payload: { token: 'a3f9-secret-token' },
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ token: 'a3f9-secret-token' });
  });

  it.each([
    ['an empty body', {}],
    ['an empty token', { token: '' }],
    ['a non-string token', { token: 123 }],
  ])('rejects %s', async (_case, payload) => {
    const response = await app.inject({
      method: 'POST',
      url: '/check',
      payload,
    });

    expect(response.statusCode).toBe(400);
    expect(response.json()).toEqual({ error: 'invalid_payload' });
  });
});
