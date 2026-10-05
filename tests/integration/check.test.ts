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
