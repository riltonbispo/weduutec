import { afterAll, describe, expect, it } from 'vitest';

import { buildApp } from '../../src/app.js';

const app = buildApp({ logger: false });

afterAll(async () => {
  await app.close();
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
