import { afterAll, describe, expect, it } from 'vitest';

import { buildApp } from '../../src/app.js';

const app = buildApp();

afterAll(async () => {
  await app.close();
});

describe('GET /health', () => {
  it('returns 200 when the server is healthy', async () => {
    const response = await app.inject({ method: 'GET', url: '/health' });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ ok: true });
  });
});
