import { describe, expect, it } from 'vitest';

import { loadEnv, loadWorkerEnv } from '../../src/config/env.js';

describe('environment configuration', () => {
  it('keeps Weduu credentials optional for the HTTP server', () => {
    const env = loadEnv({});

    expect(env.ENRICH_TIMEOUT_MS).toBe(5_000);
    expect(env.ENRICH_MAX_ATTEMPTS).toBe(5);
    expect(env.ENRICH_BACKOFF_BASE_MS).toBe(500);
    expect(env.ENRICH_BACKOFF_MAX_MS).toBe(8_000);
    expect(env.ENRICH_RATE_LIMIT_MAX_WAITS).toBe(20);
    expect(env.WEDUU_TOKEN).toBeUndefined();
  });

  it('requires Weduu credentials for the worker configuration', () => {
    expect(() => loadWorkerEnv({})).toThrow(/WEDUU_BASE_URL.*WEDUU_CID.*WEDUU_TOKEN/);

    expect(
      loadWorkerEnv({
        WEDUU_BASE_URL: 'https://platform.example.test',
        WEDUU_CID: 'cid-test',
        WEDUU_TOKEN: 'secret-test',
      }),
    ).toMatchObject({
      WEDUU_BASE_URL: 'https://platform.example.test',
      WEDUU_CID: 'cid-test',
      WEDUU_TOKEN: 'secret-test',
    });
  });
});
