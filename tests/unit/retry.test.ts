import { describe, expect, it } from 'vitest';

import { EnrichError } from '../../src/domain/errors.js';
import { computeBackoffMs, decideRetry } from '../../src/lib/retry.js';

describe('computeBackoffMs', () => {
  it('uses deterministic full jitter within the exponential ceiling', () => {
    const random = () => 0.5;
    const delays = Array.from({ length: 6 }, (_, index) =>
      computeBackoffMs(index + 1, { baseMs: 500, maxMs: 8_000, random }),
    );

    expect(delays).toEqual([250, 500, 1_000, 2_000, 4_000, 4_000]);
    expect(computeBackoffMs(4, { baseMs: 500, maxMs: 8_000, random: () => 0 })).toBe(0);
    expect(computeBackoffMs(10, { baseMs: 500, maxMs: 8_000, random: () => 0.999 })).toBeLessThan(
      8_000,
    );
  });
});

describe('decideRetry', () => {
  it.each([1, 2, 3, 4])('retries a transient error after attempt %i', (attemptsMade) => {
    const error = new EnrichError({ kind: 'transient', statusCode: 500, message: 'upstream' });

    expect(
      decideRetry(error, { attemptsMade, rateLimitWaits: 0 }, { random: () => 0.5 }),
    ).toMatchObject({ action: 'retry' });
  });

  it('fails a transient error on the fifth attempt', () => {
    const error = new EnrichError({ kind: 'transient', statusCode: 500, message: 'upstream' });

    expect(decideRetry(error, { attemptsMade: 5, rateLimitWaits: 0 })).toEqual({
      action: 'fail',
      reason: 'max_attempts_exhausted',
    });
  });

  it('uses a separate counter for rate-limit waits and stops at its limit', () => {
    const error = new EnrichError({
      kind: 'rate_limited',
      statusCode: 429,
      retryAfterMs: 2_000,
      message: 'rate limited',
    });

    for (let rateLimitWaits = 0; rateLimitWaits < 20; rateLimitWaits += 1) {
      expect(decideRetry(error, { attemptsMade: 99, rateLimitWaits })).toEqual({
        action: 'rate_limit_wait',
        delayMs: 2_000,
      });
    }
    expect(decideRetry(error, { attemptsMade: 0, rateLimitWaits: 20 })).toEqual({
      action: 'fail',
      reason: 'max_rate_limit_waits_exhausted',
    });
  });

  it.each([401, 404])('fails status %i immediately', (statusCode) => {
    const error = new EnrichError({ kind: 'permanent', statusCode, message: 'permanent' });

    expect(decideRetry(error, { attemptsMade: 0, rateLimitWaits: 0 })).toEqual({
      action: 'fail',
      reason: 'permanent',
    });
  });
});
