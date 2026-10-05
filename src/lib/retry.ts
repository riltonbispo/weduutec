import type { EnrichError } from '../domain/errors.js';

const DEFAULT_MAX_ATTEMPTS = 5;
const DEFAULT_MAX_RATE_LIMIT_WAITS = 20;
const DEFAULT_BACKOFF_BASE_MS = 500;
const DEFAULT_BACKOFF_MAX_MS = 8_000;
const DEFAULT_RATE_LIMIT_WAIT_MS = 1_000;

interface BackoffOptions {
  baseMs: number;
  maxMs: number;
  random: () => number;
}

export interface RetryCounters {
  attemptsMade: number;
  rateLimitWaits: number;
}

export interface RetryPolicy {
  maxAttempts?: number;
  maxRateLimitWaits?: number;
  baseMs?: number;
  maxMs?: number;
  random?: () => number;
}

export type RetryDecision =
  | { action: 'retry'; delayMs: number }
  | { action: 'rate_limit_wait'; delayMs: number }
  | {
      action: 'fail';
      reason: 'permanent' | 'max_attempts_exhausted' | 'max_rate_limit_waits_exhausted';
    };

export function computeBackoffMs(attempt: number, options: BackoffOptions): number {
  const ceiling = Math.min(options.maxMs, options.baseMs * 2 ** (attempt - 1));
  return Math.floor(options.random() * ceiling);
}

export function decideRetry(
  error: EnrichError,
  counters: RetryCounters,
  policy: RetryPolicy = {},
): RetryDecision {
  if (error.kind === 'permanent') {
    return { action: 'fail', reason: 'permanent' };
  }

  if (error.kind === 'rate_limited') {
    const maxRateLimitWaits = policy.maxRateLimitWaits ?? DEFAULT_MAX_RATE_LIMIT_WAITS;
    if (counters.rateLimitWaits >= maxRateLimitWaits) {
      return { action: 'fail', reason: 'max_rate_limit_waits_exhausted' };
    }

    return {
      action: 'rate_limit_wait',
      delayMs: error.retryAfterMs ?? DEFAULT_RATE_LIMIT_WAIT_MS,
    };
  }

  const maxAttempts = policy.maxAttempts ?? DEFAULT_MAX_ATTEMPTS;
  if (counters.attemptsMade >= maxAttempts) {
    return { action: 'fail', reason: 'max_attempts_exhausted' };
  }

  return {
    action: 'retry',
    delayMs: computeBackoffMs(counters.attemptsMade, {
      baseMs: policy.baseMs ?? DEFAULT_BACKOFF_BASE_MS,
      maxMs: policy.maxMs ?? DEFAULT_BACKOFF_MAX_MS,
      random: policy.random ?? Math.random,
    }),
  };
}
