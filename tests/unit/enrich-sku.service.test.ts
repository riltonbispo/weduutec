import { describe, expect, it, vi } from 'vitest';

import { EnrichError } from '../../src/domain/errors.js';
import type { ItemRecord } from '../../src/repositories/item.repository.js';
import { processSkuJob } from '../../src/services/enrich-sku.service.js';
import type {
  EnrichClientPort,
  ItemRepositoryPort,
  ProcessSkuJobDependencies,
} from '../../src/services/enrich-sku.service.js';

const JOB = { data: { runId: 'run-unit', seq: 3, sku: 'sku-004' } };

function item(overrides: Partial<ItemRecord> = {}): ItemRecord {
  return {
    runId: JOB.data.runId,
    seq: JOB.data.seq,
    sku: JOB.data.sku,
    status: 'queued',
    receivedAt: '2026-01-01T00:00:00.000Z',
    attempts: 0,
    rateLimitWaits: 0,
    ...overrides,
  };
}

function createDependencies(currentItem = item()): ProcessSkuJobDependencies {
  const client: EnrichClientPort = {
    enrich: vi.fn(() =>
      Promise.resolve({ sku: JOB.data.sku, price: 19.99, stock: 8 }),
    ),
  };
  const itemRepository: ItemRepositoryPort = {
    getItem: vi.fn(() => Promise.resolve(currentItem)),
    markProcessing: vi.fn(() => Promise.resolve(true)),
    completeItem: vi.fn(() => Promise.resolve(true)),
    failItem: vi.fn(() => Promise.resolve(true)),
    recordRetry: vi.fn(() => Promise.resolve(true)),
  };

  return {
    client,
    itemRepository,
    policy: { maxAttempts: 5, maxRateLimitWaits: 20, baseMs: 500, maxMs: 8_000 },
    random: () => 0.5,
    rateLimit: vi.fn(() => Promise.resolve()),
    logger: { info: vi.fn() },
  };
}

describe('processSkuJob', () => {
  it('completes a successfully enriched item', async () => {
    const deps = createDependencies();

    await processSkuJob(JOB, deps);

    expect(deps.itemRepository.markProcessing).toHaveBeenCalledWith('run-unit', 3);
    expect(deps.client.enrich).toHaveBeenCalledWith('sku-004');
    expect(deps.itemRepository.completeItem).toHaveBeenCalledWith('run-unit', 3, {
      sku: 'sku-004',
      price: 19.99,
      stock: 8,
    });
  });

  it('records a transient retry and throws for BullMQ', async () => {
    const deps = createDependencies();
    const error = new EnrichError({
      kind: 'transient',
      statusCode: 500,
      message: 'upstream_error',
    });
    vi.mocked(deps.client.enrich).mockRejectedValue(error);

    await expect(processSkuJob(JOB, deps)).rejects.toBe(error);

    expect(deps.itemRepository.recordRetry).toHaveBeenCalledWith('run-unit', 3, {
      attempts: 1,
      rateLimitWaits: 0,
      lastError: { kind: 'transient', statusCode: 500, message: 'upstream_error' },
    });
    expect(deps.itemRepository.failItem).not.toHaveBeenCalled();
  });

  it('persists the fifth transient failure and returns normally', async () => {
    const deps = createDependencies(item({ attempts: 4 }));
    vi.mocked(deps.client.enrich).mockRejectedValue(
      new EnrichError({ kind: 'transient', statusCode: 500, message: 'upstream_error' }),
    );

    await expect(processSkuJob(JOB, deps)).resolves.toBeUndefined();

    expect(deps.itemRepository.failItem).toHaveBeenCalledWith('run-unit', 3, {
      kind: 'transient',
      statusCode: 500,
      message: 'upstream_error',
      attempts: 5,
    });
    expect(deps.itemRepository.recordRetry).not.toHaveBeenCalled();
  });

  it('applies a BullMQ rate limit without consuming an attempt', async () => {
    const deps = createDependencies(item({ attempts: 2, rateLimitWaits: 4 }));
    vi.mocked(deps.client.enrich).mockRejectedValue(
      new EnrichError({
        kind: 'rate_limited',
        statusCode: 429,
        retryAfterMs: 2_000,
        message: 'rate_limited',
      }),
    );

    await expect(processSkuJob(JOB, deps)).rejects.toMatchObject({
      message: 'bullmq:rateLimitExceeded',
    });

    expect(deps.itemRepository.recordRetry).toHaveBeenCalledWith('run-unit', 3, {
      attempts: 2,
      rateLimitWaits: 5,
      lastError: { kind: 'rate_limited', statusCode: 429, message: 'rate_limited' },
    });
    expect(deps.rateLimit).toHaveBeenCalledWith(2_000);
  });

  it('fails after exhausting rate-limit waits', async () => {
    const deps = createDependencies(item({ attempts: 2, rateLimitWaits: 20 }));
    vi.mocked(deps.client.enrich).mockRejectedValue(
      new EnrichError({
        kind: 'rate_limited',
        statusCode: 429,
        retryAfterMs: 2_000,
        message: 'rate_limited',
      }),
    );

    await expect(processSkuJob(JOB, deps)).resolves.toBeUndefined();

    expect(deps.itemRepository.failItem).toHaveBeenCalledWith('run-unit', 3, {
      kind: 'rate_limited',
      statusCode: 429,
      message: 'rate_limited',
      attempts: 2,
    });
    expect(deps.rateLimit).not.toHaveBeenCalled();
  });

  it.each([401, 404])('fails status %i immediately', async (statusCode) => {
    const deps = createDependencies();
    vi.mocked(deps.client.enrich).mockRejectedValue(
      new EnrichError({ kind: 'permanent', statusCode, message: 'permanent_http_error' }),
    );

    await expect(processSkuJob(JOB, deps)).resolves.toBeUndefined();

    expect(deps.itemRepository.failItem).toHaveBeenCalledWith('run-unit', 3, {
      kind: 'permanent',
      statusCode,
      message: 'permanent_http_error',
      attempts: 1,
    });
    expect(deps.itemRepository.recordRetry).not.toHaveBeenCalled();
  });

  it.each(['completed', 'failed'] as const)(
    'skips an item already in terminal status %s',
    async (status) => {
      const deps = createDependencies(item({ status }));

      await processSkuJob(JOB, deps);

      expect(deps.itemRepository.markProcessing).not.toHaveBeenCalled();
      expect(deps.client.enrich).not.toHaveBeenCalled();
    },
  );

  it('propagates a completion persistence error without marking the item failed', async () => {
    const deps = createDependencies();
    const persistenceError = new Error('Redis write failed');
    vi.mocked(deps.itemRepository.completeItem).mockRejectedValue(persistenceError);

    await expect(processSkuJob(JOB, deps)).rejects.toBe(persistenceError);

    expect(deps.itemRepository.failItem).not.toHaveBeenCalled();
    expect(deps.itemRepository.recordRetry).not.toHaveBeenCalled();
  });
});
