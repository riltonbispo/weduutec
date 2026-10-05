import { describe, expect, it, vi } from 'vitest';

import type { ItemRecord } from '../../src/repositories/item.repository.js';
import { CallbackService } from '../../src/services/callback.service.js';

function item(seq: number, status: 'completed' | 'failed'): ItemRecord {
  const base: ItemRecord = {
    runId: 'run-1',
    seq,
    sku: `sku-${String(seq + 1).padStart(3, '0')}`,
    status,
    receivedAt: '2026-01-01T00:00:00.000Z',
    attempts: 1,
    rateLimitWaits: 0,
  };
  if (status === 'completed') {
    return { ...base, price: 10 + seq, stock: seq, completedAt: '2026-01-01T00:00:01.000Z' };
  }
  return { ...base, error: 'not found', errorKind: 'permanent', statusCode: 404 };
}

describe('CallbackService', () => {
  it('builds an ordered payload with completed items only', async () => {
    const warn = vi.fn();
    const service = new CallbackService(
      {
        getRun: () =>
          Promise.resolve({ runId: 'run-1', total: 3, status: 'sending', callbackAttempts: 1 }),
      },
      {
        getItems: () =>
          Promise.resolve([item(0, 'completed'), item(1, 'failed'), item(2, 'completed')]),
        listRunItemSeqs: () => Promise.resolve([0, 1, 2, 4]),
      },
      { warn },
    );

    await expect(service.buildCallbackPayload('run-1')).resolves.toEqual({
      runId: 'run-1',
      result: [
        { seq: 0, sku: 'sku-001', price: 10, stock: 0 },
        { seq: 2, sku: 'sku-003', price: 12, stock: 2 },
      ],
    });
    expect(service.getFailedCount('run-1')).toBe(1);
    expect(warn).toHaveBeenCalledTimes(2);
  });
});
