import { describe, expect, it, vi } from 'vitest';

import type { SkuQueueWriter } from '../../src/queues/sku.queue.js';
import {
  type ProcessItemRepository,
  ProcessMessageService,
  ProcessMessageUnavailableError,
} from '../../src/services/process-message.service.js';

const MESSAGE = { run_id: 'run-unit', seq: 0, sku: 'sku-001' };

class RedisConnectionError extends Error {
  readonly code = 'ECONNREFUSED';
}

function createServiceWithReceiveError(error: Error): ProcessMessageService {
  const repository: ProcessItemRepository = {
    receiveItem: vi.fn(() => Promise.reject(error)),
    markQueued: vi.fn(() => Promise.resolve(true)),
  };
  const queue: SkuQueueWriter = {
    add: vi.fn(() => Promise.resolve(undefined)),
    getJob: vi.fn(() => Promise.resolve(undefined)),
  };

  return new ProcessMessageService(repository, queue);
}

describe('ProcessMessageService error classification', () => {
  it('converts Redis availability errors', async () => {
    const redisError = new RedisConnectionError('connect ECONNREFUSED 127.0.0.1:6379');
    const service = createServiceWithReceiveError(redisError);

    await expect(service.process(MESSAGE)).rejects.toMatchObject({
      name: ProcessMessageUnavailableError.name,
      cause: redisError,
    });
  });

  it('propagates non-Redis errors unchanged', async () => {
    const domainError = new Error('unexpected repository invariant');
    const service = createServiceWithReceiveError(domainError);

    await expect(service.process(MESSAGE)).rejects.toBe(domainError);
  });
});
