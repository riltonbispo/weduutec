import type { SkuQueueWriter } from '../queues/sku.queue.js';
import { enqueueSku, ensureSkuJob } from '../queues/sku.queue.js';
import type { ReceiveItemResult } from '../repositories/item.repository.js';
import type { ProcessBody } from '../schemas/process.schema.js';

const REDIS_ERROR_CODES = new Set([
  'EAI_AGAIN',
  'ECONNREFUSED',
  'ECONNRESET',
  'EPIPE',
  'ETIMEDOUT',
]);

const REDIS_ERROR_MESSAGES = [
  'command timed out',
  'connection is closed',
  'max retries per request',
  'socket closed unexpectedly',
];

export interface ProcessItemRepository {
  receiveItem(runId: string, seq: number, sku: string): Promise<ReceiveItemResult>;
  markQueued(runId: string, seq: number): Promise<boolean>;
}

function isRedisUnavailableError(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return false;
  }

  const code = 'code' in error ? error.code : undefined;
  if (typeof code === 'string' && REDIS_ERROR_CODES.has(code)) {
    return true;
  }

  if (error.name === 'MaxRetriesPerRequestError') {
    return true;
  }

  const normalizedMessage = error.message.toLowerCase();
  if (REDIS_ERROR_MESSAGES.some((message) => normalizedMessage.includes(message))) {
    return true;
  }

  return 'cause' in error && isRedisUnavailableError(error.cause);
}

export class ProcessMessageUnavailableError extends Error {
  constructor(cause: unknown) {
    super('Message processing storage is unavailable', { cause });
    this.name = 'ProcessMessageUnavailableError';
  }
}

export interface ProcessMessageHandler {
  process(message: ProcessBody): Promise<void>;
}

export class ProcessMessageService implements ProcessMessageHandler {
  constructor(
    private readonly itemRepository: ProcessItemRepository,
    private readonly queue: SkuQueueWriter,
  ) {}

  async process(message: ProcessBody): Promise<void> {
    try {
      const item = await this.itemRepository.receiveItem(message.run_id, message.seq, message.sku);
      const jobData = {
        runId: message.run_id,
        seq: message.seq,
        sku: message.sku,
      };

      if (!item.created && (item.status === 'completed' || item.status === 'failed')) {
        return;
      }

      if (item.created) {
        await enqueueSku(this.queue, jobData);
      } else {
        await ensureSkuJob(this.queue, jobData);
      }
      await this.itemRepository.markQueued(message.run_id, message.seq);
    } catch (error) {
      if (isRedisUnavailableError(error)) {
        throw new ProcessMessageUnavailableError(error);
      }

      throw error;
    }
  }
}
