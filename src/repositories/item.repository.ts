import type { Redis } from 'ioredis';

export type ItemStatus = 'received' | 'queued' | 'processing' | 'completed' | 'failed';

export interface ReceiveItemResult {
  created: boolean;
  status: ItemStatus;
}

const RECEIVE_ITEM_SCRIPT = `
if redis.call('EXISTS', KEYS[1]) == 0 then
  redis.call('HSET', KEYS[1],
    'run_id', ARGV[1],
    'seq', ARGV[2],
    'sku', ARGV[3],
    'status', 'received',
    'received_at', ARGV[4]
  )
  return { 1, 'received' }
end

return { 0, redis.call('HGET', KEYS[1], 'status') }
`;

const MARK_QUEUED_SCRIPT = `
if redis.call('HGET', KEYS[1], 'status') == 'received' then
  redis.call('HSET', KEYS[1], 'status', 'queued', 'queued_at', ARGV[1])
  return 1
end

return 0
`;

const ITEM_STATUSES = new Set<string>(['received', 'queued', 'processing', 'completed', 'failed']);

export function itemKey(runId: string, seq: number): string {
  return `item:${runId}:${String(seq)}`;
}

function isItemStatus(value: unknown): value is ItemStatus {
  return typeof value === 'string' && ITEM_STATUSES.has(value);
}

function parseReceiveItemResult(result: unknown): ReceiveItemResult {
  if (
    !Array.isArray(result) ||
    result.length !== 2 ||
    (result[0] !== 0 && result[0] !== 1) ||
    !isItemStatus(result[1])
  ) {
    throw new Error('Redis returned an invalid receiveItem result');
  }

  return { created: result[0] === 1, status: result[1] };
}

export class ItemRepository {
  constructor(private readonly redis: Redis) {}

  async receiveItem(runId: string, seq: number, sku: string): Promise<ReceiveItemResult> {
    const result: unknown = await this.redis.eval(
      RECEIVE_ITEM_SCRIPT,
      1,
      itemKey(runId, seq),
      runId,
      String(seq),
      sku,
      new Date().toISOString(),
    );

    return parseReceiveItemResult(result);
  }

  async markQueued(runId: string, seq: number): Promise<boolean> {
    const result: unknown = await this.redis.eval(
      MARK_QUEUED_SCRIPT,
      1,
      itemKey(runId, seq),
      new Date().toISOString(),
    );

    if (result !== 0 && result !== 1) {
      throw new Error('Redis returned an invalid markQueued result');
    }

    return result === 1;
  }

  async getStatus(runId: string, seq: number): Promise<ItemStatus | null> {
    const status: unknown = await this.redis.hget(itemKey(runId, seq), 'status');

    if (status === null || isItemStatus(status)) {
      return status;
    }

    throw new Error('Redis returned an invalid item status');
  }
}
