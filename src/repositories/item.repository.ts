import type { Redis } from 'ioredis';
import { z } from 'zod';

import type { EnrichErrorKind } from '../domain/errors.js';

export type ItemStatus = 'received' | 'queued' | 'processing' | 'completed' | 'failed';

export interface ReceiveItemResult {
  created: boolean;
  status: ItemStatus;
}

export interface ItemErrorDetails {
  kind: EnrichErrorKind;
  statusCode?: number;
  message: string;
}

export interface ItemRecord {
  runId: string;
  seq: number;
  sku: string;
  status: ItemStatus;
  receivedAt: string;
  queuedAt?: string;
  startedAt?: string;
  completedAt?: string;
  lastAttemptAt?: string;
  price?: number;
  stock?: number;
  attempts: number;
  rateLimitWaits: number;
  error?: string;
  errorKind?: EnrichErrorKind;
  statusCode?: number;
  lastError?: ItemErrorDetails;
}

export interface CompleteItemData {
  price: number;
  stock: number;
}

export interface FailItemData extends ItemErrorDetails {
  attempts: number;
}

export interface RecordRetryData {
  attempts: number;
  rateLimitWaits: number;
  lastError: ItemErrorDetails;
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

const MARK_PROCESSING_SCRIPT = `
local status = redis.call('HGET', KEYS[1], 'status')
if status == 'queued' or status == 'processing' then
  redis.call('HSET', KEYS[1], 'status', 'processing', 'started_at', ARGV[1])
  redis.call('HINCRBY', KEYS[1], 'attempts', 1)
  return 1
end

return 0
`;

const COMPLETE_ITEM_SCRIPT = `
local status = redis.call('HGET', KEYS[1], 'status')
if status == 'completed' then
  return 1
end
if status == false or status == 'failed' then
  return 0
end

local attempts = redis.call('HGET', KEYS[1], 'attempts') or '1'
redis.call('HSET', KEYS[1],
  'status', 'completed',
  'price', ARGV[1],
  'stock', ARGV[2],
  'completed_at', ARGV[3],
  'attempts', attempts
)
return 1
`;

const FAIL_ITEM_SCRIPT = `
local status = redis.call('HGET', KEYS[1], 'status')
if status == 'failed' then
  return 1
end
if status == false or status == 'completed' then
  return 0
end

redis.call('HSET', KEYS[1],
  'status', 'failed',
  'error', ARGV[1],
  'error_kind', ARGV[2],
  'attempts', ARGV[4],
  'last_attempt_at', ARGV[5]
)
if ARGV[3] == '' then
  redis.call('HDEL', KEYS[1], 'status_code')
else
  redis.call('HSET', KEYS[1], 'status_code', ARGV[3])
end
return 1
`;

const RECORD_RETRY_SCRIPT = `
local status = redis.call('HGET', KEYS[1], 'status')
if status == false or status == 'completed' or status == 'failed' then
  return 0
end

redis.call('HSET', KEYS[1],
  'status', 'queued',
  'attempts', ARGV[1],
  'rate_limit_waits', ARGV[2],
  'last_error', ARGV[3],
  'last_error_kind', ARGV[4],
  'last_attempt_at', ARGV[6]
)
if ARGV[5] == '' then
  redis.call('HDEL', KEYS[1], 'last_status_code')
else
  redis.call('HSET', KEYS[1], 'last_status_code', ARGV[5])
end
return 1
`;

const ITEM_STATUSES = new Set<string>(['received', 'queued', 'processing', 'completed', 'failed']);

const itemHashSchema = z
  .object({
    run_id: z.string().min(1),
    seq: z.coerce.number().int().min(0),
    sku: z.string().min(1),
    status: z.enum(['received', 'queued', 'processing', 'completed', 'failed']),
    received_at: z.string().min(1),
    queued_at: z.string().min(1).optional(),
    started_at: z.string().min(1).optional(),
    completed_at: z.string().min(1).optional(),
    last_attempt_at: z.string().min(1).optional(),
    price: z.coerce.number().finite().min(0).optional(),
    stock: z.coerce.number().int().min(0).optional(),
    attempts: z.coerce.number().int().min(0).default(0),
    rate_limit_waits: z.coerce.number().int().min(0).default(0),
    error: z.string().min(1).optional(),
    error_kind: z.enum(['transient', 'rate_limited', 'permanent']).optional(),
    status_code: z.coerce.number().int().positive().optional(),
    last_error: z.string().min(1).optional(),
    last_error_kind: z.enum(['transient', 'rate_limited', 'permanent']).optional(),
    last_status_code: z.coerce.number().int().positive().optional(),
  })
  .superRefine((item, context) => {
    if (
      item.status === 'completed' &&
      (item.price === undefined || item.stock === undefined || item.completed_at === undefined)
    ) {
      context.addIssue({ code: z.ZodIssueCode.custom, message: 'completed item is incomplete' });
    }
    if (
      item.status === 'failed' &&
      (item.error === undefined ||
        item.error_kind === undefined ||
        item.last_attempt_at === undefined)
    ) {
      context.addIssue({ code: z.ZodIssueCode.custom, message: 'failed item is incomplete' });
    }
    if ((item.last_error === undefined) !== (item.last_error_kind === undefined)) {
      context.addIssue({ code: z.ZodIssueCode.custom, message: 'last error is incomplete' });
    }
  });

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

function parseBooleanResult(result: unknown, operation: string): boolean {
  if (result !== 0 && result !== 1) {
    throw new Error(`Redis returned an invalid ${operation} result`);
  }

  return result === 1;
}

function optionalStatusCode(statusCode: number | undefined): string {
  return statusCode === undefined ? '' : String(statusCode);
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

    return parseBooleanResult(result, 'markQueued');
  }

  async markProcessing(runId: string, seq: number): Promise<boolean> {
    const result: unknown = await this.redis.eval(
      MARK_PROCESSING_SCRIPT,
      1,
      itemKey(runId, seq),
      new Date().toISOString(),
    );

    return parseBooleanResult(result, 'markProcessing');
  }

  async completeItem(runId: string, seq: number, data: CompleteItemData): Promise<boolean> {
    const result: unknown = await this.redis.eval(
      COMPLETE_ITEM_SCRIPT,
      1,
      itemKey(runId, seq),
      String(data.price),
      String(data.stock),
      new Date().toISOString(),
    );

    return parseBooleanResult(result, 'completeItem');
  }

  async failItem(runId: string, seq: number, data: FailItemData): Promise<boolean> {
    const result: unknown = await this.redis.eval(
      FAIL_ITEM_SCRIPT,
      1,
      itemKey(runId, seq),
      data.message,
      data.kind,
      optionalStatusCode(data.statusCode),
      String(data.attempts),
      new Date().toISOString(),
    );

    return parseBooleanResult(result, 'failItem');
  }

  async recordRetry(runId: string, seq: number, data: RecordRetryData): Promise<boolean> {
    const result: unknown = await this.redis.eval(
      RECORD_RETRY_SCRIPT,
      1,
      itemKey(runId, seq),
      String(data.attempts),
      String(data.rateLimitWaits),
      data.lastError.message,
      data.lastError.kind,
      optionalStatusCode(data.lastError.statusCode),
      new Date().toISOString(),
    );

    return parseBooleanResult(result, 'recordRetry');
  }

  async getItem(runId: string, seq: number): Promise<ItemRecord | null> {
    const hash: unknown = await this.redis.hgetall(itemKey(runId, seq));
    const hashRecord = z.record(z.string()).safeParse(hash);
    if (!hashRecord.success) {
      throw new Error('Redis returned an invalid item hash');
    }
    if (Object.keys(hashRecord.data).length === 0) {
      return null;
    }

    const parsed = itemHashSchema.safeParse(hashRecord.data);
    if (!parsed.success) {
      throw new Error(`Redis returned an invalid item: ${parsed.error.message}`);
    }

    const item = parsed.data;
    if (item.run_id !== runId || item.seq !== seq) {
      throw new Error('Redis returned an item with a mismatched identity');
    }
    const lastError =
      item.last_error === undefined || item.last_error_kind === undefined
        ? undefined
        : {
            message: item.last_error,
            kind: item.last_error_kind,
            statusCode: item.last_status_code,
          };

    return {
      runId: item.run_id,
      seq: item.seq,
      sku: item.sku,
      status: item.status,
      receivedAt: item.received_at,
      queuedAt: item.queued_at,
      startedAt: item.started_at,
      completedAt: item.completed_at,
      lastAttemptAt: item.last_attempt_at,
      price: item.price,
      stock: item.stock,
      attempts: item.attempts,
      rateLimitWaits: item.rate_limit_waits,
      error: item.error,
      errorKind: item.error_kind,
      statusCode: item.status_code,
      lastError,
    };
  }

  async getStatus(runId: string, seq: number): Promise<ItemStatus | null> {
    const status: unknown = await this.redis.hget(itemKey(runId, seq), 'status');

    if (status === null || isItemStatus(status)) {
      return status;
    }

    throw new Error('Redis returned an invalid item status');
  }
}
