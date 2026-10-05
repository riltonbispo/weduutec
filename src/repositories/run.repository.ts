import type { Redis } from 'ioredis';
import { z } from 'zod';

import type { CallbackClaimResult, Run, RunResolution } from '../domain/run.js';

const OPEN_RUNS_KEY = 'runs:open';

const REGISTER_RUN_SCRIPT = `
local existing_total = redis.call('HGET', KEYS[1], 'total')
local conflict = 0

if existing_total == false then
  redis.call('HSET', KEYS[1],
    'total', ARGV[1],
    'status', 'open',
    'registered_at', ARGV[2]
  )
  if redis.call('HEXISTS', KEYS[1], 'last_progress_at') == 0 then
    redis.call('HSET', KEYS[1], 'last_progress_at', ARGV[2])
  end
  existing_total = ARGV[1]
elseif existing_total ~= ARGV[1] then
  conflict = 1
end

local status = redis.call('HGET', KEYS[1], 'status')
if status ~= 'completed' and status ~= 'callback_failed' then
  redis.call('SADD', KEYS[2], ARGV[3])
end

return { conflict, existing_total }
`;

const CLAIM_CALLBACK_SCRIPT = `
local total = redis.call('HGET', KEYS[1], 'total')
if total == false then
  return 'no_total'
end

local status = redis.call('HGET', KEYS[1], 'status') or 'open'
if status == 'completed' or status == 'callback_failed' then
  return 'done'
end

local now = tonumber(ARGV[1])
if status == 'sending' then
  local lease_until = tonumber(redis.call('HGET', KEYS[1], 'lease_until') or '0')
  if lease_until > now then
    return 'busy'
  end
end

local next_attempt_at = tonumber(redis.call('HGET', KEYS[1], 'next_attempt_at') or '0')
if next_attempt_at > now then
  return 'wait'
end

for seq = 0, tonumber(total) - 1 do
  if redis.call('SISMEMBER', KEYS[2], tostring(seq)) == 0 then
    return 'incomplete'
  end
end

local attempt = redis.call('HINCRBY', KEYS[1], 'callback_attempts', 1)
redis.call('HSET', KEYS[1],
  'status', 'sending',
  'lease_until', tostring(now + tonumber(ARGV[2]))
)
return { 'claimed', attempt }
`;

const MARK_CALLBACK_SENT_SCRIPT = `
if redis.call('HGET', KEYS[1], 'status') ~= 'sending' then
  return 0
end

redis.call('HSET', KEYS[1], 'status', 'completed', 'sent_at', ARGV[1])
redis.call('HDEL', KEYS[1], 'lease_until', 'next_attempt_at', 'last_error')
redis.call('SREM', KEYS[2], ARGV[2])
return 1
`;

const MARK_CALLBACK_RETRY_SCRIPT = `
if redis.call('HGET', KEYS[1], 'status') ~= 'sending' then
  return 0
end

local attempts = tonumber(redis.call('HGET', KEYS[1], 'callback_attempts') or '0')
if ARGV[3] == '1' or attempts >= tonumber(ARGV[4]) then
  redis.call('HSET', KEYS[1], 'status', 'callback_failed', 'last_error', ARGV[1])
  redis.call('HDEL', KEYS[1], 'lease_until', 'next_attempt_at')
  redis.call('SREM', KEYS[2], ARGV[5])
  return 2
end

redis.call('HSET', KEYS[1],
  'status', 'open',
  'last_error', ARGV[1],
  'next_attempt_at', ARGV[2]
)
redis.call('HDEL', KEYS[1], 'lease_until')
redis.call('SADD', KEYS[2], ARGV[5])
return 1
`;

const MARK_STALLED_SCRIPT = `
if redis.call('HGET', KEYS[1], 'status') ~= 'open' then
  return 0
end

redis.call('HSET', KEYS[1], 'status', 'stalled', 'last_error', 'missing_sequences')
return 1
`;

const runHashSchema = z.object({
  total: z.coerce.number().int().positive().optional(),
  status: z.enum(['open', 'stalled', 'sending', 'completed', 'callback_failed']).default('open'),
  registered_at: z.coerce.number().int().min(0).optional(),
  last_progress_at: z.coerce.number().int().min(0).optional(),
  callback_attempts: z.coerce.number().int().min(0).default(0),
  next_attempt_at: z.coerce.number().int().min(0).optional(),
  lease_until: z.coerce.number().int().min(0).optional(),
  sent_at: z.coerce.number().int().min(0).optional(),
  last_error: z.string().min(1).optional(),
});

export interface RegisterRunResult {
  conflict: boolean;
  total: number;
}

export interface MarkCallbackRetryData {
  error: string;
  nextAttemptAt: number;
  forceFailed?: boolean;
}

function runKey(runId: string): string {
  return `run:${runId}`;
}

function resolvedKey(runId: string): string {
  return `run:${runId}:resolved`;
}

function parseBooleanResult(result: unknown, operation: string): boolean {
  if (result !== 0 && result !== 1) {
    throw new Error(`Redis returned an invalid ${operation} result`);
  }
  return result === 1;
}

export class RunRepository {
  constructor(
    private readonly redis: Redis,
    private readonly callbackMaxAttempts = 5,
  ) {}

  async registerRun(runId: string, total: number, now: number): Promise<RegisterRunResult> {
    const result: unknown = await this.redis.eval(
      REGISTER_RUN_SCRIPT,
      2,
      runKey(runId),
      OPEN_RUNS_KEY,
      String(total),
      String(now),
      runId,
    );
    if (
      !Array.isArray(result) ||
      result.length !== 2 ||
      (result[0] !== 0 && result[0] !== 1) ||
      (typeof result[1] !== 'string' && typeof result[1] !== 'number')
    ) {
      throw new Error('Redis returned an invalid registerRun result');
    }
    const retainedTotal = Number(result[1]);
    if (!Number.isInteger(retainedTotal) || retainedTotal < 1) {
      throw new Error('Redis returned an invalid run total');
    }
    return { conflict: result[0] === 1, total: retainedTotal };
  }

  async getRun(runId: string): Promise<Run | null> {
    const raw: unknown = await this.redis.hgetall(runKey(runId));
    const hash = z.record(z.string()).safeParse(raw);
    if (!hash.success) {
      throw new Error('Redis returned an invalid run hash');
    }
    if (Object.keys(hash.data).length === 0) {
      return null;
    }
    const parsed = runHashSchema.safeParse(hash.data);
    if (!parsed.success) {
      throw new Error(`Redis returned an invalid run: ${parsed.error.message}`);
    }
    return {
      runId,
      total: parsed.data.total,
      status: parsed.data.status,
      registeredAt: parsed.data.registered_at,
      lastProgressAt: parsed.data.last_progress_at,
      callbackAttempts: parsed.data.callback_attempts,
      nextAttemptAt: parsed.data.next_attempt_at,
      leaseUntil: parsed.data.lease_until,
      sentAt: parsed.data.sent_at,
      lastError: parsed.data.last_error,
    };
  }

  async listOpenRunIds(): Promise<string[]> {
    const result: unknown = await this.redis.smembers(OPEN_RUNS_KEY);
    const parsed = z.array(z.string().min(1)).safeParse(result);
    if (!parsed.success) {
      throw new Error('Redis returned invalid open run IDs');
    }
    return parsed.data;
  }

  async claimCallback(runId: string, now: number, leaseMs: number): Promise<CallbackClaimResult> {
    const result: unknown = await this.redis.eval(
      CLAIM_CALLBACK_SCRIPT,
      2,
      runKey(runId),
      resolvedKey(runId),
      String(now),
      String(leaseMs),
    );
    if (
      result === 'no_total' ||
      result === 'done' ||
      result === 'busy' ||
      result === 'wait' ||
      result === 'incomplete'
    ) {
      return result;
    }
    if (
      Array.isArray(result) &&
      result.length === 2 &&
      result[0] === 'claimed' &&
      (typeof result[1] === 'number' || typeof result[1] === 'string')
    ) {
      const attempt = Number(result[1]);
      if (Number.isInteger(attempt) && attempt > 0) {
        return { claimed: true, attempt };
      }
    }
    throw new Error('Redis returned an invalid claimCallback result');
  }

  async markCallbackSent(runId: string, now: number): Promise<boolean> {
    const result: unknown = await this.redis.eval(
      MARK_CALLBACK_SENT_SCRIPT,
      2,
      runKey(runId),
      OPEN_RUNS_KEY,
      String(now),
      runId,
    );
    return parseBooleanResult(result, 'markCallbackSent');
  }

  async markCallbackRetry(
    runId: string,
    data: MarkCallbackRetryData,
  ): Promise<'retry' | 'failed' | 'ignored'> {
    const result: unknown = await this.redis.eval(
      MARK_CALLBACK_RETRY_SCRIPT,
      2,
      runKey(runId),
      OPEN_RUNS_KEY,
      data.error,
      String(data.nextAttemptAt),
      data.forceFailed === true ? '1' : '0',
      String(this.callbackMaxAttempts),
      runId,
    );
    if (result === 0) return 'ignored';
    if (result === 1) return 'retry';
    if (result === 2) return 'failed';
    throw new Error('Redis returned an invalid markCallbackRetry result');
  }

  async markStalled(runId: string, now: number): Promise<boolean> {
    const result: unknown = await this.redis.eval(
      MARK_STALLED_SCRIPT,
      1,
      runKey(runId),
      String(now),
    );
    return parseBooleanResult(result, 'markStalled');
  }

  async getResolution(runId: string, total: number): Promise<RunResolution> {
    const result: unknown = await this.redis.smembers(resolvedKey(runId));
    const parsed = z.array(z.coerce.number().int().min(0)).safeParse(result);
    if (!parsed.success) {
      throw new Error('Redis returned invalid resolved sequences');
    }
    const resolvedSeqs = [...new Set(parsed.data)].sort((left, right) => left - right);
    const resolvedSet = new Set(resolvedSeqs);
    const missingSeqs = Array.from({ length: total }, (_, seq) => seq).filter(
      (seq) => !resolvedSet.has(seq),
    );
    return { resolvedSeqs, missingSeqs };
  }

  async removeOpenRun(runId: string): Promise<void> {
    await this.redis.srem(OPEN_RUNS_KEY, runId);
  }
}
