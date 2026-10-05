import type { Redis } from 'ioredis';

import { createRedisConnection } from '../../src/lib/redis.js';

const TEST_REDIS_TIMEOUTS = {
  commandTimeoutMs: 2_000,
  connectTimeoutMs: 1_000,
} as const;

export function createTestRedisConnection(redisUrl: string): Redis {
  return createRedisConnection(redisUrl, TEST_REDIS_TIMEOUTS);
}
