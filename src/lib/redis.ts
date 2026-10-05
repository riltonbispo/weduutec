import { Redis } from 'ioredis';

interface RedisConnectionTimeouts {
  commandTimeoutMs?: number;
  connectTimeoutMs?: number;
}

export function createRedisConnection(
  redisUrl: string,
  timeouts: RedisConnectionTimeouts = {},
): Redis {
  return new Redis(redisUrl, {
    lazyConnect: true,
    connectTimeout: timeouts.connectTimeoutMs ?? 75,
    commandTimeout: timeouts.commandTimeoutMs ?? 175,
    maxRetriesPerRequest: 0,
    retryStrategy: () => 50,
  });
}

export function createWorkerRedisConnection(redisUrl: string): Redis {
  return new Redis(redisUrl, {
    lazyConnect: true,
    maxRetriesPerRequest: null,
  });
}
