import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import type { FastifyInstance } from 'fastify';
import type { Redis } from 'ioredis';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { z } from 'zod';

import { createMockPlatform } from '../../scripts/mock-platform.js';
import type { MockPlatform } from '../../scripts/mock-platform.js';
import { buildApp } from '../../src/app.js';
import { createSkuQueue } from '../../src/queues/sku.queue.js';
import type { SkuQueue } from '../../src/queues/sku.queue.js';
import { createTestRedisConnection } from '../helpers/redis.js';

const TEST_REDIS_URL = process.env.TEST_REDIS_URL ?? 'redis://127.0.0.1:6379/14';
const TOTAL = 10;
const credentialsSchema = z.object({ cid: z.string(), token: z.string() });
const burstSchema = z.object({ run_id: z.string(), total: z.number() });

let app: FastifyInstance;
let appUrl: string;
let redis: Redis;
let queue: SkuQueue;
let reportsDir: string;
let platforms: MockPlatform[] = [];

beforeAll(async () => {
  redis = createTestRedisConnection(TEST_REDIS_URL);
  await redis.connect();
  queue = createSkuQueue(redis);
  app = buildApp({ redis, queue, logLevel: 'silent' });
  await app.listen({ port: 0, host: '127.0.0.1' });
  const address = app.server.address();
  if (address === null || typeof address === 'string') {
    throw new Error('Integration app did not bind to a TCP port');
  }
  appUrl = `http://127.0.0.1:${String(address.port)}`;
});

beforeEach(async () => {
  await redis.flushdb();
  reportsDir = await mkdtemp(join(tmpdir(), 'weduu-mock-integration-'));
  platforms = [];
});

afterEach(async () => {
  await Promise.all(
    platforms.map(async (platform) => {
      await platform.stop();
    }),
  );
  await rm(reportsDir, { recursive: true, force: true });
});

afterAll(async () => {
  await app.close();
  await queue.close();
  await redis.quit();
});

async function exerciseBurst(earlyDispatch: boolean): Promise<void> {
  const platform = createMockPlatform({
    port: 0,
    total: TOTAL,
    dupRate: 0.5,
    errorRate: 0,
    seed: 42,
    earlyDispatch,
    reportsDir,
    printReports: false,
  });
  platforms.push(platform);
  const platformUrl = await platform.start();

  const registrationResponse = await fetch(`${platformUrl}/register`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ name: 'Integration test', webhook: appUrl }),
  });
  const registrationBody: unknown = await registrationResponse.json();
  const credentials = credentialsSchema.parse(registrationBody);

  const burstResponse = await fetch(`${platformUrl}/burst/${credentials.cid}`, {
    method: 'POST',
    headers: { 'x-token': credentials.token },
  });
  const burstBody: unknown = await burstResponse.json();
  const run = burstSchema.parse(burstBody);
  await platform.waitForDispatch(run.run_id);

  const report = platform.reports.find((candidate) => candidate.run_id === run.run_id);

  expect(registrationResponse.status).toBe(200);
  expect(burstResponse.status).toBe(200);
  expect(run.total).toBe(TOTAL);
  await expect(redis.keys(`item:${run.run_id}:*`)).resolves.toHaveLength(TOTAL);
  await expect(queue.getJobCounts('waiting')).resolves.toMatchObject({ waiting: TOTAL });
  expect(report).toBeDefined();
  expect(report?.ack.above_600_ms).toBe(0);
}

describe('mock platform with the real process endpoint', () => {
  it('dispatches a normal burst without duplicate items or jobs', async () => {
    await exerciseBurst(false);
  });

  it('dispatches before the burst response when EARLY_DISPATCH is enabled', async () => {
    await exerciseBurst(true);
  });
});
