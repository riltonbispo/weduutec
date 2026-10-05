import { mkdtemp, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import Fastify from 'fastify';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { z } from 'zod';

import {
  createDispatchPlan,
  createMockPlatform,
  expectedEnrichment,
  mulberry32,
  skuForSeq,
} from '../../scripts/mock-platform.js';
import type { MockPlatform } from '../../scripts/mock-platform.js';

const credentialsSchema = z.object({ cid: z.string(), token: z.string() });
const burstSchema = z.object({ run_id: z.string(), total: z.number() });

const webhook = Fastify({ logger: false });
let webhookUrl: string;
let reportsDir: string;
let platforms: MockPlatform[] = [];

webhook.post('/ok/check', (request) => request.body);
webhook.post('/bad/check', () => ({ token: 'wrong-token' }));
webhook.post('/ok/process', () => ({ ok: true }));

beforeAll(async () => {
  await webhook.listen({ port: 0, host: '127.0.0.1' });
  const address = webhook.server.address();
  if (address === null || typeof address === 'string') {
    throw new Error('Test webhook did not bind to a TCP port');
  }
  webhookUrl = `http://127.0.0.1:${String(address.port)}`;
});

beforeEach(async () => {
  reportsDir = await mkdtemp(join(tmpdir(), 'weduu-mock-reports-'));
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
  await webhook.close();
});

function createPlatform(options: Parameters<typeof createMockPlatform>[0] = {}): MockPlatform {
  const platform = createMockPlatform({
    reportsDir,
    printReports: false,
    errorRate: 0,
    ...options,
  });
  platforms.push(platform);
  return platform;
}

async function register(platform: MockPlatform) {
  const response = await platform.app.inject({
    method: 'POST',
    url: '/register',
    payload: { name: 'Unit test', webhook: `${webhookUrl}/ok` },
  });
  const body: unknown = response.json();

  expect(response.statusCode).toBe(200);
  return credentialsSchema.parse(body);
}

async function burst(platform: MockPlatform, credentials: z.infer<typeof credentialsSchema>) {
  const response = await platform.app.inject({
    method: 'POST',
    url: `/burst/${credentials.cid}`,
    headers: { 'x-token': credentials.token },
  });
  const body: unknown = response.json();

  expect(response.statusCode).toBe(200);
  return burstSchema.parse(body);
}

function callbackItem(seq: number) {
  const sku = skuForSeq(seq);
  const expected = expectedEnrichment(sku);
  if (expected === null) {
    throw new Error(`No deterministic enrichment for ${sku}`);
  }

  return { seq, sku, ...expected };
}

describe('mock platform', () => {
  it('registers a webhook after a successful handshake and rejects a failed one', async () => {
    const platform = createPlatform();

    const success = await platform.app.inject({
      method: 'POST',
      url: '/register',
      payload: { name: 'Valid webhook', webhook: `${webhookUrl}/ok` },
    });
    const failure = await platform.app.inject({
      method: 'POST',
      url: '/register',
      payload: { name: 'Invalid webhook', webhook: `${webhookUrl}/bad` },
    });

    expect(success.statusCode).toBe(200);
    expect(credentialsSchema.safeParse(success.json()).success).toBe(true);
    expect(failure.statusCode).toBe(422);
    expect(failure.json()).toMatchObject({ error: 'handshake_failed' });
  });

  it('limits five simultaneous enrich calls to three in flight', async () => {
    const platform = createPlatform({ total: 5 });
    const credentials = await register(platform);
    const run = await burst(platform, credentials);
    await platform.waitForDispatch(run.run_id);

    const responses = await Promise.all(
      Array.from(
        { length: 5 },
        async () =>
          await platform.app.inject({
            method: 'GET',
            url: '/enrich/sku-001',
            headers: { 'x-cid': credentials.cid, 'x-token': credentials.token },
          }),
      ),
    );
    const rateLimited = responses.filter((response) => response.statusCode === 429);

    expect(rateLimited.length).toBeGreaterThanOrEqual(2);
    expect(rateLimited.every((response) => response.headers['retry-after'] === '1')).toBe(true);
    expect(platform.metrics.peakEnrichInFlight).toBeLessThanOrEqual(3);
    expect(platform.metrics.enrichByStatus[200]).toBe(3);
  });

  it('produces the same dispatch decisions for the same seed', () => {
    const first = createDispatchPlan(20, 0.25, new Set([3, 8]), mulberry32(42));
    const second = createDispatchPlan(20, 0.25, new Set([3, 8]), mulberry32(42));

    expect(first).toEqual(second);
  });

  it('validates callbacks and reports missing, duplicate, unordered and divergent items', async () => {
    const platform = createPlatform({ total: 4, dupRate: 0 });
    const credentials = await register(platform);
    const run = await burst(platform, credentials);
    await platform.waitForDispatch(run.run_id);

    const invalidResponse = await platform.app.inject({
      method: 'POST',
      url: '/callback',
      headers: { 'x-token': credentials.token },
      payload: { cid: credentials.cid, run_id: run.run_id },
    });
    const divergentItem = callbackItem(1);
    divergentItem.price += 1;
    const validResponse = await platform.app.inject({
      method: 'POST',
      url: '/callback',
      headers: { 'x-token': credentials.token },
      payload: {
        cid: credentials.cid,
        run_id: run.run_id,
        result: [callbackItem(0), callbackItem(2), callbackItem(0), divergentItem],
      },
    });
    const report = platform.reports.at(-1);

    expect(invalidResponse.statusCode).toBe(400);
    expect(validResponse.statusCode).toBe(200);
    expect(report).toBeDefined();
    expect(report?.callback.missing_seqs).toEqual([3]);
    expect(report?.callback.duplicate_seqs).toEqual([0]);
    expect(report?.callback.out_of_order_seqs).toEqual([0, 1]);
    expect(report?.callback.divergence_seqs).toEqual([1]);
    expect(report?.verdict.status).toBe('FAIL');
    await expect(readdir(reportsDir)).resolves.toEqual([`${run.run_id}-1.json`]);
  });
});
