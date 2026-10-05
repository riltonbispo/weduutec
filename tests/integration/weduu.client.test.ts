import Fastify from 'fastify';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { z } from 'zod';

import { createMockPlatform, skuForSeq } from '../../scripts/mock-platform.js';
import { WeduuClient } from '../../src/clients/weduu.client.js';

const credentialsSchema = z.object({ cid: z.string(), token: z.string() });
const runSchema = z.object({ run_id: z.string() });
const webhook = Fastify({ logger: false });
let webhookUrl: string;

webhook.post('/check', (request) => request.body);
webhook.post('/process', () => ({ ok: true }));

beforeAll(async () => {
  await webhook.listen({ port: 0, host: '127.0.0.1' });
  const address = webhook.server.address();
  if (address === null || typeof address === 'string') {
    throw new Error('Test webhook did not bind to a TCP port');
  }
  webhookUrl = `http://127.0.0.1:${String(address.port)}`;
});

afterAll(async () => {
  await webhook.close();
});

describe('WeduuClient with the mock platform', () => {
  it('keeps 20 requests within the server concurrency limit without 429 responses', async () => {
    const platform = createMockPlatform({
      port: 0,
      total: 20,
      dupRate: 0,
      errorRate: 0,
      seed: 42,
      printReports: false,
    });

    try {
      const platformUrl = await platform.start();
      const registrationResponse = await fetch(`${platformUrl}/register`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ name: 'WeduuClient integration', webhook: webhookUrl }),
      });
      const credentials = credentialsSchema.parse(await registrationResponse.json());
      const burstResponse = await fetch(`${platformUrl}/burst/${credentials.cid}`, {
        method: 'POST',
        headers: { 'x-token': credentials.token },
      });
      const run = runSchema.parse(await burstResponse.json());
      await platform.waitForDispatch(run.run_id);
      const client = new WeduuClient({
        baseUrl: platformUrl,
        cid: credentials.cid,
        token: credentials.token,
        timeoutMs: 2_000,
      });

      const results = await Promise.all(
        Array.from({ length: 20 }, async (_, seq) => await client.enrich(skuForSeq(seq))),
      );

      expect(results).toHaveLength(20);
      expect(platform.metrics.enrichByStatus[200]).toBe(20);
      expect(platform.metrics.enrichByStatus[429]).toBe(0);
      expect(platform.metrics.peakEnrichInFlight).toBeLessThanOrEqual(3);
    } finally {
      await platform.stop();
    }
  }, 15_000);
});
