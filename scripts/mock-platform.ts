import { mkdir, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import Fastify, { LogController } from 'fastify';
import type { FastifyInstance } from 'fastify';
import { z } from 'zod';

const REQUEST_TIMEOUT_MS = 3_000;
const MAX_DELIVERY_ATTEMPTS = 3;
const MAX_ENRICH_CONCURRENCY = 3;

const registerBodySchema = z.object({
  name: z.string().min(1),
  webhook: z.string().url(),
});

const checkResponseSchema = z.object({ token: z.string() });

const callbackItemSchema = z.object({
  seq: z.number().int().min(0),
  sku: z.string().min(1),
  price: z.number().finite(),
  stock: z.number().int().min(0),
});

const callbackBodySchema = z.object({
  cid: z.string().min(1),
  run_id: z.string().min(1),
  result: z.array(callbackItemSchema),
});

type CallbackItem = z.infer<typeof callbackItemSchema>;
type EnrichStatus = 200 | 401 | 404 | 429 | 500;
type Random = () => number;

export interface MockPlatformOptions {
  port?: number;
  total?: number;
  dupRate?: number;
  errorRate?: number;
  seed?: number;
  dropSeqs?: Iterable<number>;
  invalidSkuSeqs?: Iterable<number>;
  earlyDispatch?: boolean;
  reportsDir?: string;
  logger?: boolean;
  printReports?: boolean;
}

interface ResolvedMockPlatformOptions {
  port: number;
  total: number;
  dupRate: number;
  errorRate: number;
  seed: number;
  dropSeqs: Set<number>;
  invalidSkuSeqs: Set<number>;
  earlyDispatch: boolean;
  reportsDir: string;
  logger: boolean;
  printReports: boolean;
}

interface Registration {
  cid: string;
  token: string;
  webhook: string;
}

export interface DispatchMessage {
  seq: number;
  sku: string;
  duplicate: boolean;
}

interface AckSample {
  durationMs: number;
  timedOut: boolean;
  non2xx: boolean;
}

interface Deferred {
  promise: Promise<void>;
  resolve: () => void;
  reject: (error: unknown) => void;
}

interface RunState {
  runId: string;
  cid: string;
  total: number;
  startedAt: string;
  startedAtMs: number;
  deliveryPlan: DispatchMessage[];
  ackSamples: AckSample[];
  sendAttemptsStarted: number;
  dispatch: Deferred;
  callbackCount: number;
}

export interface MockPlatformMetrics {
  enrichByStatus: Record<EnrichStatus, number>;
  enrichInFlight: number;
  peakEnrichInFlight: number;
}

export interface MockReport {
  run_id: string;
  generated_at: string;
  ack: {
    average_ms: number;
    p95_ms: number;
    max_ms: number;
    above_600_ms: number;
    timeouts: number;
    non_2xx: number;
  };
  messages: {
    sent: number;
    duplicates_sent: number;
    expected: number;
  };
  callback: {
    items_received: number;
    duplicate_seqs: number[];
    missing_seqs: number[];
    out_of_order_seqs: number[];
    divergence_seqs: number[];
  };
  enrich: {
    by_status: Record<EnrichStatus, number>;
    peak_in_flight: number;
  };
  total_time_ms: number;
  callbacks_for_run: number;
  verdict: {
    status: 'PASS' | 'FAIL';
    reasons: string[];
  };
}

export interface MockPlatform {
  app: FastifyInstance;
  start: () => Promise<string>;
  stop: () => Promise<void>;
  reports: MockReport[];
  metrics: MockPlatformMetrics;
  waitForDispatch: (runId: string) => Promise<void>;
}

export function mulberry32(seed: number): Random {
  let value = seed >>> 0;

  return () => {
    value += 0x6d2b79f5;
    let result = value;
    result = Math.imul(result ^ (result >>> 15), result | 1);
    result ^= result + Math.imul(result ^ (result >>> 7), result | 61);
    return ((result ^ (result >>> 14)) >>> 0) / 4_294_967_296;
  };
}

export function skuForSeq(seq: number): string {
  return `sku-${String(seq + 1).padStart(3, '0')}`;
}

export function expectedEnrichment(sku: string): { price: number; stock: number } | null {
  const match = /^sku-(\d{3})$/.exec(sku);
  if (match === null) {
    return null;
  }

  const skuNumber = Number(match[1]);
  if (!Number.isInteger(skuNumber) || skuNumber < 1) {
    return null;
  }

  return {
    price: Number((9.99 + skuNumber * 1.37).toFixed(2)),
    stock: (skuNumber * 17) % 101,
  };
}

function shuffle<T>(values: T[], random: Random): T[] {
  for (let index = values.length - 1; index > 0; index -= 1) {
    const swapIndex = Math.floor(random() * (index + 1));
    [values[index], values[swapIndex]] = [values[swapIndex], values[index]];
  }

  return values;
}

export function createDispatchPlan(
  total: number,
  dupRate: number,
  dropSeqs: ReadonlySet<number>,
  random: Random,
): DispatchMessage[] {
  const originals = Array.from({ length: total }, (_, seq) => ({
    seq,
    sku: skuForSeq(seq),
    duplicate: false,
  })).filter((message) => !dropSeqs.has(message.seq));

  shuffle(originals, random);

  const deliveries = originals.flatMap((message) => {
    if (random() < dupRate) {
      return [message, { ...message, duplicate: true }];
    }

    return [message];
  });

  return shuffle(deliveries, random);
}

function createDeferred(): Deferred {
  let resolvePromise: (() => void) | undefined;
  let rejectPromise: ((error: unknown) => void) | undefined;
  const promise = new Promise<void>((resolveValue, rejectValue) => {
    resolvePromise = resolveValue;
    rejectPromise = rejectValue;
  });

  return {
    promise,
    resolve: () => resolvePromise?.(),
    reject: (error) => rejectPromise?.(error),
  };
}

function resolveOptions(options: MockPlatformOptions): ResolvedMockPlatformOptions {
  const resolvedOptions: ResolvedMockPlatformOptions = {
    port: options.port ?? 5000,
    total: options.total ?? 20,
    dupRate: options.dupRate ?? 0.25,
    errorRate: options.errorRate ?? 0.1,
    seed: options.seed ?? 42,
    dropSeqs: new Set(options.dropSeqs ?? []),
    invalidSkuSeqs: new Set(options.invalidSkuSeqs ?? []),
    earlyDispatch: options.earlyDispatch ?? false,
    reportsDir: options.reportsDir ?? resolve(process.cwd(), 'reports'),
    logger: options.logger ?? false,
    printReports: options.printReports ?? true,
  };

  if (!Number.isInteger(resolvedOptions.port) || resolvedOptions.port < 0) {
    throw new Error('port must be a non-negative integer');
  }
  if (!Number.isInteger(resolvedOptions.total) || resolvedOptions.total < 1) {
    throw new Error('total must be a positive integer');
  }
  if (resolvedOptions.dupRate < 0 || resolvedOptions.dupRate > 1) {
    throw new Error('dupRate must be between 0 and 1');
  }
  if (resolvedOptions.errorRate < 0 || resolvedOptions.errorRate > 1) {
    throw new Error('errorRate must be between 0 and 1');
  }

  return resolvedOptions;
}

function appendPath(baseUrl: string, path: string): string {
  const normalizedBaseUrl = baseUrl.endsWith('/') ? baseUrl : `${baseUrl}/`;
  return new URL(path, normalizedBaseUrl).toString();
}

function randomId(prefix: string, random: Random): string {
  return `${prefix}_${Math.floor(random() * 0xffff_ffff)
    .toString(16)
    .padStart(8, '0')}`;
}

function percentile95(values: number[]): number {
  if (values.length === 0) {
    return 0;
  }

  const sortedValues = [...values].sort((left, right) => left - right);
  return sortedValues[Math.ceil(sortedValues.length * 0.95) - 1] ?? 0;
}

function uniqueSorted(values: Iterable<number>): number[] {
  return [...new Set(values)].sort((left, right) => left - right);
}

function analyzeCallback(total: number, result: CallbackItem[]) {
  const seen = new Set<number>();
  const duplicates: number[] = [];
  const outOfOrder: number[] = [];
  const divergences: number[] = [];
  let highestSeq = -1;

  for (const item of result) {
    if (seen.has(item.seq)) {
      duplicates.push(item.seq);
    }
    seen.add(item.seq);

    if (item.seq < highestSeq) {
      outOfOrder.push(item.seq);
    }
    highestSeq = Math.max(highestSeq, item.seq);

    const expectedSku = item.seq >= 0 && item.seq < total ? skuForSeq(item.seq) : null;
    const expected = expectedSku === null ? null : expectedEnrichment(expectedSku);
    if (
      expected === null ||
      item.sku !== expectedSku ||
      item.price !== expected.price ||
      item.stock !== expected.stock
    ) {
      divergences.push(item.seq);
    }
  }

  const missing = Array.from({ length: total }, (_, seq) => seq).filter((seq) => !seen.has(seq));

  return {
    items_received: result.length,
    duplicate_seqs: uniqueSorted(duplicates),
    missing_seqs: missing,
    out_of_order_seqs: uniqueSorted(outOfOrder),
    divergence_seqs: uniqueSorted(divergences),
  };
}

function buildReport(
  run: RunState,
  metrics: MockPlatformMetrics,
  result?: CallbackItem[],
): MockReport {
  const durations = run.ackSamples.map((sample) => sample.durationMs);
  const callback =
    result === undefined
      ? {
          items_received: 0,
          duplicate_seqs: [],
          missing_seqs: [],
          out_of_order_seqs: [],
          divergence_seqs: [],
        }
      : analyzeCallback(run.total, result);
  const reasons: string[] = [];
  const above600 = durations.filter((duration) => duration > 600).length;

  if (above600 > 0) reasons.push('ACK above 600 ms');
  if (metrics.peakEnrichInFlight > MAX_ENRICH_CONCURRENCY)
    reasons.push('enrich concurrency above 3');
  if (callback.missing_seqs.length > 0) reasons.push('missing items');
  if (callback.divergence_seqs.length > 0) reasons.push('price/stock divergence');
  if (callback.duplicate_seqs.length > 0) reasons.push('duplicate callback items');
  if (callback.out_of_order_seqs.length > 0) reasons.push('callback result out of order');

  return {
    run_id: run.runId,
    generated_at: new Date().toISOString(),
    ack: {
      average_ms:
        durations.length === 0
          ? 0
          : durations.reduce((total, duration) => total + duration, 0) / durations.length,
      p95_ms: percentile95(durations),
      max_ms: durations.length === 0 ? 0 : Math.max(...durations),
      above_600_ms: above600,
      timeouts: run.ackSamples.filter((sample) => sample.timedOut).length,
      non_2xx: run.ackSamples.filter((sample) => sample.non2xx).length,
    },
    messages: {
      sent: run.deliveryPlan.length,
      duplicates_sent: run.deliveryPlan.filter((message) => message.duplicate).length,
      expected: run.total,
    },
    callback,
    enrich: {
      by_status: { ...metrics.enrichByStatus },
      peak_in_flight: metrics.peakEnrichInFlight,
    },
    total_time_ms: Date.now() - run.startedAtMs,
    callbacks_for_run: run.callbackCount,
    verdict: {
      status: reasons.length === 0 ? 'PASS' : 'FAIL',
      reasons,
    },
  };
}

function printReport(report: MockReport): void {
  console.log(
    [
      `[mock] run ${report.run_id}: ${report.verdict.status}`,
      `  ACK avg/p95/max: ${report.ack.average_ms.toFixed(1)}/${report.ack.p95_ms.toFixed(1)}/${report.ack.max_ms.toFixed(1)} ms`,
      `  Messages: ${String(report.messages.sent)} sent, ${String(report.messages.duplicates_sent)} duplicates, ${String(report.messages.expected)} expected`,
      `  Callback: ${String(report.callback.items_received)} items, missing [${report.callback.missing_seqs.join(', ')}]`,
      `  Enrich peak: ${String(report.enrich.peak_in_flight)}, statuses ${JSON.stringify(report.enrich.by_status)}`,
      `  Reasons: ${report.verdict.reasons.join('; ') || 'none'}`,
    ].join('\n'),
  );
}

export function createMockPlatform(options: MockPlatformOptions = {}): MockPlatform {
  const resolvedOptions = resolveOptions(options);
  const random = mulberry32(resolvedOptions.seed);
  const app = Fastify({
    logger: resolvedOptions.logger,
    logController: new LogController({ disableRequestLogging: true }),
  });
  const registrations = new Map<string, Registration>();
  const runs = new Map<string, RunState>();
  const reports: MockReport[] = [];
  const metrics: MockPlatformMetrics = {
    enrichByStatus: { 200: 0, 401: 0, 404: 0, 429: 0, 500: 0 },
    enrichInFlight: 0,
    peakEnrichInFlight: 0,
  };
  let startedUrl: string | undefined;
  let stopped = false;

  function countEnrichStatus(status: EnrichStatus): void {
    metrics.enrichByStatus[status] += 1;
  }

  function recordAck(
    run: RunState,
    durationMs: number,
    timedOut: boolean,
    non2xx: boolean,
    warmup: boolean,
  ): void {
    if (!warmup) {
      run.ackSamples.push({ durationMs, timedOut, non2xx });
    }
  }

  async function deliverMessage(run: RunState, message: DispatchMessage): Promise<void> {
    const registration = registrations.get(run.cid);
    if (registration === undefined) {
      throw new Error(`Registration ${run.cid} disappeared during dispatch`);
    }

    for (let attempt = 1; attempt <= MAX_DELIVERY_ATTEMPTS; attempt += 1) {
      const warmup = run.sendAttemptsStarted === 0;
      run.sendAttemptsStarted += 1;
      const startedAt = performance.now();

      try {
        const response = await fetch(appendPath(registration.webhook, 'process'), {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ run_id: run.runId, seq: message.seq, sku: message.sku }),
          signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
        });
        const durationMs = performance.now() - startedAt;
        const non2xx = !response.ok;
        recordAck(run, durationMs, false, non2xx, warmup);

        if (response.ok) {
          return;
        }
      } catch (error) {
        const durationMs = performance.now() - startedAt;
        const timedOut = error instanceof Error && error.name === 'TimeoutError';
        recordAck(run, durationMs, timedOut, !timedOut, warmup);
      }
    }
  }

  async function completeDispatch(run: RunState, deliveries: DispatchMessage[]): Promise<void> {
    try {
      await Promise.all(deliveries.map((message) => deliverMessage(run, message)));
      reports.push(buildReport(run, metrics));
      run.dispatch.resolve();
    } catch (error) {
      run.dispatch.reject(error);
    }
  }

  app.post('/register', async (request, reply) => {
    const parsedBody = registerBodySchema.safeParse(request.body);
    if (!parsedBody.success) {
      return reply.status(400).send({ error: 'invalid_payload' });
    }

    const cid = randomId('cid', random);
    const token = randomId('token', random);

    try {
      const response = await fetch(appendPath(parsedBody.data.webhook, 'check'), {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ token }),
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      });
      const responseBody: unknown = await response.json();
      const parsedResponse = checkResponseSchema.safeParse(responseBody);

      if (!response.ok || !parsedResponse.success || parsedResponse.data.token !== token) {
        return await reply.status(422).send({
          error: 'handshake_failed',
          reason: 'webhook did not echo the validation token',
        });
      }

      registrations.set(cid, { cid, token, webhook: parsedBody.data.webhook });
      return await reply.status(200).send({ cid, token });
    } catch (error) {
      const reason = error instanceof Error ? error.message : 'unknown handshake error';
      return reply.status(422).send({ error: 'handshake_failed', reason });
    }
  });

  app.post<{ Params: { cid: string } }>('/burst/:cid', async (request, reply) => {
    const registration = registrations.get(request.params.cid);
    if (registration === undefined || request.headers['x-token'] !== registration.token) {
      return reply.status(401).send({ error: 'unauthorized' });
    }

    const runId = randomId('run', random);
    const startedAtMs = Date.now();
    const run: RunState = {
      runId,
      cid: registration.cid,
      total: resolvedOptions.total,
      startedAt: new Date(startedAtMs).toISOString(),
      startedAtMs,
      deliveryPlan: createDispatchPlan(
        resolvedOptions.total,
        resolvedOptions.dupRate,
        resolvedOptions.dropSeqs,
        random,
      ),
      ackSamples: [],
      sendAttemptsStarted: 0,
      dispatch: createDeferred(),
      callbackCount: 0,
    };
    runs.set(runId, run);

    if (resolvedOptions.earlyDispatch && run.deliveryPlan.length > 0) {
      const earlyDeliveries = run.deliveryPlan.slice(0, Math.min(3, run.deliveryPlan.length));
      const remainingDeliveries = run.deliveryPlan.slice(earlyDeliveries.length);
      await Promise.all(earlyDeliveries.map((message) => deliverMessage(run, message)));
      setImmediate(() => {
        void completeDispatch(run, remainingDeliveries);
      });
    } else {
      setImmediate(() => {
        void completeDispatch(run, run.deliveryPlan);
      });
    }

    return reply.status(200).send({
      run_id: run.runId,
      total: run.total,
      started_at: run.startedAt,
    });
  });

  app.get<{ Params: { sku: string } }>('/enrich/:sku', async (request, reply) => {
    const cid = request.headers['x-cid'];
    const token = request.headers['x-token'];
    const registration = typeof cid === 'string' ? registrations.get(cid) : undefined;

    if (registration === undefined || token !== registration.token) {
      countEnrichStatus(401);
      return reply.status(401).send({ error: 'unauthorized' });
    }

    const expected = expectedEnrichment(request.params.sku);
    const skuMatch = /^sku-(\d{3})$/.exec(request.params.sku);
    const skuSeq = skuMatch === null ? -1 : Number(skuMatch[1]) - 1;
    const knownSku = [...runs.values()].some(
      (run) => run.cid === registration.cid && skuSeq >= 0 && skuSeq < run.total,
    );

    if (expected === null || !knownSku || resolvedOptions.invalidSkuSeqs.has(skuSeq)) {
      countEnrichStatus(404);
      return reply.status(404).send({ error: 'sku_not_found' });
    }

    if (metrics.enrichInFlight >= MAX_ENRICH_CONCURRENCY) {
      countEnrichStatus(429);
      return reply.header('retry-after', '1').status(429).send({ error: 'rate_limited' });
    }

    metrics.enrichInFlight += 1;
    metrics.peakEnrichInFlight = Math.max(metrics.peakEnrichInFlight, metrics.enrichInFlight);
    const shouldFail = random() < resolvedOptions.errorRate;
    const latencyMs = 400 + Math.floor(random() * 401);

    try {
      await new Promise<void>((resolveDelay) => {
        setTimeout(resolveDelay, latencyMs);
      });

      if (shouldFail) {
        countEnrichStatus(500);
        return await reply.status(500).send({ error: 'transient_error' });
      }

      countEnrichStatus(200);
      return await reply.status(200).send({ sku: request.params.sku, ...expected });
    } finally {
      metrics.enrichInFlight -= 1;
    }
  });

  app.post('/callback', async (request, reply) => {
    const token = request.headers['x-token'];
    const authenticatedRegistration = [...registrations.values()].find(
      (registration) => registration.token === token,
    );
    if (authenticatedRegistration === undefined) {
      return reply.status(401).send({ error: 'unauthorized' });
    }

    const parsedBody = callbackBodySchema.safeParse(request.body);
    if (!parsedBody.success) {
      return reply.status(400).send({ error: 'invalid_payload' });
    }
    if (parsedBody.data.cid !== authenticatedRegistration.cid) {
      return reply.status(401).send({ error: 'unauthorized' });
    }

    const run = runs.get(parsedBody.data.run_id);
    if (run?.cid !== authenticatedRegistration.cid) {
      return reply.status(400).send({ error: 'unknown_run' });
    }

    run.callbackCount += 1;
    const report = buildReport(run, metrics, parsedBody.data.result);
    await mkdir(resolvedOptions.reportsDir, { recursive: true });
    await writeFile(
      resolve(resolvedOptions.reportsDir, `${run.runId}-${String(run.callbackCount)}.json`),
      `${JSON.stringify(report, null, 2)}\n`,
      'utf8',
    );
    reports.push(report);
    if (resolvedOptions.printReports) {
      printReport(report);
    }

    return reply.status(200).send(report);
  });

  return {
    app,
    reports,
    metrics,
    async start() {
      if (startedUrl !== undefined) {
        return startedUrl;
      }

      await app.listen({ port: resolvedOptions.port, host: '127.0.0.1' });
      const address = app.server.address();
      if (address === null || typeof address === 'string') {
        throw new Error('Mock platform did not bind to a TCP port');
      }
      startedUrl = `http://127.0.0.1:${String(address.port)}`;
      return startedUrl;
    },
    async stop() {
      if (stopped) {
        return;
      }
      stopped = true;
      await Promise.allSettled([...runs.values()].map((run) => run.dispatch.promise));
      await app.close();
    },
    async waitForDispatch(runId: string) {
      const run = runs.get(runId);
      if (run === undefined) {
        throw new Error(`Unknown run: ${runId}`);
      }
      await run.dispatch.promise;
    },
  };
}

function parseSeqList(value: string): number[] {
  if (value.trim() === '') {
    return [];
  }

  return value.split(',').map((part) => {
    const seq = Number(part.trim());
    if (!Number.isInteger(seq) || seq < 0) {
      throw new Error(`Invalid sequence number: ${part}`);
    }
    return seq;
  });
}

const cliOptionsSchema = z.object({
  MOCK_PORT: z.coerce.number().int().min(0).max(65_535).default(5000),
  TOTAL: z.coerce.number().int().positive().default(20),
  DUP_RATE: z.coerce.number().min(0).max(1).default(0.25),
  ERROR_RATE: z.coerce.number().min(0).max(1).default(0.1),
  SEED: z.coerce.number().int().default(42),
  DROP_SEQS: z.string().default(''),
  INVALID_SKU_SEQS: z.string().default(''),
  EARLY_DISPATCH: z.enum(['true', 'false']).default('false'),
});

async function runCli(): Promise<void> {
  const cliOptions = cliOptionsSchema.parse(process.env);
  const platform = createMockPlatform({
    port: cliOptions.MOCK_PORT,
    total: cliOptions.TOTAL,
    dupRate: cliOptions.DUP_RATE,
    errorRate: cliOptions.ERROR_RATE,
    seed: cliOptions.SEED,
    dropSeqs: parseSeqList(cliOptions.DROP_SEQS),
    invalidSkuSeqs: parseSeqList(cliOptions.INVALID_SKU_SEQS),
    earlyDispatch: cliOptions.EARLY_DISPATCH === 'true',
    logger: true,
    printReports: true,
  });
  const url = await platform.start();
  console.log(`[mock] platform listening at ${url}`);

  const stop = () => {
    void platform.stop();
  };
  process.once('SIGINT', stop);
  process.once('SIGTERM', stop);
}

const isCliEntrypoint = resolve(process.argv[1]) === fileURLToPath(import.meta.url);

if (isCliEntrypoint) {
  try {
    await runCli();
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unknown mock platform error';
    console.error(`[mock] failed to start: ${message}`);
    process.exitCode = 1;
  }
}
