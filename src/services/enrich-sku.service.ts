import { Worker } from 'bullmq';

import type { Enrichment } from '../clients/weduu.client.js';
import { EnrichError } from '../domain/errors.js';
import { decideRetry } from '../lib/retry.js';
import type { RetryPolicy } from '../lib/retry.js';
import type { SkuJobData } from '../queues/sku.queue.js';
import type { ItemErrorDetails, ItemRecord } from '../repositories/item.repository.js';

interface SkuJob {
  data: SkuJobData;
}

export interface EnrichClientPort {
  enrich: (sku: string) => Promise<Enrichment>;
}

export interface ItemRepositoryPort {
  getItem: (runId: string, seq: number) => Promise<ItemRecord | null>;
  markProcessing: (runId: string, seq: number) => Promise<boolean>;
  completeItem: (
    runId: string,
    seq: number,
    data: { price: number; stock: number },
  ) => Promise<boolean>;
  failItem: (
    runId: string,
    seq: number,
    data: ItemErrorDetails & { attempts: number },
  ) => Promise<boolean>;
  recordRetry: (
    runId: string,
    seq: number,
    data: { attempts: number; rateLimitWaits: number; lastError: ItemErrorDetails },
  ) => Promise<boolean>;
}

export interface EnrichLogger {
  info(context: Record<string, unknown>, message: string): void;
}

export interface ProcessSkuJobDependencies {
  client: EnrichClientPort;
  itemRepository: ItemRepositoryPort;
  policy: Omit<RetryPolicy, 'random'>;
  random: () => number;
  rateLimit: (delayMs: number) => Promise<void>;
  logger: EnrichLogger;
}

function isTerminal(item: ItemRecord): boolean {
  return item.status === 'completed' || item.status === 'failed';
}

function errorDetails(error: EnrichError): ItemErrorDetails {
  return {
    kind: error.kind,
    statusCode: error.statusCode,
    message: error.message,
  };
}

function logAttempt(
  deps: ProcessSkuJobDependencies,
  job: SkuJobData,
  attempt: number,
  startedAt: number,
  kind: string,
  action: string,
  statusCode?: number,
): void {
  deps.logger.info(
    {
      run_id: job.runId,
      seq: job.seq,
      sku: job.sku,
      attempt,
      status_code: statusCode,
      duration_ms: performance.now() - startedAt,
      kind,
      action,
    },
    'SKU enrich attempt finished',
  );
}

export async function processSkuJob(
  job: SkuJob,
  deps: ProcessSkuJobDependencies,
): Promise<void> {
  const { runId, seq, sku } = job.data;
  const item = await deps.itemRepository.getItem(runId, seq);
  if (item === null) {
    throw new Error(`Item ${runId}:${String(seq)} does not exist`);
  }
  if (isTerminal(item)) {
    return;
  }

  const marked = await deps.itemRepository.markProcessing(runId, seq);
  if (!marked) {
    const currentItem = await deps.itemRepository.getItem(runId, seq);
    if (currentItem !== null && isTerminal(currentItem)) {
      return;
    }
    throw new Error(`Item ${runId}:${String(seq)} is not ready for processing`);
  }

  const attempt = item.attempts + 1;
  const startedAt = performance.now();

  try {
    const enrichment = await deps.client.enrich(sku);
    const completed = await deps.itemRepository.completeItem(runId, seq, enrichment);
    if (!completed) {
      throw new Error(`Item ${runId}:${String(seq)} could not be marked completed`);
    }
    logAttempt(deps, job.data, attempt, startedAt, 'success', 'completed');
  } catch (error) {
    if (!(error instanceof EnrichError)) {
      logAttempt(deps, job.data, attempt, startedAt, 'internal', 'error');
      throw error;
    }

    const decision = decideRetry(
      error,
      {
        attemptsMade: error.kind === 'rate_limited' ? item.attempts : attempt,
        rateLimitWaits: item.rateLimitWaits,
      },
      { ...deps.policy, random: deps.random },
    );
    const details = errorDetails(error);

    if (decision.action === 'fail') {
      const failed = await deps.itemRepository.failItem(runId, seq, {
        ...details,
        attempts: error.kind === 'rate_limited' ? item.attempts : attempt,
      });
      if (!failed) {
        throw new Error(`Item ${runId}:${String(seq)} could not be marked failed`);
      }
      logAttempt(deps, job.data, attempt, startedAt, error.kind, 'fail', error.statusCode);
      return;
    }

    if (decision.action === 'retry') {
      const recorded = await deps.itemRepository.recordRetry(runId, seq, {
        attempts: attempt,
        rateLimitWaits: item.rateLimitWaits,
        lastError: details,
      });
      if (!recorded) {
        throw new Error(`Retry for item ${runId}:${String(seq)} could not be recorded`);
      }
      logAttempt(deps, job.data, attempt, startedAt, error.kind, 'retry', error.statusCode);
      throw error;
    }

    const recorded = await deps.itemRepository.recordRetry(runId, seq, {
      attempts: item.attempts,
      rateLimitWaits: item.rateLimitWaits + 1,
      lastError: details,
    });
    if (!recorded) {
      throw new Error(`Rate-limit wait for item ${runId}:${String(seq)} could not be recorded`);
    }
    await deps.rateLimit(decision.delayMs);
    logAttempt(
      deps,
      job.data,
      attempt,
      startedAt,
      error.kind,
      'rate_limit_wait',
      error.statusCode,
    );
    throw Worker.RateLimitError();
  }
}
