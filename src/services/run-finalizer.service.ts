import type { CallbackPayload } from '../clients/weduu.client.js';
import { EnrichError } from '../domain/errors.js';
import type { CallbackClaimResult } from '../domain/run.js';
import { computeBackoffMs } from '../lib/retry.js';
import type { MarkCallbackRetryData } from '../repositories/run.repository.js';

export interface FinalizerRunRepositoryPort {
  claimCallback(runId: string, now: number, leaseMs: number): Promise<CallbackClaimResult>;
  markCallbackSent(runId: string, now: number): Promise<boolean>;
  markCallbackRetry(
    runId: string,
    data: MarkCallbackRetryData,
  ): Promise<'retry' | 'failed' | 'ignored'>;
}

export interface CallbackBuilderPort {
  buildCallbackPayload(runId: string): Promise<CallbackPayload>;
  getFailedCount?(runId: string): number;
}

export interface CallbackClientPort {
  sendCallback(payload: CallbackPayload): Promise<void>;
}

export interface FinalizerLogger {
  info(context: Record<string, unknown>, message: string): void;
  error(context: Record<string, unknown>, message: string): void;
}

export type FinalizeResult =
  | Exclude<CallbackClaimResult, { claimed: true; attempt: number }>
  | 'sent'
  | 'retry_scheduled'
  | 'callback_failed';

export interface RunFinalizerOptions {
  runs: FinalizerRunRepositoryPort;
  callbackService: CallbackBuilderPort;
  client: CallbackClientPort;
  leaseMs: number;
  backoffBaseMs: number;
  random: () => number;
  now?: () => number;
  logger: FinalizerLogger;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export class RunFinalizer {
  private readonly now: () => number;

  constructor(private readonly options: RunFinalizerOptions) {
    this.now = options.now ?? Date.now;
  }

  async finalize(runId: string): Promise<FinalizeResult> {
    const startedAt = performance.now();
    const claimed = await this.options.runs.claimCallback(runId, this.now(), this.options.leaseMs);
    if (typeof claimed === 'string') return claimed;

    try {
      const payload = await this.options.callbackService.buildCallbackPayload(runId);
      await this.options.client.sendCallback(payload);
      const marked = await this.options.runs.markCallbackSent(runId, this.now());
      if (!marked) throw new Error(`Callback lease for run ${runId} was lost`);
      this.options.logger.info(
        {
          run_id: runId,
          callback_attempt: claimed.attempt,
          items_sent: payload.result.length,
          items_failed: this.options.callbackService.getFailedCount?.(runId) ?? 0,
          duration_ms: performance.now() - startedAt,
        },
        'run callback sent',
      );
      return 'sent';
    } catch (error) {
      const permanent = error instanceof EnrichError && error.kind === 'permanent';
      const delayMs = computeBackoffMs(claimed.attempt, {
        baseMs: this.options.backoffBaseMs,
        maxMs: this.options.backoffBaseMs * 16,
        random: this.options.random,
      });
      const outcome = await this.options.runs.markCallbackRetry(runId, {
        error: errorMessage(error),
        nextAttemptAt: this.now() + delayMs,
        forceFailed: permanent,
      });
      this.options.logger.error(
        {
          run_id: runId,
          callback_attempt: claimed.attempt,
          kind: permanent ? 'permanent' : 'transient',
          action: outcome,
          error: errorMessage(error),
        },
        'run callback failed',
      );
      return outcome === 'failed' ? 'callback_failed' : 'retry_scheduled';
    }
  }
}
