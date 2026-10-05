import type { Run, RunResolution } from '../domain/run.js';
import type { FinalizeResult } from '../services/run-finalizer.service.js';

interface SweeperRunRepositoryPort {
  listOpenRunIds(): Promise<string[]>;
  getRun(runId: string): Promise<Run | null>;
  getResolution(runId: string, total: number): Promise<RunResolution>;
  markStalled(runId: string, now: number): Promise<boolean>;
  removeOpenRun(runId: string): Promise<void>;
}

interface SweeperLogger {
  debug(context: Record<string, unknown>, message: string): void;
  error(context: Record<string, unknown>, message: string): void;
}

export interface RunSweeperOptions {
  runs: SweeperRunRepositoryPort;
  finalizeRun(runId: string): Promise<FinalizeResult>;
  intervalMs: number;
  stallTimeoutMs: number;
  now?: () => number;
  logger: SweeperLogger;
}

export interface RunSweeper {
  start(): void;
  stop(): Promise<void>;
  tick(): Promise<void>;
}

export function createRunSweeper(options: RunSweeperOptions): RunSweeper {
  const now = options.now ?? Date.now;
  let timer: NodeJS.Timeout | undefined;
  let active: Promise<void> | undefined;

  const sweep = async (): Promise<void> => {
    for (const runId of await options.runs.listOpenRunIds()) {
      try {
        const run = await options.runs.getRun(runId);
        if (run === null || run.status === 'completed' || run.status === 'callback_failed') {
          await options.runs.removeOpenRun(runId);
          continue;
        }
        if (run.total === undefined) {
          options.logger.debug({ run_id: runId }, 'run has no registered total');
          continue;
        }
        const finalized = await options.finalizeRun(runId);
        if (finalized === 'sent' || finalized === 'callback_failed' || finalized === 'done') {
          await options.runs.removeOpenRun(runId);
          continue;
        }
        const lastProgressAt = run.lastProgressAt ?? run.registeredAt ?? now();
        if (finalized === 'incomplete' && now() - lastProgressAt > options.stallTimeoutMs) {
          const resolution = await options.runs.getResolution(runId, run.total);
          if (await options.runs.markStalled(runId, now())) {
            options.logger.error(
              {
                run_id: runId,
                total: run.total,
                resolved_count: run.total - resolution.missingSeqs.length,
                missing_count: resolution.missingSeqs.length,
                missing_seqs: resolution.missingSeqs,
              },
              'run stalled with missing items',
            );
          }
        }
      } catch (error) {
        options.logger.error({ run_id: runId, err: error }, 'run sweep failed');
      }
    }
  };

  return {
    start() {
      timer ??= setInterval(() => {
        active ??= sweep().finally(() => (active = undefined));
      }, options.intervalMs);
      timer.unref();
    },
    async stop() {
      if (timer !== undefined) clearInterval(timer);
      timer = undefined;
      await active;
    },
    async tick() {
      if (active !== undefined) return active;
      active = sweep().finally(() => (active = undefined));
      return active;
    },
  };
}
