export type RunStatus = 'open' | 'stalled' | 'sending' | 'completed' | 'callback_failed';

export interface Run {
  runId: string;
  total?: number;
  status: RunStatus;
  registeredAt?: number;
  lastProgressAt?: number;
  callbackAttempts: number;
  nextAttemptAt?: number;
  leaseUntil?: number;
  sentAt?: number;
  lastError?: string;
}

export type CallbackClaimResult =
  'no_total' | 'done' | 'busy' | 'wait' | 'incomplete' | { claimed: true; attempt: number };

export interface RunResolution {
  resolvedSeqs: number[];
  missingSeqs: number[];
}
