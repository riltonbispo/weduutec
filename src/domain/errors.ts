export type EnrichErrorKind = 'transient' | 'rate_limited' | 'permanent';

export interface EnrichErrorOptions {
  kind: EnrichErrorKind;
  statusCode?: number;
  retryAfterMs?: number;
  message: string;
  cause?: unknown;
}

export class EnrichError extends Error {
  readonly kind: EnrichErrorKind;
  readonly statusCode: number | undefined;
  readonly retryAfterMs: number | undefined;

  constructor(options: EnrichErrorOptions) {
    super(options.message, { cause: options.cause });
    this.name = 'EnrichError';
    this.kind = options.kind;
    this.statusCode = options.statusCode;
    this.retryAfterMs = options.retryAfterMs;
  }
}
