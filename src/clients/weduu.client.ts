import { z } from 'zod';

import { EnrichError } from '../domain/errors.js';
import { Semaphore } from '../lib/semaphore.js';

const DEFAULT_RETRY_AFTER_MS = 1_000;
const MAX_RETRY_AFTER_MS = 30_000;

const enrichmentSchema = z.object({
  sku: z.string(),
  price: z.number().finite().min(0),
  stock: z.number().int().min(0),
});

export interface Enrichment {
  sku: string;
  price: number;
  stock: number;
}

export interface WeduuClientOptions {
  baseUrl: string;
  cid: string;
  token: string;
  timeoutMs: number;
  maxInFlight?: number;
  fetchImpl?: typeof fetch;
}

function enrichUrl(baseUrl: string, sku: string): string {
  const normalizedBaseUrl = baseUrl.endsWith('/') ? baseUrl : `${baseUrl}/`;
  return new URL(`enrich/${encodeURIComponent(sku)}`, normalizedBaseUrl).toString();
}

function retryAfterMs(value: string | null): number {
  if (value === null) {
    return DEFAULT_RETRY_AFTER_MS;
  }

  const trimmedValue = value.trim();
  if (/^\d+$/.test(trimmedValue)) {
    const seconds = Number(trimmedValue);
    return Number.isFinite(seconds)
      ? Math.min(seconds * 1_000, MAX_RETRY_AFTER_MS)
      : MAX_RETRY_AFTER_MS;
  }

  const retryAt = Date.parse(trimmedValue);
  if (Number.isNaN(retryAt)) {
    return DEFAULT_RETRY_AFTER_MS;
  }

  return Math.min(Math.max(retryAt - Date.now(), 0), MAX_RETRY_AFTER_MS);
}

function isTransportError(error: unknown): boolean {
  return (
    error instanceof TypeError ||
    (error instanceof Error && (error.name === 'AbortError' || error.name === 'TimeoutError'))
  );
}

async function cancelResponseBody(response: Response): Promise<void> {
  if (response.body !== null && !response.bodyUsed) {
    await response.body.cancel();
  }
}

async function parseEnrichment(response: Response, requestedSku: string): Promise<Enrichment> {
  let body: unknown;

  try {
    body = await response.json();
  } catch (error) {
    if (isTransportError(error)) {
      throw error;
    }

    throw new EnrichError({
      kind: 'permanent',
      statusCode: response.status,
      message: 'invalid_response',
      cause: error,
    });
  }

  const parsedBody = enrichmentSchema.safeParse(body);
  if (!parsedBody.success || parsedBody.data.sku !== requestedSku) {
    throw new EnrichError({
      kind: 'permanent',
      statusCode: response.status,
      message: 'invalid_response',
    });
  }

  return parsedBody.data;
}

export class WeduuClient {
  private readonly baseUrl: string;
  private readonly cid: string;
  private readonly token: string;
  private readonly timeoutMs: number;
  private readonly fetchImpl: typeof fetch;
  private readonly semaphore: Semaphore;

  constructor(options: WeduuClientOptions) {
    if (!Number.isInteger(options.timeoutMs) || options.timeoutMs < 1) {
      throw new Error('timeoutMs must be a positive integer');
    }

    this.baseUrl = options.baseUrl;
    this.cid = options.cid;
    this.token = options.token;
    this.timeoutMs = options.timeoutMs;
    this.fetchImpl = options.fetchImpl ?? fetch;
    this.semaphore = new Semaphore(options.maxInFlight ?? 3);
  }

  async enrich(sku: string): Promise<Enrichment> {
    await this.semaphore.acquire();

    try {
      const signal = AbortSignal.timeout(this.timeoutMs);
      const response = await this.fetchImpl(enrichUrl(this.baseUrl, sku), {
        method: 'GET',
        headers: {
          'x-cid': this.cid,
          'x-token': this.token,
        },
        signal,
      });

      if (response.status === 200) {
        return await parseEnrichment(response, sku);
      }

      const parsedRetryAfterMs =
        response.status === 429 ? retryAfterMs(response.headers.get('retry-after')) : undefined;
      await cancelResponseBody(response);

      if (response.status === 429) {
        throw new EnrichError({
          kind: 'rate_limited',
          statusCode: response.status,
          retryAfterMs: parsedRetryAfterMs,
          message: 'rate_limited',
        });
      }

      if (response.status >= 500) {
        throw new EnrichError({
          kind: 'transient',
          statusCode: response.status,
          message: 'upstream_error',
        });
      }

      throw new EnrichError({
        kind: 'permanent',
        statusCode: response.status,
        message: 'permanent_http_error',
      });
    } catch (error) {
      if (error instanceof EnrichError) {
        throw error;
      }

      throw new EnrichError({
        kind: 'transient',
        message:
          error instanceof Error && (error.name === 'AbortError' || error.name === 'TimeoutError')
            ? 'timeout'
            : 'network_error',
        cause: error,
      });
    } finally {
      this.semaphore.release();
    }
  }
}
