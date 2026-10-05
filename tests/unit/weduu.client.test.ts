import { afterEach, describe, expect, it, vi } from 'vitest';

import { WeduuClient } from '../../src/clients/weduu.client.js';
import { EnrichError } from '../../src/domain/errors.js';

const BASE_OPTIONS = {
  baseUrl: 'https://platform.example.test/api',
  cid: 'cid-test',
  token: 'token-secret',
  timeoutMs: 1_000,
} as const;

function jsonResponse(body: unknown, status = 200, headers?: Record<string, string>): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json', ...headers },
  });
}

async function expectEnrichError(promise: Promise<unknown>): Promise<EnrichError> {
  try {
    await promise;
  } catch (error) {
    if (error instanceof EnrichError) {
      return error;
    }
    throw error;
  }

  throw new Error('Expected enrich to reject');
}

function inputUrl(input: string | URL | Request): string {
  return input instanceof Request ? input.url : input.toString();
}

function requestedSku(input: string | URL | Request): string {
  const lastPathSegment = new URL(inputUrl(input)).pathname.split('/').at(-1);
  if (lastPathSegment === undefined) {
    throw new Error('Missing SKU in request URL');
  }
  return decodeURIComponent(lastPathSegment);
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe('WeduuClient response handling', () => {
  it('returns a validated enrichment', async () => {
    const fetchImpl: typeof fetch = () =>
      Promise.resolve(jsonResponse({ sku: 'sku-001', price: 10.5, stock: 7 }));
    const client = new WeduuClient({ ...BASE_OPTIONS, fetchImpl });

    await expect(client.enrich('sku-001')).resolves.toEqual({
      sku: 'sku-001',
      price: 10.5,
      stock: 7,
    });
  });

  it.each([
    [500, 'transient'],
    [503, 'transient'],
    [401, 'permanent'],
    [404, 'permanent'],
    [400, 'permanent'],
  ] as const)('maps status %i to %s', async (statusCode, kind) => {
    const fetchImpl: typeof fetch = () =>
      Promise.resolve(jsonResponse({ error: 'failure' }, statusCode));
    const client = new WeduuClient({ ...BASE_OPTIONS, fetchImpl });

    const error = await expectEnrichError(client.enrich('sku-001'));

    expect(error).toMatchObject({ kind, statusCode });
  });

  it.each(['AbortError', 'TimeoutError'])('maps %s to a transient timeout', async (name) => {
    const sourceError = new Error('timed out');
    sourceError.name = name;
    const fetchImpl: typeof fetch = async () => await Promise.reject(sourceError);
    const client = new WeduuClient({ ...BASE_OPTIONS, fetchImpl });

    const error = await expectEnrichError(client.enrich('sku-001'));

    expect(error).toMatchObject({ kind: 'transient', message: 'timeout', cause: sourceError });
  });

  it('maps network errors to transient', async () => {
    const sourceError = new TypeError('connection reset');
    const fetchImpl: typeof fetch = async () => await Promise.reject(sourceError);
    const client = new WeduuClient({ ...BASE_OPTIONS, fetchImpl });

    const error = await expectEnrichError(client.enrich('sku-001'));

    expect(error).toMatchObject({ kind: 'transient', message: 'network_error' });
    expect(error.cause).toBe(sourceError);
  });

  it.each([
    ['malformed JSON', new Response('{', { status: 200 })],
    ['negative price', jsonResponse({ sku: 'sku-001', price: -1, stock: 1 })],
    ['fractional stock', jsonResponse({ sku: 'sku-001', price: 1, stock: 1.5 })],
  ])('maps an invalid response body (%s) to permanent', async (_case, response) => {
    const fetchImpl: typeof fetch = () => Promise.resolve(response);
    const client = new WeduuClient({ ...BASE_OPTIONS, fetchImpl });

    const error = await expectEnrichError(client.enrich('sku-001'));

    expect(error).toMatchObject({
      kind: 'permanent',
      statusCode: 200,
      message: 'invalid_response',
    });
  });

  it('rejects a response for a different SKU', async () => {
    const fetchImpl: typeof fetch = () =>
      Promise.resolve(jsonResponse({ sku: 'sku-other', price: 1, stock: 1 }));
    const client = new WeduuClient({ ...BASE_OPTIONS, fetchImpl });

    const error = await expectEnrichError(client.enrich('sku-001'));

    expect(error).toMatchObject({ kind: 'permanent', message: 'invalid_response' });
  });

  it('cancels an error response body', async () => {
    const response = new Response('upstream failure', { status: 500 });
    if (response.body === null) {
      throw new Error('Expected a response body');
    }
    const cancel = vi.spyOn(response.body, 'cancel');
    const fetchImpl: typeof fetch = () => Promise.resolve(response);
    const client = new WeduuClient({ ...BASE_OPTIONS, fetchImpl });

    await expect(client.enrich('sku-001')).rejects.toBeInstanceOf(EnrichError);

    expect(cancel).toHaveBeenCalledOnce();
  });
});

describe('WeduuClient retry-after parsing', () => {
  it.each([
    ['seconds', '2', 2_000],
    ['missing', null, 1_000],
    ['invalid', 'later', 1_000],
    ['above the cap', '90', 30_000],
  ])('parses %s retry-after', async (_case, header, expectedDelay) => {
    const fetchImpl: typeof fetch = () =>
      Promise.resolve(
        jsonResponse(
          { error: 'rate_limited' },
          429,
          header === null ? undefined : { 'retry-after': header },
        ),
      );
    const client = new WeduuClient({ ...BASE_OPTIONS, fetchImpl });

    const error = await expectEnrichError(client.enrich('sku-001'));

    expect(error).toMatchObject({
      kind: 'rate_limited',
      statusCode: 429,
      retryAfterMs: expectedDelay,
    });
  });

  it('parses an HTTP date retry-after', async () => {
    const now = Date.now();
    const retryAt = new Date(now + 5_000).toUTCString();
    vi.spyOn(Date, 'now').mockReturnValue(now);
    const fetchImpl: typeof fetch = () =>
      Promise.resolve(
        jsonResponse({ error: 'rate_limited' }, 429, { 'retry-after': retryAt }),
      );
    const client = new WeduuClient({ ...BASE_OPTIONS, fetchImpl });

    const error = await expectEnrichError(client.enrich('sku-001'));

    expect(error.retryAfterMs).toBe(Date.parse(retryAt) - now);
  });
});

describe('WeduuClient request and concurrency', () => {
  it('sends credentials and encodes special characters in the SKU', async () => {
    const sku = 'sku /?#ç';
    let capturedUrl = '';
    let capturedHeaders = new Headers();
    const fetchImpl: typeof fetch = (input, init) => {
      capturedUrl = inputUrl(input);
      capturedHeaders = new Headers(init?.headers);
      return Promise.resolve(jsonResponse({ sku, price: 1, stock: 2 }));
    };
    const client = new WeduuClient({ ...BASE_OPTIONS, fetchImpl });

    await client.enrich(sku);

    expect(capturedUrl).toBe(
      'https://platform.example.test/api/enrich/sku%20%2F%3F%23%C3%A7',
    );
    expect(capturedHeaders.get('x-cid')).toBe('cid-test');
    expect(capturedHeaders.get('x-token')).toBe('token-secret');
  });

  it('limits 30 simultaneous calls to three in flight', async () => {
    let inFlight = 0;
    let peakInFlight = 0;
    const fetchImpl: typeof fetch = async (input) => {
      inFlight += 1;
      peakInFlight = Math.max(peakInFlight, inFlight);
      await delay(5);
      inFlight -= 1;
      return jsonResponse({ sku: requestedSku(input), price: 1, stock: 1 });
    };
    const client = new WeduuClient({ ...BASE_OPTIONS, fetchImpl });

    const results = await Promise.all(
      Array.from({ length: 30 }, async (_, index) => await client.enrich(`sku-${String(index)}`)),
    );

    expect(results).toHaveLength(30);
    expect(peakInFlight).toBeLessThanOrEqual(3);
  });

  it('releases a slot when a request fails', async () => {
    let calls = 0;
    const fetchImpl: typeof fetch = (input) => {
      calls += 1;
      if (calls === 1) {
        return Promise.reject(new TypeError('network failure'));
      }
      return Promise.resolve(jsonResponse({ sku: requestedSku(input), price: 1, stock: 1 }));
    };
    const client = new WeduuClient({ ...BASE_OPTIONS, maxInFlight: 1, fetchImpl });

    const results = await Promise.allSettled([client.enrich('first'), client.enrich('second')]);

    expect(results.map((result) => result.status)).toEqual(['rejected', 'fulfilled']);
    expect(calls).toBe(2);
  });

  it('does not count semaphore waiting time toward the timeout', async () => {
    const signalWasAbortedAtStart: boolean[] = [];
    let calls = 0;
    const fetchImpl: typeof fetch = async (input, init) => {
      calls += 1;
      signalWasAbortedAtStart.push(init?.signal?.aborted ?? false);
      if (calls === 1) {
        await delay(30);
      }
      return jsonResponse({ sku: requestedSku(input), price: 1, stock: 1 });
    };
    const client = new WeduuClient({
      ...BASE_OPTIONS,
      timeoutMs: 10,
      maxInFlight: 1,
      fetchImpl,
    });

    await Promise.all([client.enrich('first'), client.enrich('second')]);

    expect(signalWasAbortedAtStart).toEqual([false, false]);
  });
});
