import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { describe, expect, it, vi } from 'vitest';

import { EnrichError } from '../../src/domain/errors.js';
import { RunFinalizer, writeCallbackReport } from '../../src/services/run-finalizer.service.js';
import type {
  CallbackBuilderPort,
  CallbackClientPort,
  FinalizerRunRepositoryPort,
} from '../../src/services/run-finalizer.service.js';

/* eslint-disable @typescript-eslint/unbound-method */

function setup(claim: Awaited<ReturnType<FinalizerRunRepositoryPort['claimCallback']>>) {
  const runs: FinalizerRunRepositoryPort = {
    claimCallback: vi.fn(() => Promise.resolve(claim)),
    markCallbackSent: vi.fn(() => Promise.resolve(true)),
    markCallbackRetry: vi.fn((): Promise<'retry'> => Promise.resolve('retry')),
  };
  const callbackService: CallbackBuilderPort = {
    buildCallbackPayload: vi.fn(() =>
      Promise.resolve({
        runId: 'run-1',
        result: [{ seq: 0, sku: 'sku-001', price: 11.36, stock: 17 }],
      }),
    ),
    getFailedCount: () => 1,
  };
  const client: CallbackClientPort = {
    sendCallback: vi.fn(() => Promise.resolve({ statusCode: 200, body: '{"status":"received"}' })),
  };
  const reportWriter = vi.fn(() => Promise.resolve('/tmp/run-1__attempt-1.json'));
  const logger = { info: vi.fn(), error: vi.fn() };
  const finalizer = new RunFinalizer({
    runs,
    callbackService,
    client,
    leaseMs: 30_000,
    backoffBaseMs: 1_000,
    random: () => 0.5,
    now: () => 10_000,
    logger,
    reportWriter,
  });
  return { finalizer, runs, callbackService, client, reportWriter, logger };
}

describe('RunFinalizer', () => {
  it.each(['incomplete', 'no_total', 'busy', 'wait'] as const)(
    'returns %s without calling the platform',
    async (reason) => {
      const context = setup(reason);
      await expect(context.finalizer.finalize('run-1')).resolves.toBe(reason);
      expect(context.client.sendCallback).not.toHaveBeenCalled();
    },
  );

  it('sends and marks a claimed callback', async () => {
    const context = setup({ claimed: true, attempt: 1 });
    await expect(context.finalizer.finalize('run-1')).resolves.toBe('sent');
    expect(context.client.sendCallback).toHaveBeenCalledOnce();
    expect(context.reportWriter).toHaveBeenCalledWith(
      'run-1',
      1,
      { statusCode: 200, body: '{"status":"received"}' },
      expect.any(String),
    );
    expect(context.runs.markCallbackSent).toHaveBeenCalledWith('run-1', 10_000);
  });

  it('marks the callback sent even when saving the response fails', async () => {
    const context = setup({ claimed: true, attempt: 1 });
    context.reportWriter.mockRejectedValue(new Error('disk full'));

    await expect(context.finalizer.finalize('run-1')).resolves.toBe('sent');

    expect(context.runs.markCallbackSent).toHaveBeenCalledWith('run-1', 10_000);
    expect(context.logger.error).toHaveBeenCalledWith(
      expect.objectContaining({ run_id: 'run-1', error: 'disk full' }),
      'failed to save callback response',
    );
  });

  it('schedules a transient failure with injected backoff and clock', async () => {
    const context = setup({ claimed: true, attempt: 2 });
    vi.mocked(context.client.sendCallback).mockRejectedValue(
      new EnrichError({ kind: 'transient', statusCode: 500, message: 'upstream' }),
    );
    await expect(context.finalizer.finalize('run-1')).resolves.toBe('retry_scheduled');
    expect(context.runs.markCallbackRetry).toHaveBeenCalledWith('run-1', {
      error: 'upstream',
      nextAttemptAt: 11_000,
      forceFailed: false,
    });
  });

  it('makes a permanent HTTP failure terminal', async () => {
    const context = setup({ claimed: true, attempt: 1 });
    vi.mocked(context.client.sendCallback).mockRejectedValue(
      new EnrichError({ kind: 'permanent', statusCode: 400, message: 'rejected' }),
    );
    vi.mocked(context.runs.markCallbackRetry).mockResolvedValue('failed');
    await expect(context.finalizer.finalize('run-1')).resolves.toBe('callback_failed');
    expect(context.runs.markCallbackRetry).toHaveBeenCalledWith(
      'run-1',
      expect.objectContaining({ forceFailed: true }),
    );
  });

  it('releases sending state after an unexpected payload error', async () => {
    const context = setup({ claimed: true, attempt: 1 });
    vi.mocked(context.callbackService.buildCallbackPayload).mockRejectedValue(
      new Error('Redis pipeline failed'),
    );
    await expect(context.finalizer.finalize('run-1')).resolves.toBe('retry_scheduled');
    expect(context.runs.markCallbackRetry).toHaveBeenCalledWith(
      'run-1',
      expect.objectContaining({ error: 'Redis pipeline failed', forceFailed: false }),
    );
  });
});

describe('writeCallbackReport', () => {
  it.each([
    ['JSON', '{"ok":true}', 'json'],
    ['text', 'accepted', 'txt'],
  ])('stores a raw %s response with the matching extension', async (_kind, body, extension) => {
    const directory = await mkdtemp(join(tmpdir(), 'weduutec-report-'));
    try {
      const filePath = await writeCallbackReport(
        'run/unsafe',
        2,
        { statusCode: 200, body },
        directory,
      );
      expect(filePath).toBe(join(directory, `run_unsafe__attempt-2.${extension}`));
      await expect(readFile(filePath, 'utf8')).resolves.toBe(body);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });
});
