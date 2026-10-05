import { z } from 'zod';

import type { CallbackPayload } from '../clients/weduu.client.js';
import type { Run } from '../domain/run.js';
import type { ItemRecord } from '../repositories/item.repository.js';

const callbackItemSchema = z.object({
  seq: z.number().int().min(0),
  sku: z.string().min(1),
  price: z.number().finite().min(0),
  stock: z.number().int().min(0),
});

const callbackPayloadSchema = z.object({
  runId: z.string().min(1),
  result: z.array(callbackItemSchema),
});

export interface CallbackRunRepositoryPort {
  getRun(runId: string): Promise<Run | null>;
}

export interface CallbackItemRepositoryPort {
  getItems(runId: string, seqs: readonly number[]): Promise<(ItemRecord | null)[]>;
  listRunItemSeqs(runId: string): Promise<number[]>;
}

export interface CallbackLogger {
  warn(context: Record<string, unknown>, message: string): void;
}

export class CallbackService {
  private readonly failedCounts = new Map<string, number>();

  constructor(
    private readonly runs: CallbackRunRepositoryPort,
    private readonly items: CallbackItemRepositoryPort,
    private readonly logger: CallbackLogger,
  ) {}

  async buildCallbackPayload(runId: string): Promise<CallbackPayload> {
    const run = await this.runs.getRun(runId);
    if (run?.total === undefined) throw new Error(`Run ${runId} has no registered total`);

    const total = run.total;
    const expectedSeqs = Array.from({ length: total }, (_, seq) => seq);
    const records = await this.items.getItems(runId, expectedSeqs);
    const result: CallbackPayload['result'] = [];
    for (let index = 0; index < records.length; index += 1) {
      const item = records[index];
      const seq = index;
      if (item === null) throw new Error(`Run ${runId} is missing item ${String(seq)}`);
      if (item.status === 'completed') {
        if (item.price === undefined || item.stock === undefined) {
          throw new Error(`Completed item ${runId}:${String(seq)} has no result`);
        }
        result.push({ seq, sku: item.sku, price: item.price, stock: item.stock });
      } else if (item.status === 'failed') {
        this.logger.warn(
          {
            run_id: runId,
            seq,
            kind: item.errorKind,
            status_code: item.statusCode,
            error: item.error,
          },
          'omitting failed item from callback',
        );
      } else {
        throw new Error(`Item ${runId}:${String(seq)} is not terminal`);
      }
    }

    const anomalousSeqs = (await this.items.listRunItemSeqs(runId)).filter((seq) => seq >= total);
    if (anomalousSeqs.length > 0) {
      this.logger.warn({ run_id: runId, seqs: anomalousSeqs }, 'items outside registered total');
    }
    this.failedCounts.set(runId, records.length - result.length);
    return callbackPayloadSchema.parse({ runId, result });
  }

  getFailedCount(runId: string): number {
    return this.failedCounts.get(runId) ?? 0;
  }
}
