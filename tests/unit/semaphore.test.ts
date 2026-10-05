import { describe, expect, it } from 'vitest';

import { Semaphore } from '../../src/lib/semaphore.js';

describe('Semaphore', () => {
  it('grants queued slots in FIFO order', async () => {
    const semaphore = new Semaphore(1);
    const acquisitionOrder: string[] = [];
    await semaphore.acquire();

    const waiters = ['first', 'second', 'third'].map(async (label) => {
      await semaphore.acquire();
      acquisitionOrder.push(label);
      semaphore.release();
    });

    semaphore.release();
    await Promise.all(waiters);

    expect(acquisitionOrder).toEqual(['first', 'second', 'third']);
  });

  it('rejects invalid capacity and unmatched release', () => {
    expect(() => new Semaphore(0)).toThrow('positive integer');
    expect(() => {
      new Semaphore(1).release();
    }).toThrow('without an acquired slot');
  });
});
