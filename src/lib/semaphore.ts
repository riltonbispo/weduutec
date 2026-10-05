export class Semaphore {
  private active = 0;
  private readonly waiters: (() => void)[] = [];

  constructor(private readonly capacity: number) {
    if (!Number.isInteger(capacity) || capacity < 1) {
      throw new Error('Semaphore capacity must be a positive integer');
    }
  }

  async acquire(): Promise<void> {
    if (this.active < this.capacity) {
      this.active += 1;
      return;
    }

    await new Promise<void>((resolve) => {
      this.waiters.push(resolve);
    });
  }

  release(): void {
    if (this.active === 0) {
      throw new Error('Cannot release a semaphore without an acquired slot');
    }

    this.active -= 1;
    const next = this.waiters.shift();
    if (next !== undefined) {
      this.active += 1;
      next();
    }
  }
}
