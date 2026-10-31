export interface AsyncChannelOptions {
  readonly capacity?: number;
}

export interface SendOptions {
  readonly signal?: AbortSignal;
}

export interface ReceiveOptions {
  readonly signal?: AbortSignal;
}

export class ChannelClosedError extends Error {
  constructor(message = 'The channel is closed.') {
    super(message);
    this.name = 'ChannelClosedError';
  }
}

export class ChannelAbortedError extends Error {
  constructor(message = 'The channel operation was aborted.') {
    super(message);
    this.name = 'AbortError';
  }
}

interface Sender<T> {
  readonly value: T;
  readonly resolve: () => void;
  readonly reject: (error: Error) => void;
  readonly signal: AbortSignal | undefined;
  readonly abort: () => void;
  active: boolean;
}

interface Receiver<T> {
  readonly resolve: (result: IteratorResult<T>) => void;
  readonly reject: (error: Error) => void;
  readonly signal: AbortSignal | undefined;
  readonly abort: () => void;
  active: boolean;
}

function abortError(reason: unknown): Error {
  if (reason instanceof Error) return reason;
  return new ChannelAbortedError();
}

export class AsyncChannel<T> implements AsyncIterable<T> {
  readonly capacity: number;
  private readonly values: T[] = [];
  private readonly senders: Sender<T>[] = [];
  private readonly receivers: Receiver<T>[] = [];
  private closed = false;
  private failure: Error | undefined;

  constructor(options: AsyncChannelOptions = {}) {
    const capacity = options.capacity ?? Number.POSITIVE_INFINITY;
    if (
      typeof capacity !== 'number' ||
      Number.isNaN(capacity) ||
      capacity <= 0 ||
      (!Number.isInteger(capacity) && capacity !== Number.POSITIVE_INFINITY)
    ) {
      throw new RangeError('Capacity must be a positive integer or Infinity.');
    }
    this.capacity = capacity;
  }

  get isClosed(): boolean {
    return this.closed;
  }

  get size(): number {
    return this.values.length;
  }

  get pendingSenders(): number {
    return this.senders.filter((sender) => sender.active).length;
  }

  get pendingReceivers(): number {
    return this.receivers.filter((receiver) => receiver.active).length;
  }

  send(value: T, options: SendOptions = {}): Promise<void> {
    if (this.closed) return Promise.reject(this.failure ?? new ChannelClosedError());
    const receiver = this.takeReceiver();
    if (receiver) {
      receiver.resolve({ done: false, value });
      return Promise.resolve();
    }
    if (this.values.length < this.capacity && this.pendingSenders === 0) {
      this.values.push(value);
      return Promise.resolve();
    }
    const { signal } = options;
    if (signal?.aborted) return Promise.reject(abortError(signal.reason));
    return new Promise<void>((resolve, reject) => {
      const sender: Sender<T> = {
        value,
        resolve,
        reject,
        signal,
        active: true,
        abort: () => {
          if (!sender.active) return;
          sender.active = false;
          signal?.removeEventListener('abort', sender.abort);
          reject(abortError(signal?.reason));
        },
      };
      this.senders.push(sender);
      signal?.addEventListener('abort', sender.abort, { once: true });
    });
  }

  receive(options: ReceiveOptions = {}): Promise<IteratorResult<T>> {
    const value = this.takeValue();
    if (value.found) return Promise.resolve({ done: false, value: value.value });
    if (this.closed) {
      return this.failure
        ? Promise.reject(this.failure)
        : Promise.resolve({ done: true, value: undefined });
    }
    const { signal } = options;
    if (signal?.aborted) return Promise.reject(abortError(signal.reason));
    return new Promise<IteratorResult<T>>((resolve, reject) => {
      const receiver: Receiver<T> = {
        resolve,
        reject,
        signal,
        active: true,
        abort: () => {
          if (!receiver.active) return;
          receiver.active = false;
          signal?.removeEventListener('abort', receiver.abort);
          reject(abortError(signal?.reason));
        },
      };
      this.receivers.push(receiver);
      signal?.addEventListener('abort', receiver.abort, { once: true });
    });
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.settleReceivers();
    this.rejectSenders(new ChannelClosedError());
  }

  error(error: Error): void {
    if (!(error instanceof Error)) throw new TypeError('Channel error must be an Error.');
    if (this.closed) return;
    this.closed = true;
    this.failure = error;
    this.settleReceivers();
    this.rejectSenders(error);
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return this.iterator();
  }

  iterator(options: ReceiveOptions = {}): AsyncIterator<T> {
    let finished = false;
    const pending = new Set<Receiver<T>>();
    const next = (): Promise<IteratorResult<T>> => {
      if (finished) return Promise.resolve({ done: true, value: undefined });
      if (options.signal?.aborted) {
        finished = true;
        return Promise.resolve({ done: true, value: undefined });
      }
      const value = this.takeValue();
      if (value.found) return Promise.resolve({ done: false, value: value.value });
      if (this.closed) {
        finished = true;
        return this.failure
          ? Promise.reject(this.failure)
          : Promise.resolve({ done: true, value: undefined });
      }
      return new Promise<IteratorResult<T>>((resolve, reject) => {
        const receiver: Receiver<T> = {
          resolve: (result) => {
            pending.delete(receiver);
            if (result.done) finished = true;
            resolve(result);
          },
          reject: (error) => {
            pending.delete(receiver);
            reject(error);
          },
          signal: options.signal,
          active: true,
          abort: () => {
            if (!receiver.active) return;
            receiver.active = false;
            options.signal?.removeEventListener('abort', receiver.abort);
            pending.delete(receiver);
            finished = true;
            resolve({ done: true, value: undefined });
          },
        };
        pending.add(receiver);
        this.receivers.push(receiver);
        options.signal?.addEventListener('abort', receiver.abort, { once: true });
      });
    };
    return {
      next,
      return: async () => {
        finished = true;
        for (const receiver of pending) {
          if (!receiver.active) continue;
          receiver.active = false;
          receiver.signal?.removeEventListener('abort', receiver.abort);
          receiver.resolve({ done: true, value: undefined });
        }
        pending.clear();
        return { done: true, value: undefined };
      },
    };
  }

  private takeValue(): { readonly found: true; readonly value: T } | { readonly found: false } {
    if (this.values.length > 0) {
      const value = this.values.shift() as T;
      this.admitSender();
      return { found: true, value };
    }
    const sender = this.takeSender();
    if (sender) {
      sender.resolve();
      return { found: true, value: sender.value };
    }
    return { found: false };
  }

  private takeReceiver(): Receiver<T> | undefined {
    while (this.receivers.length > 0) {
      const receiver = this.receivers.shift() as Receiver<T>;
      if (!receiver.active) continue;
      receiver.active = false;
      receiver.signal?.removeEventListener('abort', receiver.abort);
      return receiver;
    }
    return undefined;
  }

  private takeSender(): Sender<T> | undefined {
    while (this.senders.length > 0) {
      const sender = this.senders.shift() as Sender<T>;
      if (!sender.active) continue;
      sender.active = false;
      sender.signal?.removeEventListener('abort', sender.abort);
      return sender;
    }
    return undefined;
  }

  private admitSender(): void {
    const sender = this.takeSender();
    if (!sender) return;
    const receiver = this.takeReceiver();
    if (receiver) receiver.resolve({ done: false, value: sender.value });
    else this.values.push(sender.value);
    sender.resolve();
  }

  private settleReceivers(): void {
    if (this.values.length === 0) {
      while (this.closed && this.receivers.length > 0) {
        const receiver = this.takeReceiver();
        if (!receiver) break;
        if (this.failure) receiver.reject(this.failure);
        else receiver.resolve({ done: true, value: undefined });
      }
      return;
    }
    while (this.receivers.length > 0 && this.values.length > 0) {
      const receiver = this.takeReceiver();
      if (!receiver) break;
      const value = this.values.shift() as T;
      receiver.resolve({ done: false, value });
    }
    if (this.values.length === 0) this.settleReceivers();
  }

  private rejectSenders(error: Error): void {
    for (const sender of this.senders.splice(0)) {
      if (!sender.active) continue;
      sender.active = false;
      sender.signal?.removeEventListener('abort', sender.abort);
      sender.reject(error);
    }
  }
}
