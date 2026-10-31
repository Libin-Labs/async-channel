# @lp-labs/async-channel

A typed asynchronous FIFO channel for passing values between producers and
consumers. A finite capacity applies backpressure to producers; closing the
channel keeps buffered values readable before iteration completes.

## Import

```ts
import { AsyncChannel, ChannelClosedError } from "@lp-labs/async-channel";

const channel = new AsyncChannel<string>({ capacity: 8 });
```

The package provides ESM and CommonJS entry points with matching TypeScript
declarations. It has no runtime dependencies and requires Node.js 18 or newer.

## API

Runtime exports:

- `AsyncChannel<T>(options?)` constructs a channel. `options.capacity` defaults
  to `Infinity`; otherwise it must be a positive integer.
- `ChannelClosedError` is used when sending after normal closure.
- `ChannelAbortedError` is the fallback abort exception when a signal has no
  `Error` reason. Its `name` is `"AbortError"`.

The `AsyncChannel<T>` instance exposes:

- `capacity`, `size`, `pendingSenders`, `pendingReceivers`, and `isClosed`.
- `send(value, { signal? })`, which resolves when the value is accepted by a
  waiting receiver or enters the buffer. With a full buffer, it waits in FIFO
  order. Closing/erroring rejects queued and future sends. An aborted waiting
  send rejects with the signal's `Error` reason or `ChannelAbortedError`.
- `receive({ signal? })`, which resolves to `{ done: false, value }`, or to
  `{ done: true, value: undefined }` after a normally closed channel is drained.
  An abort while waiting rejects as described above.
- `close()`, which is idempotent and drains queued values before normal
  completion.
- `error(error)`, which closes idempotently with the supplied `Error`. Buffered
  values remain readable; once drained, waiting and future receives reject with
  that same error. Non-`Error` arguments throw `TypeError`.
- `iterator({ signal? })` and `[Symbol.asyncIterator]()`, which provide an async
  iterator. The optional signal ends that iterator when aborted; it does not
  close the channel. Calling the iterator's `return()` ends that iterator
  without closing the channel.

An iterator's natural completion does not close the channel. Use `close()` or
`error()` to signal producer completion. Consumer cancellation is local to that
consumer; a producer awaiting capacity must still be cancelled or released by
the application if no consumer remains.

## Example

```ts
const jobs = new AsyncChannel<{ id: number }>({ capacity: 2 });
const controller = new AbortController();

const consumer = (async () => {
  for await (const job of jobs.iterator({ signal: controller.signal })) {
    console.log(job.id);
  }
})();

await jobs.send({ id: 1 });
await jobs.send({ id: 2 });
jobs.close();
await consumer;
```

Use `receive` when a caller needs cancellation of just one pending read:

```ts
const controller = new AbortController();
const pending = jobs.receive({ signal: controller.signal });
controller.abort();
try {
  await pending;
} catch (error) {
  if (error instanceof Error && error.name === "AbortError") {
    // This read was cancelled; the channel remains open.
  }
}
```

## Typing and limits

`AsyncChannel<T>` carries one `T` through sends, receives, and async iteration.
The channel is in-memory and FIFO. It provides no persistence, message
acknowledgement, retry, or multi-channel selection. Infinite capacity is the
default; choose a finite positive integer to bound buffered items. Waiting
producers and consumers can be cancelled with `AbortSignal`.

## License

[MIT](./LICENSE).

## Support

If this library is useful to you, consider supporting its maintenance with a [donation via PayPal](https://www.paypal.me/LibinPrasanth).

## Author

Created by [Libin Prasanth](https://www.linkedin.com/in/libinprasanth/). Connect on LinkedIn for updates and discussion.
