import assert from 'node:assert/strict';
import test from 'node:test';
import { AsyncChannel, ChannelAbortedError, ChannelClosedError } from '../dist/esm/index.js';

test('validates capacity and defaults to unbounded', () => {
  assert.equal(new AsyncChannel().capacity, Infinity);
  for (const capacity of [0, -1, 1.5, NaN]) {
    assert.throws(() => new AsyncChannel({ capacity }), RangeError);
  }
});

test('delivers in FIFO order and drains after close', async () => {
  const channel = new AsyncChannel({ capacity: 2 });
  await channel.send(1);
  await channel.send(2);
  const waiting = channel.send(3);
  assert.equal(channel.pendingSenders, 1);
  assert.deepEqual(await channel.receive(), { done: false, value: 1 });
  await waiting;
  channel.close();
  assert.deepEqual(await channel.receive(), { done: false, value: 2 });
  assert.deepEqual(await channel.receive(), { done: false, value: 3 });
  assert.deepEqual(await channel.receive(), { done: true, value: undefined });
  assert.equal(channel.isClosed, true);
});

test('hands directly to waiting receivers and cancels individual operations', async () => {
  const channel = new AsyncChannel();
  const read = channel.receive();
  await channel.send('delivered');
  assert.deepEqual(await read, { done: false, value: 'delivered' });

  const controller = new AbortController();
  const pending = channel.receive({ signal: controller.signal });
  controller.abort();
  await assert.rejects(pending, { name: 'AbortError' });
  assert.equal(channel.pendingReceivers, 0);

  const senderController = new AbortController();
  const bounded = new AsyncChannel({ capacity: 1 });
  await bounded.send(1);
  const blocked = bounded.send(2, { signal: senderController.signal });
  senderController.abort('cancelled');
  await assert.rejects(blocked, ChannelAbortedError);
  assert.equal(bounded.pendingSenders, 0);
  assert.deepEqual(await bounded.receive(), { done: false, value: 1 });
  await bounded.send(3);
  assert.deepEqual(await bounded.receive(), { done: false, value: 3 });
});

test('multiple producers, readers, and terminal transitions preserve FIFO and settle waiters', async () => {
  const channel = new AsyncChannel({ capacity: 1 });
  await channel.send(0);
  const first = channel.send(1);
  const second = channel.send(2);
  assert.deepEqual(await channel.receive(), { done: false, value: 0 });
  await first;
  assert.deepEqual(await channel.receive(), { done: false, value: 1 });
  await second;
  channel.close();
  assert.deepEqual(await channel.receive(), { done: false, value: 2 });
  assert.deepEqual(await channel.receive(), { done: true, value: undefined });

  const errorChannel = new AsyncChannel();
  const failure = new Error('consumer failed');
  const readers = [errorChannel.receive(), errorChannel.receive()];
  errorChannel.error(failure);
  await Promise.all(readers.map((reader) => assert.rejects(reader, (error) => error === failure)));

  const closed = new AsyncChannel();
  const waiting = [closed.receive(), closed.receive()];
  closed.close();
  assert.deepEqual(await Promise.all(waiting), [
    { done: true, value: undefined },
    { done: true, value: undefined },
  ]);
});

test('closure rejects producers and error drains buffered values before rejection', async () => {
  const closed = new AsyncChannel({ capacity: 1 });
  await closed.send('queued');
  const blocked = closed.send('blocked');
  closed.close();
  await assert.rejects(blocked, ChannelClosedError);
  assert.deepEqual(await closed.receive(), { done: false, value: 'queued' });
  assert.deepEqual(await closed.receive(), { done: true, value: undefined });
  await assert.rejects(closed.send('late'), ChannelClosedError);

  const failure = new Error('failed');
  const channel = new AsyncChannel();
  await channel.send('before-error');
  channel.error(failure);
  assert.deepEqual(await channel.receive(), { done: false, value: 'before-error' });
  await assert.rejects(channel.receive(), (error) => error === failure);
  assert.throws(() => channel.error('bad'), TypeError);
});

test('iterator return and abort stop pending next calls without closing channel', async () => {
  const channel = new AsyncChannel();
  const iterator = channel.iterator();
  const next = iterator.next();
  const concurrent = iterator.next();
  await iterator.return?.();
  assert.deepEqual(await next, { done: true, value: undefined });
  assert.deepEqual(await concurrent, { done: true, value: undefined });
  assert.equal(channel.isClosed, false);

  const controller = new AbortController();
  const aborting = channel.iterator({ signal: controller.signal });
  const pending = aborting.next();
  controller.abort();
  assert.deepEqual(await pending, { done: true, value: undefined });

  await channel.send('unconsumed');
  const alreadyAborted = new AbortController();
  alreadyAborted.abort();
  assert.deepEqual(await channel.iterator({ signal: alreadyAborted.signal }).next(), {
    done: true,
    value: undefined,
  });
  assert.deepEqual(await channel.receive(), { done: false, value: 'unconsumed' });
});

test('normal async iteration drains and terminates', async () => {
  const channel = new AsyncChannel({ capacity: 2 });
  await channel.send('a');
  await channel.send('b');
  channel.close();
  const values = [];
  for await (const value of channel) values.push(value);
  assert.deepEqual(values, ['a', 'b']);
});
