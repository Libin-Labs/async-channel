import { AsyncChannel, ChannelClosedError } from '@lp-labs/async-channel';

const channel = new AsyncChannel<{ id: number }>({ capacity: 2 });
const send: Promise<void> = channel.send({ id: 1 });
const receive: Promise<IteratorResult<{ id: number }>> = channel.receive();
const closed: ChannelClosedError = new ChannelClosedError();
void send;
void receive;
void closed;
// @ts-expect-error channel payloads are typed
void channel.send({ id: 'wrong' });
