import channelPackage = require('@lp-labs/async-channel');

const channel = new channelPackage.AsyncChannel<number>();
const send: Promise<void> = channel.send(3);
const error: channelPackage.ChannelClosedError = new channelPackage.ChannelClosedError();
void send;
void error;
// @ts-expect-error channel payloads are typed
void channel.send('wrong');
