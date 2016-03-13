import {Log} from '@synccloud/logging';

export default async function ackMessage(ctx, next) {
  const {message} = ctx;
  await next();

  try {
    await message.ackAsync();
  }
  catch (exc) {
    Log.error(
      () => ({
        msg: 'Failed to ack message',
        message,
        ctx,
        exception: exc
      }),
      ({message:m}) => `${m.msg}` +
      ` from AMQP message (TAG=${m.message.fields.deliveryTag}) of ${m.ctx.consumer.queue}:\n${Log.format(m.exception)}`);

    try {
      await message.dequeueAsync();
    }
    catch (exc2) {
      Log.warning(
        () => ({
          msg: 'Failed to dequeue rpc message',
          ctx,
          message,
          exception: exc2
        }),
        ({message:m}) => `${m.msg}` +
        ` from AMQP message (TAG=${m.message.fields.deliveryTag}) of ${m.ctx.consumer.queue}:\n${Log.format(m.exception)}`);
    }

    throw exc;
  }

  Log.info(
    () => ({
      msg: 'Successfully ACKed message',
      ctx,
      message
    }),
    ({message:m}) => `${m.msg} from` +
    ` AMQP message (TAG=${m.message.fields.deliveryTag}) of ${m.ctx.consumer.queue}`);
}
