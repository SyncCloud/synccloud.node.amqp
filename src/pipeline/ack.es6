import {Log} from '@synccloud/logging';

export default async function ackMessage({message, application:app}, next) {
  await next();

  try {
    await message.ackAsync();
  }
  catch (exc) {
    Log.error(
      () => ({
        msg: 'Failed to ack message',
        app,
        message,
        exception: exc
      }),
      ({message:m}) => `${m.msg}` +
      ` from AMQP message (TAG=${m.message.fields.deliveryTag}) of ${m.app.consumer.queue}:\n${Log.format(m.exception)}`);

    try {
      await message.dequeueAsync();
    }
    catch (exc2) {
      Log.warning(
        () => ({
          msg: 'Failed to dequeue rpc message',
          app,
          message,
          exception: exc2
        }),
        ({message:m}) => `${m.msg}` +
        ` from AMQP message (TAG=${m.message.fields.deliveryTag}) of ${m.app.consumer.queue}:\n${Log.format(m.exception)}`);
    }

    throw exc;
  }

  Log.info(
    () => ({
      msg: 'Successfully ACKed message',
      app,
      message
    }),
    ({message:m}) => `${m.msg} from` +
    ` AMQP message (TAG=${m.message.fields.deliveryTag}) of ${m.app.consumer.queue}`);
}
