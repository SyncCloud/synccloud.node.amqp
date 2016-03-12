import {Log} from '@synccloud/logging';

export default function({maxRedeliveredCount}) {
  return async function redeliver({message, application:app}, next) {
    try {
      await next();
    } catch (err) {
      if (message.redeliveredCount < maxRedeliveredCount) {
        try {
          await message.ackAsync();
        }
        catch (exc) {
          Log.warning(
            () => ({
              msg: 'Failed to ack AMQP message after re-delivery',
              app,
              message,
              exception: exc
            }),
            ({message:m}) => `Failed to ack AMQP message (TAG=${m.message.deliveryTag}` +
            `) after re-delivery:\n${Log.format(m.exception)}`);
        }

        try {
          await message.redeliverAsync();
        }
        catch (exc) {
          Log.warning(
            () => ({
              msg: 'Failed to redeliver AMQP message',
              app,
              message,
              exception: exc
            }),
            ({message:m}) => `${m.msg} (TAG=${m.message.deliveryTag}` +
            `):\n${Log.format(m.exception)}`);
        }

        throw err;

      } else {
        throw err;
      }
    }
  }
}
