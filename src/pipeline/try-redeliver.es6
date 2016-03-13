import {Log} from '@synccloud/logging';

export default function({maxRedeliveredCount}) {
  return async function redeliver({message}, next) {
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
