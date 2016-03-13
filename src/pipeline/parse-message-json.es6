import {Log} from '@synccloud/logging';

export default function init({parseFn=JSON.parse}={}) {
  return async function parseMessageJsonAsync({message}, next) {
    try {
      message.body = parseFn(message.content.toString());
      Log.info(() => ({
        msg: 'Parsed event message',
        message,
        body: message.body,
      }), ({message:m}) => `${m.msg} event=${Log.format(m.body)}`);

    } catch (err) {
      Log.error(
        () => ({
          msg: 'Failed to parse AMQP message',
          message,
          exception: err
        }),
        (x) => `${x.message.msg} (TAG=${x.message.message.fields.deliveryTag}):\n` +
        Log.format(x.message.exception));

      throw new Error({message: 'failed to parse message', err});
    }

    await next();
  }
}
