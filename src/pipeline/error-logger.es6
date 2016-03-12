import {Log} from '@synccloud/logging'

export default async function logError({message, application:app}, next) {
  try {
    await next();
  } catch(err) {
    Log.error(
      () => ({
        msg: 'Error handling message',
        app,
        message,
        exception: err
      }),
      ({message:m}) => `${m.msg} (TAG=${m.message.fields.deliveryTag}` +
      `):\n${Log.format(m.exception)}`
    );
    throw err;
  }
}
