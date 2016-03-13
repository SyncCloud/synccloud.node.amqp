import {Log} from '@synccloud/logging'

export default async function logError(ctx, next) {
  const {message} = ctx;
  try {
    await next();
  } catch(err) {
    Log.error(
      () => ({
        msg: 'Error handling message',
        ctx,
        message,
        exception: err
      }),
      ({message:m}) => `${m.msg} (TAG=${m.message.fields.deliveryTag}):\n${Log.format(m.exception)}`
    );
    throw err;
  }
}
