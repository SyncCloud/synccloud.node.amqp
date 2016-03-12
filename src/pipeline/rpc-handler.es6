import {Log} from '@synccloud/logging'
import compose from 'koa-compose';

export default function createRpcRoute({name, version, channel}, ...handlers) {
  return async function rpcRoute(ctx, next) {
    const {message, application:app} = ctx;
    if (message.headers['api.rpc.name'] == name) {
      try {
        const handler = compose(handlers);
        const response = await handler(ctx);

        Log.info(
          () => ({
            msg: 'Publishing success response',
            response,
            message
          }),
          ({message:m}) => `${m.msg} ${Log.format(m.response)}: from` +
          ` AMQP message (TAG=${m.message.deliveryTag})`);

        const apiResponse = {
          version: version,
          status: "success",
          data: response
        };

        const body = new Buffer(JSON.stringify(apiResponse), 'utf8');

        await channel.publishAsync2({
          exchange: '',
          routingKey: message.properties.replyTo,
          body,
          options: {
            correlationId: message.properties.correlationId,
            headers: {
              "api.request-id": message.properties.headers["api.request-id"],
              "api.status": "success"
            }
          }
        });
      } catch(err) {
        Log.warning(
          () => ({
            msg: 'Publishing error response',
            message,
            exception: err
          }),
          ({message:m}) => `${m.msg} from AMQP message (TAG=${m.message.deliveryTag})\n${Log.format(m.exception)}`);

        const apiResponse = {
          version: version,
          status: "error",
          error: err.toJSON ? err.toJSON() : err.toString()
        };

        const body = new Buffer(JSON.stringify(apiResponse), 'utf8');

        await channel.publishAsync2({
          exchange: '',
          routingKey: message.properties.replyTo,
          body,
          options: {
            correlationId: message.properties.correlationId,
            headers: {
              "api.request-id": message.properties.headers["api.request-id"],
              "api.status": "error",
              "api.error.message": err.message,
              "api.error.code": err.$type
            }
          }
        });
      }
    } else {
      await next();
    }
  }
}
