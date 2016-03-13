export default function init({exchange, routingKey, options = {}}) {
  return async function publish(ctx, next) {
    const {message} = ctx;
    
    await next();

    const body = new Buffer(JSON.stringify(message), 'utf8');

    await ctx.publishAsync({
      exchange,
      routingKey,
      body,
      ...options
    })
  }
}
