export default function init({channel, exchange, routingKey, options = {}}) {
  return async function publish({message, application:app}, next) {
    await next();

    const body = new Buffer(JSON.stringify(message), 'utf8');

    await channel.publishAsync({
      exchange,
      routingKey,
      body,
      ...options
    })
  }
}
