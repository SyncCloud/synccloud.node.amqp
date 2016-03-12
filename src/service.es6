import {Deferred, CancellationSource, assertType} from '@synccloud/utils';
import {Log, trace} from '@synccloud/logging';
import AmqpClient from './client';
import BaseConsumer from './consumer';
import pipeline from './pipeline';
import _ from  'lodash';
import B from 'bluebird';
import path from 'path';

export default class AmqpService {
  get completion() {
    return this._completion.promise;
  }

  get cancellation() {
    return this._cancellation.token;
  }

  constructor(options) {
    assertType(options.uri, 'string', 'options.uri');

    this.options = options;
    this._consumers = [];
  }

  async getSinglePubChannel() {
      return AmqpService._pubChannel || (AmqpService._pubChannel = await this.client.channelAsync(!'confirm'));
  }

  async buildAsync(options) {
    const {uri, events, rpc} = this.options;
    try {
      this.client = new AmqpClient(uri);
      await this.client.openAsync();
      const pubChannelConfirm = await this.client.channelAsync(!!'confirm');

      if (events) {
        for (let [fileName, config] of _.pairs(events)) {
          const {
            queue,
            prefetch,
            maxRedeliveredCount,
            expectedMessage,
            exchange,
            routingKey
          } = config;

          const channel = await this.client.channelAsync(!!'confirm');
          prefetch && await channel.prefetchAsync(prefetch);
          const consumer = new BaseConsumer({queue, channel});
          consumer
            .use(pipeline.retry({maxRedeliveredCount}))
            .use(pipeline.ack)
            .use(pipeline.logError)
            .use(async (ctx, next) => {
              ctx.publishAsync = pubChannelConfirm.publishAsync2.bind(pubChannelConfirm); //todo refactor
              await next();
            })
            .use(pipeline.parseJSON());

          if (exchange && routingKey) {
            consumer.use(pipeline.publish({channel: await this.getSinglePubChannel(), exchange, routingKey}))
          }

          expectedMessage && consumer.use(pipeline.assert(expectedMessage));
          const handleEvent = require(path.join(process.cwd(), 'src', 'events', fileName));
          consumer.use(await handleEvent(options));

          this._consumers.push(consumer);
        }
      }

      if (rpc) {
        for (let [componentName, config] of _.pairs(rpc)) {
          const {queue, prefetch, version} = config;
          const channel = await this.getSinglePubChannel();
          prefetch && await channel.prefetchAsync(prefetch);
          const consumer = new BaseConsumer({queue, channel, consumer: {noAck: true}});

          consumer
            .use(pipeline.logError)
            .use(async (ctx, next) => {
              ctx.publishAsync = pubChannelConfirm.publishAsync2.bind(pubChannelConfirm); //todo refactor
              await next();
            })
            .use(pipeline.rpc.version({version}))
            .use(pipeline.parseJSON());

          for (let [fileName, handlerInfo] of _.pairs(config.handlers)) {
            const handleMessage = require(path.join(process.cwd(), 'src', 'rpc', componentName, fileName));
            const {rpcName, expectedMessage = {}, ...handlerOptions} = handlerInfo;
            consumer.use(pipeline.rpc.handler({name:rpcName, version, channel},
              pipeline.assert(expectedMessage),
              await handleMessage({...options, ...handlerOptions}))
            );
          }

          consumer.use(async ({message}) => {
            const rpcName = message.headers['api.rpc-name'];
            throw new Error(`no handler found for rpc-name: ${rpcName}`);
          });

          this._consumers.push(consumer);
        }
      }
    } catch(err) {
      this.client && await this.client.closeAsync();
      throw err;
    }

  }

  @trace
  async runAsync(cancellationToken) {
    cancellationToken.assert();

    this._cancellation = new CancellationSource();
    this._completion = new Deferred();
    cancellationToken.then(() => this._cancellation.cancel(), (err) => this._cancellation.cancel(err));

    try {
      let completionError;
      try {
        // wait for completion any consumer service
        await Promise.race(this._consumers.map((c)=>c.runAsync(this.cancellation)));
      } catch (err) {
        Log.error(() => ({
          msg: 'One of consumers exited with an error',
          error: err
        }), ({message:m})=>`${m.msg}\n${Log.format(m.error)}`);
        completionError = err;
      }

      this._cancellation.cancel(completionError);

      Promise.all(_.map([this._consumers], 'completion'))
        .then(() => this._completion.resolve())
        .catch((err) => this._completion.reject(err));

      await this.completion;
    } finally {
      this.client && await this.client.closeAsync()
    }
  }

}
