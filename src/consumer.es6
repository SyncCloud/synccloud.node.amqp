import _ from 'lodash';
import {Deferred, assertType} from '@synccloud/utils';
import {Log, trace} from '@synccloud/logging';
import compose from 'koa-compose';
import Context from './context';

export default class BaseConsumer {
  get amqp() {
    return this.options.amqp;
  }

  get consumer() {
    return this._consumer;
  }

  get completion() {
    return this._completion.promise;
  }

  constructor(options) {
    assertType(options.channel, 'object', 'channel');
    assertType(options.queue, 'string', 'options.queue');
    assertType(options.client, 'object', 'options.client');
    this.options = options;
    this.channel = options.channel;
    this.client = options.client;
    this._middlewares = [];
  }

  use(middleware) {
    assertType(middleware, 'function', 'middleware');

    //Log.debug(()=>({
    //  msg: 'Add middleware',
    //  application: this,
    //  middleware
    //}), ({message:m}) => `${m.msg} ${m.middleware._name || m.middleware.name}`);

    this._middlewares.push(middleware);
    return this;
  }

  @trace
  async runAsync(cancellationToken) {
    const className = this.constructor.name;

    Log.info(
      () => ({
        msg: `Starting ${className} consumer`,
        app: this
      }),
      ({message:m}) => m.msg);

    this._completion = new Deferred();
    this.cancellation = cancellationToken;

    try {
      await this._listenAsync();

      Log.info(
        () => ({
          msg: `Started ${className} consumer`,
          app: this
        }),
        ({message:m}) => m.msg);

      this.cancellation.then(() => this._consumer.cancelAsync(), (err) => this._consumer.cancelAsync(err));
      this._consumer.completion.then(
        () => this._completion.resolve(),
        (err) => this._completion.reject(err));

      await this.completion;
    } catch (err) {
      Log.error(
        () => ({
          msg: `${className} consumer exited with error`,
          app: this,
          exception: err
        }),
        ({message:m}) => `${m.msg}:\n${Log.format(m.exception)}`);

      throw err;
    } finally {
      await this.channel.closeAsync(); // will cancel consumers
    }
  }

  @trace
  async _handleAsync(message) {
    this.cancellation.assert();

    const context = new Context({
      message,
      client: this.client,
      consumer: this._consumer
    });

    const fn = compose(this._middlewares);

    await fn(context);

    Log.info(
      () => ({
        msg: 'Successfully handled message',
        app: this,
        message
      }),
      ({message:m}) => `${m.msg} from AMQP message (TAG=${m.message.deliveryTag}) of ${m.app.consumer.queue}`);
  }

  @trace
  async _listenAsync() {
    this.cancellation.assert();

    this._consumer = await this.channel.consumeAsync(this.options.queue, (consumer, message) => this._consumeAsync(message), this.options.consumer);
  }

  @trace
  async _consumeAsync(message) {
    Log.info(
      () => ({
        msg: 'Received AMQP message',
        app: this,
        message
      }),
      ({message:m}) => `${m.msg} (TAG=${m.message.fields.deliveryTag}) ` +
      `of ${m.app.consumer.queue}`);

    this.cancellation.assert();

    try {
      await this._handleAsync(message);
    }
    catch (exc) {
      await this._handleErrorAsync(message, exc);
    }
  }

  @trace
  async _handleErrorAsync(message, err) {
    try {
      await message.dequeueAsync();
    }
    catch (exc) {
      Log.warning(
        () => ({
          msg: 'Failed to dequeue AMQP message',
          app: this,
          message,
          exception: exc
        }),
        ({message:m}) => `${m.msg} (TAG=${m.message.fields.deliveryTag}):\n${Log.format(m.exception)}`);
    }

    await this._consumer.cancelAsync(err);
  }

}

