import _ from 'lodash';
import {Deferred} from '@synccloud/utils';
import {Log, trace} from '@synccloud/logging';
import {AmqpConsumerError, AmqpConsumerCanceledError, AmqpConsumerCancelTimeoutError} from './errors';
import AmqpMessage from './message';

export default class AmqpConsumer {
  get amqp() {
    return this.channel.amqp;
  }

  get channel() {
    return this._channel;
  }

  get queue() {
    return this._queue;
  }

  get completion() {
    return this._completion.promise;
  }

  get pending() {
    return this._pending;
  }

  get consumerTag() {
    return this._consumerTag;
  }

  get isOpen() {
    return this._completion.isPending && !this._dying;
  }

  constructor(channel, queue, handler) {
    this._channel = channel;
    this._queue = queue;
    this._handler = handler;
    this._completion = new Deferred();
    this._pending = [];

    this.toJSON = function toJSON() {
      return {
        $type: 'AmqpConsumer',
        queue: this.queue,
        consumerTag: this.consumerTag,
        completion: this._completion,
        isOpen: this.isOpen,
        handler: this._handler
      };
    };
  }

  @trace
  init(consumerTag) {
    this._consumerTag = consumerTag;
  }

  @trace
  async cancelAsync(err) {

    if (this.isOpen) {
      this._dying = true;

      await this._waitForPendingHandlersAsync();
      await this._cancelAsync();

      err ? this._completion.reject(err)
        : this._completion.resolve();
    }
  }

  @trace
  async _waitForPendingHandlersAsync() {
    const pending = new Deferred();
    Promise.all(this.pending).then(
      () => pending.resolve(),
      () => pending.resolve());
    setTimeout(() => pending.resolve(), 2000);
    await pending.promise;
  }

  @trace
  async _cancelAsync() {
    if (this._isCanceling || this._wasCanceled) {
      return;
    }

    this._isCanceling = true;

    try {
      await new Promise((resolve, reject) => {
        setTimeout(() => {
          reject(new AmqpConsumerCancelTimeoutError(this));
        }, 3000);

        this.channel._channel.cancel(this.consumerTag)
          .then(() => {
            this._wasCanceled = true;
            resolve()
          }, (err) => reject(err));
      });
    }
    catch (exc) {
      Log.warning(
        () => ({
          msg: 'Failed to cancel AMQP consumer',
          amqp: this.amqp,
          channel: this.channel,
          consumer: this,
          exception: AmqpConsumerError.wrap(this, exc)
        }),
        (x) => `${x.message.msg} of ${x.message.consumer.queue} at ` +
        `${x.message.amqp.uri}:\n${Log.format(x.message.exception)}`);
    } finally {
      delete this._isCanceling;
    }
  }

  @trace
  _handle(message) {
    if (!this.consumerTag) {
      process.nextTick(() => this._handle(message));
    }
    else if (message) {
      process.nextTick(() => this._handleAsync(message));
    }
    else {
      this.cancelAsync(new AmqpConsumerCanceledError(this));
    }
  }

  @trace
  async _handleAsync(message) {
    let pending;
    try {
      pending = new Promise((resolve) => {
        Object.setPrototypeOf(message, new AmqpMessage(this));
        resolve(this._handler(this, message));
      });
      this.pending.push(pending);
      //noinspection BadExpressionStatementJS
      await pending;
    }
    catch (exc) {
      const index = this.pending.indexOf(pending);
      (index >= 0) && this.pending.splice(index, 1);

      Log.error(
        () => ({
          msg: 'Failed to handle AMQP message',
          amqp: this.amqp,
          channel: this.channel,
          consumer: this,
          message: message,
          exception: exc
        }),
        (x) => `${x.message.msg} from ${x.message.consumer.queue} TAG=${x.message.message.fields.deliveryTag} at ${x.message.amqp.uri}:\n` +
        Log.format(x.message.exception));

      // if message handler return unhandled error and message is unacked - dequeue message
      if (message.status == 'non-acked') {
        try {
          await message.dequeueAsync();
        }
        catch (exc2) {
          Log.warning(
            () => ({
              msg: 'Failed to dequeue AMQP message',
              amqp: this.amqp,
              channel: this.channel,
              consumer: this,
              message: message,
              exception: exc2
            }),
            (x) => `${x.message.msg} from ${x.message.consumer.queue} at ${x.message.amqp.uri}:\n` +
            Log.format(x.message.exception));
        }
      }

      try {
        await this.cancelAsync(exc);
      }
      catch (ignore) {
      }
    }
  }
}
