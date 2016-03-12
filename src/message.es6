import _ from 'lodash';
import {trace} from '@synccloud/logging';
import {AmqpConsumerError, AmqpConsumerCanceledError, AmqpConsumerCancelTimeoutError} from './errors';

export default class AmqpMessage {
  get consumer() {
    return this._consumer;
  }

  get status() {
    return this._status || 'non-acked';
  }

  get deliveryTag() {
    return this.fields && this.fields.deliveryTag;
  }

  get redeliveredCount() {
    return (this.properties && this.properties.headers &&
      parseInt(this.properties.headers['x-redelivered-count'])) || 0;
  }

  get headers() {
    return this.properties.headers;
  }

  constructor(consumer) {
    this._consumer = consumer;
  }

  toJSON() {
    return {
      fields: this.fields,
      properties: this.properties,
      content: this.content ? {
        $type: 'byte[]',
        length: this.content.length
      } : void 0
    };
  }

  @trace
  async ackAsync() {
    if (this.status !== 'non-acked') {
      throw new Error(`Can not ack message with status ${this.status}`);
    }
    try {
      this._status = 'acked';
      this.consumer.channel._channel.ack(this);
    }
    catch (exc) {
      throw AmqpConsumerError.wrap(this.consumer, exc);
    }
  }

  @trace
  async dequeueAsync() {
    if (this.status !== 'non-acked') {
      throw new Error(`Can not dequeue message with status ${this.status}`);
    }
    try {
      this._status = 'dequeued';
      this.consumer.channel._channel.reject(this, !'requeue');
    }
    catch (exc) {
      throw AmqpConsumerError.wrap(this.consumer, exc);
    }
  }

  @trace
  async requeueAsync() {
    if (this.status !== 'non-acked') {
      throw new Error(`Can not requeue message with status ${this.status}`);
    }
    try {
      this._status = 'requeued';
      this.consumer.channel._channel.reject(this, !!'requeue');
    }
    catch (exc) {
      throw AmqpConsumerError.wrap(this.consumer, exc);
    }
  }

  @trace
  async tryRequeueAsync() {
    if (this.status === 'non-acked') {
      await this.requeueAsync();
    }
  }

  @trace
  async redeliverAsync() {
    const properties = _.merge(this.properties, {
      headers: {'x-redelivered-count': this.redeliveredCount + 1}
    });

    await this.consumer
      .channel._channel.publish(
        this.fields.exchange,
        this.fields.routingKey,
        this.content, properties);

    await this.consumer
      .channel._channel
      .waitForConfirms();
  }
}
