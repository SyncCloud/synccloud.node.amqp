import amqplib from 'amqplib';
import {Deferred, Cancellation, CancellationSource} from '@synccloud/utils';
import {Log, trace} from '@synccloud/logging';

import {
  AmqpClientClosedError,
  AmqpClientCloseTimeoutError,
  AmqpRpcFailedError,
  AmqpRpcTimeoutError
} from './errors';

import AmqpChannel from './channel';

export default class AmqpClient {
  static get defaultOptions() {
    return AmqpClient._defaultOptions || {
        replyQueue: false,
        timeout: 5000,
        heartbeat: 10
      };
  }

  static set defaultOptions(value) {
    AmqpClient._defaultOptions = value;
  }

  get uri() {
    return this._uri;
  }

  get channels() {
    return this._channels;
  }

  get completion() {
    return this._completion.promise;
  }

  get isOpen() {
    return this._completion.isPending && !this._dying;
  }

  constructor(uri) {
    this._uri = uri;

    this._channels = null;
    this._exceptions = null;
    this._completion = null;

    this.toJSON = function toJSON() {
      return {
        $type: 'AmqpClient',
        uri: this.uri,
        channels: this.channels,
        completion: this._completion,
        cancellation: this._cancellation,
        dying: this._dying
      };
    };
  }

  @trace
  async invokeRpcAsync(rpcCall) {
    const {exchange, name, request, requestId, version, timeout = 30000} = rpcCall;
    Log.info(
      () => ({
        msg: `Initiating RPC call`,
        rpcCall,
      }), ({message:m}) => `${m.msg} ${m.rpcCall.name} request=${m.rpcCall.request}`);

    let response;
    const channel = await this.channelAsync(!'confirm');
    const DIRECT_REPLY_TO_QUEUE = 'amq.rabbitmq.reply-to';

    const consumer = await channel.consumeAsync(DIRECT_REPLY_TO_QUEUE, handleResponse, {
      noAck: true,
      exclusive: true
    });

    const serialized = new Buffer(JSON.stringify(request), 'utf8');

    await channel.publishAsync({
      exchange,
      routingKey: name,
      body: serialized,
      persistent: false,
      expiration: timeout,
      replyTo: DIRECT_REPLY_TO_QUEUE,
      headers: {
        'api.request-id': requestId,
        "api.rpc.name": name,
        "api.rpc.version": version
      }
    });

    const t = setTimeout(() => consumer.cancelAsync(new AmqpRpcTimeoutError(this, rpcCall)), timeout);

    await consumer.completion;

    clearTimeout(t);

    if (response.status == 'success') {
      return response.data;
    } else {
      throw new AmqpRpcFailedError(this, rpcCall, response);
    }

    function handleResponse(consumer, message) {
      const body = message.content.toString('utf8');
      try {
        response = JSON.parse(body);
        Log[response.status=='success' ? 'info' : 'warning'](
          () => ({
            msg: `Receive RPC response`,
            rpcCall,
            response: body
          }), ({message:m}) => `${m.msg} ${Log.format(m.response)}`);
        consumer.cancelAsync();
      } catch (err) {
        consumer.cancelAsync(err);
      }
    }
  }

  @trace
  async openAsync() {
    let cleanup = () => {
    };

    this._dying = false;
    this._channels = [];
    this._completion = new Deferred();

    this._clientClose = () => {
      this._completion.reject(
        new AmqpClientClosedError(this));
    };
    this._clientError = (err) => {
      Log.error(
        () => ({
          msg: 'AMQP client error',
          amqp: this,
          exception: err
        }),
        (x) => `Error in AMQP connection to ${x.message.amqp.uri}:\n` +
        Log.format(x.message.exception));

      this.closeAsync(err);
    };

    try {
      const client = this._client = await amqplib
        .connect(this.uri, AmqpClient.defaultOptions);

      this.completion.then(() => cleanup(), () => cleanup());
      this._client.on('close', this._clientClose);
      this._client.on('error', this._clientError);

      cleanup = () => {
        client.removeListener('close', this._clientClose);
        client.removeListener('error', this._clientError);
      };
    }
    catch (exc) {
      this._completion.reject(exc);
      throw exc;
    }
  }

  //noinspection JSMethodCanBeStatic
  @trace
  async closeAsync(err) {
    if (this.isOpen) {
      this._dying = true;

      setTimeout(() => this._completion.reject(
        new AmqpClientCloseTimeoutError(this)), 10000);

      this._clientClose = () => {
        err ? this._completion.reject(err)
          : this._completion.resolve();
      };

      await Promise.all(this.channels.map(
        (channel) => channel.closeAsync(err)));

      try {
        await this._client.close();
      }
      catch (exc) {
        Log.warning(
          () => ({
            msg: 'Failed to close AMQP connection',
            amqp: this,
            exception: exc
          }),
          (x) => `${x.message.msg} to ${x.message.amqp.uri}:\n` +
          Log.format(x.message.exception));
      }
    }

    try {
      await this.completion;
    }
    catch (exc) {
      // ignore
    }
  }

  @trace
  async channelAsync(confirm) {
    const channel = confirm
      ? await this._client.createConfirmChannel()
      : await this._client.createChannel();

    const result = new AmqpChannel(this, channel);
    this.channels.push(result);
    return result;
  }
}
