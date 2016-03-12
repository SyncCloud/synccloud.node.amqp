import AmqpChannel from './channel';
import AmqpClient from './client';
import AmqpConsumer from './amqp-consumer';
import AmqpService from './service';

const errors = require('./errors');

export default {
  AmqpChannel,
  AmqpClient,
  AmqpConsumer,
  AmqpService,
  ...errors
}
