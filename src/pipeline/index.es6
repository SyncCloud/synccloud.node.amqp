import ack from './ack';
import parseJSON from './parse-message-json.es6';
import retry from './try-redeliver.es6';
import logError from './error-logger.es6';
import rpcHandler from './rpc-handler.es6';
import rpcVersion from './rpc-version';
import assertMessage from './assert-message.es6';
import publish from './publish';

export default {
  retry,
  ack,
  parseJSON,
  logError,
  publish,
  assert: assertMessage,
  rpc: {
    handler: rpcHandler,
    version: rpcVersion
  }
}
