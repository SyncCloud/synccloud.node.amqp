import ack from './ack';
import parseJSON from './parse-message-json';
import retry from './try-redeliver';
import logError from './error-logger';
import rpcHandler from './rpc-handler';
import rpcVersion from './rpc-version';
import assertMessage from './assert-message';
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
