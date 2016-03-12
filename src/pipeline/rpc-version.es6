export default function initRpcVersion({version}) {
  return async function rpcVersion({message}, next) {
    // todo check message rpc version
    await next();
  }
}
