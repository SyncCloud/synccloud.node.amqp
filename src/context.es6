import _ from 'lodash';

export default class Context {
  constructor({...props}) {
    _.assign(this, props);
  }

  toJSON() {
    return {
      consumer: this.consumer,
      client: this.client,
      message: this.message,
    };
  }

  async publishAsync(options) {
    if (!Context._pubChannel) {
      Context._pubChannel = await this.client.channelAsync(!'confirm');
    }

    return Context._pubChannel.publishAsync(options);
  }

}
