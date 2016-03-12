import _ from 'lodash';

export default class Context {
  constructor({...props}) {
    _.assign(this, props);
  }

  toJSON() {
    return {
      channel: this.channel,
      connection: this.connection,
      message: this.message,
      application: this.application,
    };
  }

}
