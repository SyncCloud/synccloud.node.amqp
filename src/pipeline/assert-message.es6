import _ from 'lodash';
import {assertType} from '@synccloud/utils';
import deep from 'deep-property';

export default function initAssertMessage(assertionRules) {
  return async({message}, next) => {
    for (let [key, type] of _.toPairs(assertionRules)) {
      assertType(deep.get(message.body, key), type, `message.body.${key}`);
    }

    return await next();
  }

}
