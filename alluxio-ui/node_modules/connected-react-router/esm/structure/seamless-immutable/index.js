import SeamlessImmutable from 'seamless-immutable';
import getIn from '../plain/getIn';
var Immutable = SeamlessImmutable.static;
var structure = {
  fromJS: function fromJS(value) {
    return Immutable.from(value);
  },
  getIn: getIn,
  merge: function merge(state, payload) {
    return Immutable.merge(state, payload);
  },
  toJS: function toJS(value) {
    return Immutable.asMutable(value);
  }
};
export default structure;