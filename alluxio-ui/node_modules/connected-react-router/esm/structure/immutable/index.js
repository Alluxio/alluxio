import { Iterable, fromJS as _fromJS } from 'immutable';
import getIn from './getIn';
var structure = {
  fromJS: function fromJS(jsValue) {
    return _fromJS(jsValue, function (key, value) {
      return Iterable.isIndexed(value) ? value.toList() : value.toMap();
    });
  },
  getIn: getIn,
  merge: function merge(state, payload) {
    return state.merge(payload);
  },
  toJS: function toJS(value) {
    return Iterable.isIterable(value) ? value.toJS() : value;
  }
};
export default structure;