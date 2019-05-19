/* Code from github.com/erikras/redux-form by Erik Rasmussen */
import { Iterable } from 'immutable';
import plainGetIn from '../plain/getIn';

var getIn = function getIn(state, path) {
  return Iterable.isIterable(state) ? state.getIn(path) : plainGetIn(state, path);
};

export default getIn;