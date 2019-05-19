/**
 * This action type will be dispatched when your history
 * receives a location change.
 */
export var LOCATION_CHANGE = '@@router/LOCATION_CHANGE';
export var onLocationChanged = function onLocationChanged(location, action) {
  var isFirstRendering = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : false;
  return {
    type: LOCATION_CHANGE,
    payload: {
      location: location,
      action: action,
      isFirstRendering: isFirstRendering
    }
  };
};
/**
 * This action type will be dispatched by the history actions below.
 * If you're writing a middleware to watch for navigation events, be sure to
 * look for actions of this type.
 */

export var CALL_HISTORY_METHOD = '@@router/CALL_HISTORY_METHOD';

var updateLocation = function updateLocation(method) {
  return function () {
    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    return {
      type: CALL_HISTORY_METHOD,
      payload: {
        method: method,
        args: args
      }
    };
  };
};
/**
 * These actions correspond to the history API.
 * The associated routerMiddleware will capture these events before they get to
 * your reducer and reissue them as the matching function on your history.
 */


export var push = updateLocation('push');
export var replace = updateLocation('replace');
export var go = updateLocation('go');
export var goBack = updateLocation('goBack');
export var goForward = updateLocation('goForward');
export var routerActions = {
  push: push,
  replace: replace,
  go: go,
  goBack: goBack,
  goForward: goForward
};