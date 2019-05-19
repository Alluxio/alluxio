"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.routerActions = exports.goForward = exports.goBack = exports.go = exports.replace = exports.push = exports.CALL_HISTORY_METHOD = exports.onLocationChanged = exports.LOCATION_CHANGE = void 0;

/**
 * This action type will be dispatched when your history
 * receives a location change.
 */
var LOCATION_CHANGE = '@@router/LOCATION_CHANGE';
exports.LOCATION_CHANGE = LOCATION_CHANGE;

var onLocationChanged = function onLocationChanged(location, action) {
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


exports.onLocationChanged = onLocationChanged;
var CALL_HISTORY_METHOD = '@@router/CALL_HISTORY_METHOD';
exports.CALL_HISTORY_METHOD = CALL_HISTORY_METHOD;

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


var push = updateLocation('push');
exports.push = push;
var replace = updateLocation('replace');
exports.replace = replace;
var go = updateLocation('go');
exports.go = go;
var goBack = updateLocation('goBack');
exports.goBack = goBack;
var goForward = updateLocation('goForward');
exports.goForward = goForward;
var routerActions = {
  push: push,
  replace: replace,
  go: go,
  goBack: goBack,
  goForward: goForward
};
exports.routerActions = routerActions;