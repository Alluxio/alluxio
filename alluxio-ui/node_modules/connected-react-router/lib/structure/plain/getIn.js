"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;

/* Code from github.com/erikras/redux-form by Erik Rasmussen */
var getIn = function getIn(state, path) {
  if (!state) {
    return state;
  }

  var length = path.length;

  if (!length) {
    return undefined;
  }

  var result = state;

  for (var i = 0; i < length && !!result; ++i) {
    result = result[path[i]];
  }

  return result;
};

var _default = getIn;
exports.default = _default;