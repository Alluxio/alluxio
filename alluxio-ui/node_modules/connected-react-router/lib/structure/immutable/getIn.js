"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;

var _immutable = require("immutable");

var _getIn = _interopRequireDefault(require("../plain/getIn"));

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

/* Code from github.com/erikras/redux-form by Erik Rasmussen */
var getIn = function getIn(state, path) {
  return _immutable.Iterable.isIterable(state) ? state.getIn(path) : (0, _getIn.default)(state, path);
};

var _default = getIn;
exports.default = _default;