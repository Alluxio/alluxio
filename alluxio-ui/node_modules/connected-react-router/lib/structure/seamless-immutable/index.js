"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;

var _seamlessImmutable = _interopRequireDefault(require("seamless-immutable"));

var _getIn = _interopRequireDefault(require("../plain/getIn"));

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

var Immutable = _seamlessImmutable.default.static;
var structure = {
  fromJS: function fromJS(value) {
    return Immutable.from(value);
  },
  getIn: _getIn.default,
  merge: function merge(state, payload) {
    return Immutable.merge(state, payload);
  },
  toJS: function toJS(value) {
    return Immutable.asMutable(value);
  }
};
var _default = structure;
exports.default = _default;