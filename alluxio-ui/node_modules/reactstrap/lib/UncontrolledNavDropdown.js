"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

exports.__esModule = true;
exports.default = void 0;

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _react = _interopRequireDefault(require("react"));

var _utils = require("./utils");

var _UncontrolledDropdown = _interopRequireDefault(require("./UncontrolledDropdown"));

var UncontrolledNavDropdown = function UncontrolledNavDropdown(props) {
  (0, _utils.warnOnce)('The "UncontrolledNavDropdown" component has been deprecated.\nPlease use component "UncontrolledDropdown" with nav prop.');
  return _react.default.createElement(_UncontrolledDropdown.default, (0, _extends2.default)({
    nav: true
  }, props));
};

var _default = UncontrolledNavDropdown;
exports.default = _default;