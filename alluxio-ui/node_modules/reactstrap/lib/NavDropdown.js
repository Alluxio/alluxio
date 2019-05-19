"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

exports.__esModule = true;
exports.default = NavDropdown;

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _react = _interopRequireDefault(require("react"));

var _utils = require("./utils");

var _Dropdown = _interopRequireDefault(require("./Dropdown"));

function NavDropdown(props) {
  (0, _utils.warnOnce)('The "NavDropdown" component has been deprecated.\nPlease use component "Dropdown" with nav prop.');
  return _react.default.createElement(_Dropdown.default, (0, _extends2.default)({
    nav: true
  }, props));
}