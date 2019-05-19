"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

exports.__esModule = true;
exports.default = void 0;

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _react = _interopRequireDefault(require("react"));

var _propTypes = _interopRequireDefault(require("prop-types"));

var _Dropdown = _interopRequireDefault(require("./Dropdown"));

var propTypes = {
  children: _propTypes.default.node
};

var ButtonDropdown = function ButtonDropdown(props) {
  return _react.default.createElement(_Dropdown.default, (0, _extends2.default)({
    group: true
  }, props));
};

ButtonDropdown.propTypes = propTypes;
var _default = ButtonDropdown;
exports.default = _default;