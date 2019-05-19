"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

exports.__esModule = true;
exports.default = void 0;

var _react = _interopRequireDefault(require("react"));

var _propTypes = _interopRequireDefault(require("prop-types"));

var _Dropdown = _interopRequireDefault(require("./Dropdown"));

var propTypes = {
  addonType: _propTypes.default.oneOf(['prepend', 'append']).isRequired,
  children: _propTypes.default.node
};

var InputGroupButtonDropdown = function InputGroupButtonDropdown(props) {
  return _react.default.createElement(_Dropdown.default, props);
};

InputGroupButtonDropdown.propTypes = propTypes;
var _default = InputGroupButtonDropdown;
exports.default = _default;