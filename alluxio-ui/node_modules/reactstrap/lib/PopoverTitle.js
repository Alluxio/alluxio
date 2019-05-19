"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

exports.__esModule = true;
exports.default = PopoverTitle;

var _react = _interopRequireDefault(require("react"));

var _PopoverHeader = _interopRequireDefault(require("./PopoverHeader"));

var _utils = require("./utils");

function PopoverTitle(props) {
  (0, _utils.warnOnce)('The "PopoverTitle" component has been deprecated.\nPlease use component "PopoverHeader".');
  return _react.default.createElement(_PopoverHeader.default, props);
}