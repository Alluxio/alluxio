"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

exports.__esModule = true;
exports.default = PopoverContent;

var _react = _interopRequireDefault(require("react"));

var _PopoverBody = _interopRequireDefault(require("./PopoverBody"));

var _utils = require("./utils");

function PopoverContent(props) {
  (0, _utils.warnOnce)('The "PopoverContent" component has been deprecated.\nPlease use component "PopoverBody".');
  return _react.default.createElement(_PopoverBody.default, props);
}