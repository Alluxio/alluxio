"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

exports.__esModule = true;
exports.default = CardBlock;

var _react = _interopRequireDefault(require("react"));

var _CardBody = _interopRequireDefault(require("./CardBody"));

var _utils = require("./utils");

function CardBlock(props) {
  (0, _utils.warnOnce)('The "CardBlock" component has been deprecated.\nPlease use component "CardBody".');
  return _react.default.createElement(_CardBody.default, props);
}