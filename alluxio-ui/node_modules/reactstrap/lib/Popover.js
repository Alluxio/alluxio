"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

exports.__esModule = true;
exports.default = void 0;

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _react = _interopRequireDefault(require("react"));

var _classnames = _interopRequireDefault(require("classnames"));

var _TooltipPopoverWrapper = _interopRequireWildcard(require("./TooltipPopoverWrapper"));

var defaultProps = {
  placement: 'right',
  placementPrefix: 'bs-popover',
  trigger: 'click'
};

var Popover = function Popover(props) {
  var popperClasses = (0, _classnames.default)('popover', 'show', props.className);
  var classes = (0, _classnames.default)('popover-inner', props.innerClassName);
  return _react.default.createElement(_TooltipPopoverWrapper.default, (0, _extends2.default)({}, props, {
    className: popperClasses,
    innerClassName: classes
  }));
};

Popover.propTypes = _TooltipPopoverWrapper.propTypes;
Popover.defaultProps = defaultProps;
var _default = Popover;
exports.default = _default;