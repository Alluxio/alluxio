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
  placement: 'top',
  autohide: true,
  placementPrefix: 'bs-tooltip',
  trigger: 'click hover focus'
};

var Tooltip = function Tooltip(props) {
  var popperClasses = (0, _classnames.default)('tooltip', 'show', props.className);
  var classes = (0, _classnames.default)('tooltip-inner', props.innerClassName);
  return _react.default.createElement(_TooltipPopoverWrapper.default, (0, _extends2.default)({}, props, {
    className: popperClasses,
    innerClassName: classes
  }));
};

Tooltip.propTypes = _TooltipPopoverWrapper.propTypes;
Tooltip.defaultProps = defaultProps;
var _default = Tooltip;
exports.default = _default;