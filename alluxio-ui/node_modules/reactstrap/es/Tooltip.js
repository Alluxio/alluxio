import _extends from "@babel/runtime/helpers/esm/extends";
import React from 'react';
import classNames from 'classnames';
import TooltipPopoverWrapper, { propTypes } from './TooltipPopoverWrapper';
var defaultProps = {
  placement: 'top',
  autohide: true,
  placementPrefix: 'bs-tooltip',
  trigger: 'click hover focus'
};

var Tooltip = function Tooltip(props) {
  var popperClasses = classNames('tooltip', 'show', props.className);
  var classes = classNames('tooltip-inner', props.innerClassName);
  return React.createElement(TooltipPopoverWrapper, _extends({}, props, {
    className: popperClasses,
    innerClassName: classes
  }));
};

Tooltip.propTypes = propTypes;
Tooltip.defaultProps = defaultProps;
export default Tooltip;