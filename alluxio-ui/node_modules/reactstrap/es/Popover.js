import _extends from "@babel/runtime/helpers/esm/extends";
import React from 'react';
import classNames from 'classnames';
import TooltipPopoverWrapper, { propTypes } from './TooltipPopoverWrapper';
var defaultProps = {
  placement: 'right',
  placementPrefix: 'bs-popover',
  trigger: 'click'
};

var Popover = function Popover(props) {
  var popperClasses = classNames('popover', 'show', props.className);
  var classes = classNames('popover-inner', props.innerClassName);
  return React.createElement(TooltipPopoverWrapper, _extends({}, props, {
    className: popperClasses,
    innerClassName: classes
  }));
};

Popover.propTypes = propTypes;
Popover.defaultProps = defaultProps;
export default Popover;