import _extends from "@babel/runtime/helpers/esm/extends";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread";
import _objectWithoutPropertiesLoose from "@babel/runtime/helpers/esm/objectWithoutPropertiesLoose";
import React from 'react';
import PropTypes from 'prop-types';
import classNames from 'classnames';
import { Popper } from 'react-popper';
import { mapToCssModules, tagPropType } from './utils';
var propTypes = {
  tag: tagPropType,
  children: PropTypes.node.isRequired,
  right: PropTypes.bool,
  flip: PropTypes.bool,
  modifiers: PropTypes.object,
  className: PropTypes.string,
  cssModule: PropTypes.object,
  persist: PropTypes.bool
};
var defaultProps = {
  tag: 'div',
  flip: true
};
var contextTypes = {
  isOpen: PropTypes.bool.isRequired,
  direction: PropTypes.oneOf(['up', 'down', 'left', 'right']).isRequired,
  inNavbar: PropTypes.bool.isRequired
};
var noFlipModifier = {
  flip: {
    enabled: false
  }
};
var directionPositionMap = {
  up: 'top',
  left: 'left',
  right: 'right',
  down: 'bottom'
};

var DropdownMenu = function DropdownMenu(props, context) {
  var className = props.className,
      cssModule = props.cssModule,
      right = props.right,
      tag = props.tag,
      flip = props.flip,
      modifiers = props.modifiers,
      persist = props.persist,
      attrs = _objectWithoutPropertiesLoose(props, ["className", "cssModule", "right", "tag", "flip", "modifiers", "persist"]);

  var classes = mapToCssModules(classNames(className, 'dropdown-menu', {
    'dropdown-menu-right': right,
    show: context.isOpen
  }), cssModule);
  var Tag = tag;

  if (persist || context.isOpen && !context.inNavbar) {
    Tag = Popper;
    var position1 = directionPositionMap[context.direction] || 'bottom';
    var position2 = right ? 'end' : 'start';
    attrs.placement = position1 + "-" + position2;
    attrs.component = tag;
    attrs.modifiers = !flip ? _objectSpread({}, modifiers, noFlipModifier) : modifiers;
  }

  return React.createElement(Tag, _extends({
    tabIndex: "-1",
    role: "menu"
  }, attrs, {
    "aria-hidden": !context.isOpen,
    className: classes,
    "x-placement": attrs.placement
  }));
};

DropdownMenu.propTypes = propTypes;
DropdownMenu.defaultProps = defaultProps;
DropdownMenu.contextTypes = contextTypes;
export default DropdownMenu;