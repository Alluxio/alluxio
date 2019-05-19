"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

exports.__esModule = true;
exports.default = void 0;

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread"));

var _objectWithoutPropertiesLoose2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutPropertiesLoose"));

var _react = _interopRequireDefault(require("react"));

var _propTypes = _interopRequireDefault(require("prop-types"));

var _classnames = _interopRequireDefault(require("classnames"));

var _reactPopper = require("react-popper");

var _utils = require("./utils");

var propTypes = {
  tag: _utils.tagPropType,
  children: _propTypes.default.node.isRequired,
  right: _propTypes.default.bool,
  flip: _propTypes.default.bool,
  modifiers: _propTypes.default.object,
  className: _propTypes.default.string,
  cssModule: _propTypes.default.object,
  persist: _propTypes.default.bool
};
var defaultProps = {
  tag: 'div',
  flip: true
};
var contextTypes = {
  isOpen: _propTypes.default.bool.isRequired,
  direction: _propTypes.default.oneOf(['up', 'down', 'left', 'right']).isRequired,
  inNavbar: _propTypes.default.bool.isRequired
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
      attrs = (0, _objectWithoutPropertiesLoose2.default)(props, ["className", "cssModule", "right", "tag", "flip", "modifiers", "persist"]);
  var classes = (0, _utils.mapToCssModules)((0, _classnames.default)(className, 'dropdown-menu', {
    'dropdown-menu-right': right,
    show: context.isOpen
  }), cssModule);
  var Tag = tag;

  if (persist || context.isOpen && !context.inNavbar) {
    Tag = _reactPopper.Popper;
    var position1 = directionPositionMap[context.direction] || 'bottom';
    var position2 = right ? 'end' : 'start';
    attrs.placement = position1 + "-" + position2;
    attrs.component = tag;
    attrs.modifiers = !flip ? (0, _objectSpread2.default)({}, modifiers, noFlipModifier) : modifiers;
  }

  return _react.default.createElement(Tag, (0, _extends2.default)({
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
var _default = DropdownMenu;
exports.default = _default;