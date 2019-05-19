"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

exports.__esModule = true;
exports.default = void 0;

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _objectWithoutPropertiesLoose2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutPropertiesLoose"));

var _react = _interopRequireDefault(require("react"));

var _propTypes = _interopRequireDefault(require("prop-types"));

var _classnames = _interopRequireDefault(require("classnames"));

var _utils = require("./utils");

var propTypes = {
  light: _propTypes.default.bool,
  dark: _propTypes.default.bool,
  inverse: (0, _utils.deprecated)(_propTypes.default.bool, 'Please use the prop "dark"'),
  full: _propTypes.default.bool,
  fixed: _propTypes.default.string,
  sticky: _propTypes.default.string,
  color: _propTypes.default.string,
  role: _propTypes.default.string,
  tag: _utils.tagPropType,
  className: _propTypes.default.string,
  cssModule: _propTypes.default.object,
  toggleable: (0, _utils.deprecated)(_propTypes.default.oneOfType([_propTypes.default.bool, _propTypes.default.string]), 'Please use the prop "expand"'),
  expand: _propTypes.default.oneOfType([_propTypes.default.bool, _propTypes.default.string])
};
var defaultProps = {
  tag: 'nav',
  expand: false
};

var getExpandClass = function getExpandClass(expand) {
  if (expand === false) {
    return false;
  } else if (expand === true || expand === 'xs') {
    return 'navbar-expand';
  }

  return "navbar-expand-" + expand;
}; // To better maintain backwards compatibility while toggleable is deprecated.
// We must map breakpoints to the next breakpoint so that toggleable and expand do the same things at the same breakpoint.


var toggleableToExpand = {
  xs: 'sm',
  sm: 'md',
  md: 'lg',
  lg: 'xl'
};

var getToggleableClass = function getToggleableClass(toggleable) {
  if (toggleable === undefined || toggleable === 'xl') {
    return false;
  } else if (toggleable === false) {
    return 'navbar-expand';
  }

  return "navbar-expand-" + (toggleable === true ? 'sm' : toggleableToExpand[toggleable] || toggleable);
};

var Navbar = function Navbar(props) {
  var _classNames;

  var toggleable = props.toggleable,
      expand = props.expand,
      className = props.className,
      cssModule = props.cssModule,
      light = props.light,
      dark = props.dark,
      inverse = props.inverse,
      fixed = props.fixed,
      sticky = props.sticky,
      color = props.color,
      Tag = props.tag,
      attributes = (0, _objectWithoutPropertiesLoose2.default)(props, ["toggleable", "expand", "className", "cssModule", "light", "dark", "inverse", "fixed", "sticky", "color", "tag"]);
  var classes = (0, _utils.mapToCssModules)((0, _classnames.default)(className, 'navbar', getExpandClass(expand) || getToggleableClass(toggleable), (_classNames = {
    'navbar-light': light,
    'navbar-dark': inverse || dark
  }, _classNames["bg-" + color] = color, _classNames["fixed-" + fixed] = fixed, _classNames["sticky-" + sticky] = sticky, _classNames)), cssModule);
  return _react.default.createElement(Tag, (0, _extends2.default)({}, attributes, {
    className: classes
  }));
};

Navbar.propTypes = propTypes;
Navbar.defaultProps = defaultProps;
var _default = Navbar;
exports.default = _default;