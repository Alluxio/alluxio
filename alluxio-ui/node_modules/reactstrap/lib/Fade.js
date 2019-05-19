"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

exports.__esModule = true;
exports.default = void 0;

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _objectWithoutPropertiesLoose2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutPropertiesLoose"));

var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread"));

var _react = _interopRequireDefault(require("react"));

var _propTypes = _interopRequireDefault(require("prop-types"));

var _classnames = _interopRequireDefault(require("classnames"));

var _reactTransitionGroup = require("react-transition-group");

var _utils = require("./utils");

var propTypes = (0, _objectSpread2.default)({}, _reactTransitionGroup.Transition.propTypes, {
  children: _propTypes.default.oneOfType([_propTypes.default.arrayOf(_propTypes.default.node), _propTypes.default.node]),
  tag: _utils.tagPropType,
  baseClass: _propTypes.default.string,
  baseClassActive: _propTypes.default.string,
  className: _propTypes.default.string,
  cssModule: _propTypes.default.object,
  innerRef: _propTypes.default.oneOfType([_propTypes.default.object, _propTypes.default.string, _propTypes.default.func])
});
var defaultProps = (0, _objectSpread2.default)({}, _reactTransitionGroup.Transition.defaultProps, {
  tag: 'div',
  baseClass: 'fade',
  baseClassActive: 'show',
  timeout: _utils.TransitionTimeouts.Fade,
  appear: true,
  enter: true,
  exit: true,
  in: true
});

function Fade(props) {
  var Tag = props.tag,
      baseClass = props.baseClass,
      baseClassActive = props.baseClassActive,
      className = props.className,
      cssModule = props.cssModule,
      children = props.children,
      innerRef = props.innerRef,
      otherProps = (0, _objectWithoutPropertiesLoose2.default)(props, ["tag", "baseClass", "baseClassActive", "className", "cssModule", "children", "innerRef"]);
  var transitionProps = (0, _utils.pick)(otherProps, _utils.TransitionPropTypeKeys);
  var childProps = (0, _utils.omit)(otherProps, _utils.TransitionPropTypeKeys);
  return _react.default.createElement(_reactTransitionGroup.Transition, transitionProps, function (status) {
    var isActive = status === 'entered';
    var classes = (0, _utils.mapToCssModules)((0, _classnames.default)(className, baseClass, isActive && baseClassActive), cssModule);
    return _react.default.createElement(Tag, (0, _extends2.default)({
      className: classes
    }, childProps, {
      ref: innerRef
    }), children);
  });
}

Fade.propTypes = propTypes;
Fade.defaultProps = defaultProps;
var _default = Fade;
exports.default = _default;