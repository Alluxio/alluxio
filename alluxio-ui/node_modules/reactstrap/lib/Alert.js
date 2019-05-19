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

var _utils = require("./utils");

var _Fade = _interopRequireDefault(require("./Fade"));

var propTypes = {
  children: _propTypes.default.node,
  className: _propTypes.default.string,
  closeClassName: _propTypes.default.string,
  closeAriaLabel: _propTypes.default.string,
  cssModule: _propTypes.default.object,
  color: _propTypes.default.string,
  fade: _propTypes.default.bool,
  isOpen: _propTypes.default.bool,
  toggle: _propTypes.default.func,
  tag: _utils.tagPropType,
  transition: _propTypes.default.shape(_Fade.default.propTypes),
  innerRef: _propTypes.default.oneOfType([_propTypes.default.object, _propTypes.default.string, _propTypes.default.func])
};
var defaultProps = {
  color: 'success',
  isOpen: true,
  tag: 'div',
  closeAriaLabel: 'Close',
  fade: true,
  transition: (0, _objectSpread2.default)({}, _Fade.default.defaultProps, {
    unmountOnExit: true
  })
};

function Alert(props) {
  var className = props.className,
      closeClassName = props.closeClassName,
      closeAriaLabel = props.closeAriaLabel,
      cssModule = props.cssModule,
      Tag = props.tag,
      color = props.color,
      isOpen = props.isOpen,
      toggle = props.toggle,
      children = props.children,
      transition = props.transition,
      fade = props.fade,
      innerRef = props.innerRef,
      attributes = (0, _objectWithoutPropertiesLoose2.default)(props, ["className", "closeClassName", "closeAriaLabel", "cssModule", "tag", "color", "isOpen", "toggle", "children", "transition", "fade", "innerRef"]);
  var classes = (0, _utils.mapToCssModules)((0, _classnames.default)(className, 'alert', "alert-" + color, {
    'alert-dismissible': toggle
  }), cssModule);
  var closeClasses = (0, _utils.mapToCssModules)((0, _classnames.default)('close', closeClassName), cssModule);
  var alertTransition = (0, _objectSpread2.default)({}, _Fade.default.defaultProps, transition, {
    baseClass: fade ? transition.baseClass : '',
    timeout: fade ? transition.timeout : 0
  });
  return _react.default.createElement(_Fade.default, (0, _extends2.default)({}, attributes, alertTransition, {
    tag: Tag,
    className: classes,
    in: isOpen,
    role: "alert",
    innerRef: innerRef
  }), toggle ? _react.default.createElement("button", {
    type: "button",
    className: closeClasses,
    "aria-label": closeAriaLabel,
    onClick: toggle
  }, _react.default.createElement("span", {
    "aria-hidden": "true"
  }, "\xD7")) : null, children);
}

Alert.propTypes = propTypes;
Alert.defaultProps = defaultProps;
var _default = Alert;
exports.default = _default;