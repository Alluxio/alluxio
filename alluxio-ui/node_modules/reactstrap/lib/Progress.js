"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

exports.__esModule = true;
exports.default = void 0;

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _objectWithoutPropertiesLoose2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutPropertiesLoose"));

var _react = _interopRequireDefault(require("react"));

var _propTypes = _interopRequireDefault(require("prop-types"));

var _classnames = _interopRequireDefault(require("classnames"));

var _lodash = _interopRequireDefault(require("lodash.tonumber"));

var _utils = require("./utils");

var propTypes = {
  children: _propTypes.default.node,
  bar: _propTypes.default.bool,
  multi: _propTypes.default.bool,
  tag: _utils.tagPropType,
  value: _propTypes.default.oneOfType([_propTypes.default.string, _propTypes.default.number]),
  max: _propTypes.default.oneOfType([_propTypes.default.string, _propTypes.default.number]),
  animated: _propTypes.default.bool,
  striped: _propTypes.default.bool,
  color: _propTypes.default.string,
  className: _propTypes.default.string,
  barClassName: _propTypes.default.string,
  cssModule: _propTypes.default.object
};
var defaultProps = {
  tag: 'div',
  value: 0,
  max: 100
};

var Progress = function Progress(props) {
  var children = props.children,
      className = props.className,
      barClassName = props.barClassName,
      cssModule = props.cssModule,
      value = props.value,
      max = props.max,
      animated = props.animated,
      striped = props.striped,
      color = props.color,
      bar = props.bar,
      multi = props.multi,
      Tag = props.tag,
      attributes = (0, _objectWithoutPropertiesLoose2.default)(props, ["children", "className", "barClassName", "cssModule", "value", "max", "animated", "striped", "color", "bar", "multi", "tag"]);
  var percent = (0, _lodash.default)(value) / (0, _lodash.default)(max) * 100;
  var progressClasses = (0, _utils.mapToCssModules)((0, _classnames.default)(className, 'progress'), cssModule);
  var progressBarClasses = (0, _utils.mapToCssModules)((0, _classnames.default)('progress-bar', bar ? className || barClassName : barClassName, animated ? 'progress-bar-animated' : null, color ? "bg-" + color : null, striped || animated ? 'progress-bar-striped' : null), cssModule);
  var ProgressBar = multi ? children : _react.default.createElement("div", {
    className: progressBarClasses,
    style: {
      width: percent + "%"
    },
    role: "progressbar",
    "aria-valuenow": value,
    "aria-valuemin": "0",
    "aria-valuemax": max,
    children: children
  });

  if (bar) {
    return ProgressBar;
  }

  return _react.default.createElement(Tag, (0, _extends2.default)({}, attributes, {
    className: progressClasses,
    children: ProgressBar
  }));
};

Progress.propTypes = propTypes;
Progress.defaultProps = defaultProps;
var _default = Progress;
exports.default = _default;