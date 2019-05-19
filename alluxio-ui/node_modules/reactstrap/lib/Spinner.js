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
  tag: _utils.tagPropType,
  type: _propTypes.default.string,
  size: _propTypes.default.string,
  color: _propTypes.default.string,
  className: _propTypes.default.string,
  cssModule: _propTypes.default.object,
  children: _propTypes.default.string
};
var defaultProps = {
  tag: 'div',
  type: 'border',
  children: 'Loading...'
};

var Spinner = function Spinner(props) {
  var className = props.className,
      cssModule = props.cssModule,
      type = props.type,
      size = props.size,
      color = props.color,
      children = props.children,
      Tag = props.tag,
      attributes = (0, _objectWithoutPropertiesLoose2.default)(props, ["className", "cssModule", "type", "size", "color", "children", "tag"]);
  var classes = (0, _utils.mapToCssModules)((0, _classnames.default)(className, size ? "spinner-" + type + "-" + size : false, "spinner-" + type, color ? "text-" + color : false), cssModule);
  return _react.default.createElement(Tag, (0, _extends2.default)({
    role: "status"
  }, attributes, {
    className: classes
  }), children && _react.default.createElement("span", {
    className: (0, _utils.mapToCssModules)('sr-only', cssModule)
  }, children));
};

Spinner.propTypes = propTypes;
Spinner.defaultProps = defaultProps;
var _default = Spinner;
exports.default = _default;