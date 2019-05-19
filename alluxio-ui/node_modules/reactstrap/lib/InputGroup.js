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
  size: _propTypes.default.string,
  className: _propTypes.default.string,
  cssModule: _propTypes.default.object
};
var defaultProps = {
  tag: 'div'
};

var InputGroup = function InputGroup(props) {
  var className = props.className,
      cssModule = props.cssModule,
      Tag = props.tag,
      size = props.size,
      attributes = (0, _objectWithoutPropertiesLoose2.default)(props, ["className", "cssModule", "tag", "size"]);
  var classes = (0, _utils.mapToCssModules)((0, _classnames.default)(className, 'input-group', size ? "input-group-" + size : null), cssModule);
  return _react.default.createElement(Tag, (0, _extends2.default)({}, attributes, {
    className: classes
  }));
};

InputGroup.propTypes = propTypes;
InputGroup.defaultProps = defaultProps;
var _default = InputGroup;
exports.default = _default;