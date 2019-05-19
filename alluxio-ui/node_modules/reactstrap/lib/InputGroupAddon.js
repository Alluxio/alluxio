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

var _InputGroupText = _interopRequireDefault(require("./InputGroupText"));

var propTypes = {
  tag: _utils.tagPropType,
  addonType: _propTypes.default.oneOf(['prepend', 'append']).isRequired,
  children: _propTypes.default.node,
  className: _propTypes.default.string,
  cssModule: _propTypes.default.object
};
var defaultProps = {
  tag: 'div'
};

var InputGroupAddon = function InputGroupAddon(props) {
  var className = props.className,
      cssModule = props.cssModule,
      Tag = props.tag,
      addonType = props.addonType,
      children = props.children,
      attributes = (0, _objectWithoutPropertiesLoose2.default)(props, ["className", "cssModule", "tag", "addonType", "children"]);
  var classes = (0, _utils.mapToCssModules)((0, _classnames.default)(className, 'input-group-' + addonType), cssModule); // Convenience to assist with transition

  if (typeof children === 'string') {
    return _react.default.createElement(Tag, (0, _extends2.default)({}, attributes, {
      className: classes
    }), _react.default.createElement(_InputGroupText.default, {
      children: children
    }));
  }

  return _react.default.createElement(Tag, (0, _extends2.default)({}, attributes, {
    className: classes,
    children: children
  }));
};

InputGroupAddon.propTypes = propTypes;
InputGroupAddon.defaultProps = defaultProps;
var _default = InputGroupAddon;
exports.default = _default;