"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

exports.__esModule = true;
exports.default = void 0;

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread"));

var _objectWithoutPropertiesLoose2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutPropertiesLoose"));

var _react = _interopRequireDefault(require("react"));

var _propTypes = _interopRequireDefault(require("prop-types"));

var _Button = _interopRequireDefault(require("./Button"));

var _InputGroupAddon = _interopRequireDefault(require("./InputGroupAddon"));

var _utils = require("./utils");

var propTypes = {
  tag: _utils.tagPropType,
  addonType: _propTypes.default.oneOf(['prepend', 'append']).isRequired,
  children: _propTypes.default.node,
  groupClassName: _propTypes.default.string,
  groupAttributes: _propTypes.default.object,
  className: _propTypes.default.string,
  cssModule: _propTypes.default.object
};

var InputGroupButton = function InputGroupButton(props) {
  (0, _utils.warnOnce)('The "InputGroupButton" component has been deprecated.\nPlease use component "InputGroupAddon".');
  var children = props.children,
      groupClassName = props.groupClassName,
      groupAttributes = props.groupAttributes,
      propsWithoutGroup = (0, _objectWithoutPropertiesLoose2.default)(props, ["children", "groupClassName", "groupAttributes"]);

  if (typeof children === 'string') {
    var cssModule = propsWithoutGroup.cssModule,
        tag = propsWithoutGroup.tag,
        addonType = propsWithoutGroup.addonType,
        attributes = (0, _objectWithoutPropertiesLoose2.default)(propsWithoutGroup, ["cssModule", "tag", "addonType"]);
    var allGroupAttributes = (0, _objectSpread2.default)({}, groupAttributes, {
      cssModule: cssModule,
      tag: tag,
      addonType: addonType
    });
    return _react.default.createElement(_InputGroupAddon.default, (0, _extends2.default)({}, allGroupAttributes, {
      className: groupClassName
    }), _react.default.createElement(_Button.default, (0, _extends2.default)({}, attributes, {
      children: children
    })));
  }

  return _react.default.createElement(_InputGroupAddon.default, (0, _extends2.default)({}, props, {
    children: children
  }));
};

InputGroupButton.propTypes = propTypes;
var _default = InputGroupButton;
exports.default = _default;