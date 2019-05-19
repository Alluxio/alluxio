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
  className: _propTypes.default.string,
  id: _propTypes.default.oneOfType([_propTypes.default.string, _propTypes.default.number]).isRequired,
  type: _propTypes.default.string.isRequired,
  label: _propTypes.default.node,
  inline: _propTypes.default.bool,
  valid: _propTypes.default.bool,
  invalid: _propTypes.default.bool,
  bsSize: _propTypes.default.string,
  cssModule: _propTypes.default.object,
  children: _propTypes.default.oneOfType([_propTypes.default.node, _propTypes.default.array, _propTypes.default.func]),
  innerRef: _propTypes.default.oneOfType([_propTypes.default.object, _propTypes.default.string, _propTypes.default.func])
};

function CustomInput(props) {
  var className = props.className,
      label = props.label,
      inline = props.inline,
      valid = props.valid,
      invalid = props.invalid,
      cssModule = props.cssModule,
      children = props.children,
      bsSize = props.bsSize,
      innerRef = props.innerRef,
      attributes = (0, _objectWithoutPropertiesLoose2.default)(props, ["className", "label", "inline", "valid", "invalid", "cssModule", "children", "bsSize", "innerRef"]);
  var type = attributes.type;
  var customClass = (0, _utils.mapToCssModules)((0, _classnames.default)(className, "custom-" + type, bsSize ? "custom-" + type + "-" + bsSize : false), cssModule);
  var validationClassNames = (0, _utils.mapToCssModules)((0, _classnames.default)(invalid && 'is-invalid', valid && 'is-valid'), cssModule);

  if (type === 'select') {
    return _react.default.createElement("select", (0, _extends2.default)({}, attributes, {
      ref: innerRef,
      className: (0, _classnames.default)(validationClassNames, customClass)
    }), children);
  }

  if (type === 'file') {
    return _react.default.createElement("div", {
      className: customClass
    }, _react.default.createElement("input", (0, _extends2.default)({}, attributes, {
      ref: innerRef,
      className: (0, _classnames.default)(validationClassNames, (0, _utils.mapToCssModules)('custom-file-input', cssModule))
    })), _react.default.createElement("label", {
      className: (0, _utils.mapToCssModules)('custom-file-label', cssModule),
      htmlFor: attributes.id
    }, label || 'Choose file'));
  }

  if (type !== 'checkbox' && type !== 'radio' && type !== 'switch') {
    return _react.default.createElement("input", (0, _extends2.default)({}, attributes, {
      ref: innerRef,
      className: (0, _classnames.default)(validationClassNames, customClass)
    }));
  }

  var wrapperClasses = (0, _classnames.default)(customClass, (0, _utils.mapToCssModules)((0, _classnames.default)('custom-control', {
    'custom-control-inline': inline
  }), cssModule));
  return _react.default.createElement("div", {
    className: wrapperClasses
  }, _react.default.createElement("input", (0, _extends2.default)({}, attributes, {
    type: type === 'switch' ? 'checkbox' : type,
    ref: innerRef,
    className: (0, _classnames.default)(validationClassNames, (0, _utils.mapToCssModules)('custom-control-input', cssModule))
  })), _react.default.createElement("label", {
    className: (0, _utils.mapToCssModules)('custom-control-label', cssModule),
    htmlFor: attributes.id
  }, label), children);
}

CustomInput.propTypes = propTypes;
var _default = CustomInput;
exports.default = _default;