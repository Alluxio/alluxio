import _extends from "@babel/runtime/helpers/esm/extends";
import _objectWithoutPropertiesLoose from "@babel/runtime/helpers/esm/objectWithoutPropertiesLoose";
import React from 'react';
import PropTypes from 'prop-types';
import classNames from 'classnames';
import { mapToCssModules } from './utils';
var propTypes = {
  className: PropTypes.string,
  id: PropTypes.oneOfType([PropTypes.string, PropTypes.number]).isRequired,
  type: PropTypes.string.isRequired,
  label: PropTypes.node,
  inline: PropTypes.bool,
  valid: PropTypes.bool,
  invalid: PropTypes.bool,
  bsSize: PropTypes.string,
  cssModule: PropTypes.object,
  children: PropTypes.oneOfType([PropTypes.node, PropTypes.array, PropTypes.func]),
  innerRef: PropTypes.oneOfType([PropTypes.object, PropTypes.string, PropTypes.func])
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
      attributes = _objectWithoutPropertiesLoose(props, ["className", "label", "inline", "valid", "invalid", "cssModule", "children", "bsSize", "innerRef"]);

  var type = attributes.type;
  var customClass = mapToCssModules(classNames(className, "custom-" + type, bsSize ? "custom-" + type + "-" + bsSize : false), cssModule);
  var validationClassNames = mapToCssModules(classNames(invalid && 'is-invalid', valid && 'is-valid'), cssModule);

  if (type === 'select') {
    return React.createElement("select", _extends({}, attributes, {
      ref: innerRef,
      className: classNames(validationClassNames, customClass)
    }), children);
  }

  if (type === 'file') {
    return React.createElement("div", {
      className: customClass
    }, React.createElement("input", _extends({}, attributes, {
      ref: innerRef,
      className: classNames(validationClassNames, mapToCssModules('custom-file-input', cssModule))
    })), React.createElement("label", {
      className: mapToCssModules('custom-file-label', cssModule),
      htmlFor: attributes.id
    }, label || 'Choose file'));
  }

  if (type !== 'checkbox' && type !== 'radio' && type !== 'switch') {
    return React.createElement("input", _extends({}, attributes, {
      ref: innerRef,
      className: classNames(validationClassNames, customClass)
    }));
  }

  var wrapperClasses = classNames(customClass, mapToCssModules(classNames('custom-control', {
    'custom-control-inline': inline
  }), cssModule));
  return React.createElement("div", {
    className: wrapperClasses
  }, React.createElement("input", _extends({}, attributes, {
    type: type === 'switch' ? 'checkbox' : type,
    ref: innerRef,
    className: classNames(validationClassNames, mapToCssModules('custom-control-input', cssModule))
  })), React.createElement("label", {
    className: mapToCssModules('custom-control-label', cssModule),
    htmlFor: attributes.id
  }, label), children);
}

CustomInput.propTypes = propTypes;
export default CustomInput;