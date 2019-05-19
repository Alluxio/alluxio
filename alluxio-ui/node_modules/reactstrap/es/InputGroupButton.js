import _extends from "@babel/runtime/helpers/esm/extends";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread";
import _objectWithoutPropertiesLoose from "@babel/runtime/helpers/esm/objectWithoutPropertiesLoose";
import React from 'react';
import PropTypes from 'prop-types';
import Button from './Button';
import InputGroupAddon from './InputGroupAddon';
import { warnOnce, tagPropType } from './utils';
var propTypes = {
  tag: tagPropType,
  addonType: PropTypes.oneOf(['prepend', 'append']).isRequired,
  children: PropTypes.node,
  groupClassName: PropTypes.string,
  groupAttributes: PropTypes.object,
  className: PropTypes.string,
  cssModule: PropTypes.object
};

var InputGroupButton = function InputGroupButton(props) {
  warnOnce('The "InputGroupButton" component has been deprecated.\nPlease use component "InputGroupAddon".');

  var children = props.children,
      groupClassName = props.groupClassName,
      groupAttributes = props.groupAttributes,
      propsWithoutGroup = _objectWithoutPropertiesLoose(props, ["children", "groupClassName", "groupAttributes"]);

  if (typeof children === 'string') {
    var cssModule = propsWithoutGroup.cssModule,
        tag = propsWithoutGroup.tag,
        addonType = propsWithoutGroup.addonType,
        attributes = _objectWithoutPropertiesLoose(propsWithoutGroup, ["cssModule", "tag", "addonType"]);

    var allGroupAttributes = _objectSpread({}, groupAttributes, {
      cssModule: cssModule,
      tag: tag,
      addonType: addonType
    });

    return React.createElement(InputGroupAddon, _extends({}, allGroupAttributes, {
      className: groupClassName
    }), React.createElement(Button, _extends({}, attributes, {
      children: children
    })));
  }

  return React.createElement(InputGroupAddon, _extends({}, props, {
    children: children
  }));
};

InputGroupButton.propTypes = propTypes;
export default InputGroupButton;