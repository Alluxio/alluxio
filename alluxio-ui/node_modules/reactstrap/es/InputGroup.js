import _extends from "@babel/runtime/helpers/esm/extends";
import _objectWithoutPropertiesLoose from "@babel/runtime/helpers/esm/objectWithoutPropertiesLoose";
import React from 'react';
import PropTypes from 'prop-types';
import classNames from 'classnames';
import { mapToCssModules, tagPropType } from './utils';
var propTypes = {
  tag: tagPropType,
  size: PropTypes.string,
  className: PropTypes.string,
  cssModule: PropTypes.object
};
var defaultProps = {
  tag: 'div'
};

var InputGroup = function InputGroup(props) {
  var className = props.className,
      cssModule = props.cssModule,
      Tag = props.tag,
      size = props.size,
      attributes = _objectWithoutPropertiesLoose(props, ["className", "cssModule", "tag", "size"]);

  var classes = mapToCssModules(classNames(className, 'input-group', size ? "input-group-" + size : null), cssModule);
  return React.createElement(Tag, _extends({}, attributes, {
    className: classes
  }));
};

InputGroup.propTypes = propTypes;
InputGroup.defaultProps = defaultProps;
export default InputGroup;