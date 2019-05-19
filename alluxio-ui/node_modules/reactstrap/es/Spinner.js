import _extends from "@babel/runtime/helpers/esm/extends";
import _objectWithoutPropertiesLoose from "@babel/runtime/helpers/esm/objectWithoutPropertiesLoose";
import React from 'react';
import PropTypes from 'prop-types';
import classNames from 'classnames';
import { mapToCssModules, tagPropType } from './utils';
var propTypes = {
  tag: tagPropType,
  type: PropTypes.string,
  size: PropTypes.string,
  color: PropTypes.string,
  className: PropTypes.string,
  cssModule: PropTypes.object,
  children: PropTypes.string
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
      attributes = _objectWithoutPropertiesLoose(props, ["className", "cssModule", "type", "size", "color", "children", "tag"]);

  var classes = mapToCssModules(classNames(className, size ? "spinner-" + type + "-" + size : false, "spinner-" + type, color ? "text-" + color : false), cssModule);
  return React.createElement(Tag, _extends({
    role: "status"
  }, attributes, {
    className: classes
  }), children && React.createElement("span", {
    className: mapToCssModules('sr-only', cssModule)
  }, children));
};

Spinner.propTypes = propTypes;
Spinner.defaultProps = defaultProps;
export default Spinner;