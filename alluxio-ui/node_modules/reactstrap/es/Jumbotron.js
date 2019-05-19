import _extends from "@babel/runtime/helpers/esm/extends";
import _objectWithoutPropertiesLoose from "@babel/runtime/helpers/esm/objectWithoutPropertiesLoose";
import React from 'react';
import PropTypes from 'prop-types';
import classNames from 'classnames';
import { mapToCssModules, tagPropType } from './utils';
var propTypes = {
  tag: tagPropType,
  fluid: PropTypes.bool,
  className: PropTypes.string,
  cssModule: PropTypes.object
};
var defaultProps = {
  tag: 'div'
};

var Jumbotron = function Jumbotron(props) {
  var className = props.className,
      cssModule = props.cssModule,
      Tag = props.tag,
      fluid = props.fluid,
      attributes = _objectWithoutPropertiesLoose(props, ["className", "cssModule", "tag", "fluid"]);

  var classes = mapToCssModules(classNames(className, 'jumbotron', fluid ? 'jumbotron-fluid' : false), cssModule);
  return React.createElement(Tag, _extends({}, attributes, {
    className: classes
  }));
};

Jumbotron.propTypes = propTypes;
Jumbotron.defaultProps = defaultProps;
export default Jumbotron;