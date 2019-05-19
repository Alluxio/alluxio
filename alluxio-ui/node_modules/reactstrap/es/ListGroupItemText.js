import _extends from "@babel/runtime/helpers/esm/extends";
import _objectWithoutPropertiesLoose from "@babel/runtime/helpers/esm/objectWithoutPropertiesLoose";
import React from 'react';
import PropTypes from 'prop-types';
import classNames from 'classnames';
import { mapToCssModules, tagPropType } from './utils';
var propTypes = {
  tag: tagPropType,
  className: PropTypes.any,
  cssModule: PropTypes.object
};
var defaultProps = {
  tag: 'p'
};

var ListGroupItemText = function ListGroupItemText(props) {
  var className = props.className,
      cssModule = props.cssModule,
      Tag = props.tag,
      attributes = _objectWithoutPropertiesLoose(props, ["className", "cssModule", "tag"]);

  var classes = mapToCssModules(classNames(className, 'list-group-item-text'), cssModule);
  return React.createElement(Tag, _extends({}, attributes, {
    className: classes
  }));
};

ListGroupItemText.propTypes = propTypes;
ListGroupItemText.defaultProps = defaultProps;
export default ListGroupItemText;