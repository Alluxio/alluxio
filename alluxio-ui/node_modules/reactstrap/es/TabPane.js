import _extends from "@babel/runtime/helpers/esm/extends";
import _objectWithoutPropertiesLoose from "@babel/runtime/helpers/esm/objectWithoutPropertiesLoose";
import React from 'react';
import PropTypes from 'prop-types';
import classNames from 'classnames';
import { mapToCssModules, tagPropType } from './utils';
var propTypes = {
  tag: tagPropType,
  className: PropTypes.string,
  cssModule: PropTypes.object,
  tabId: PropTypes.any
};
var defaultProps = {
  tag: 'div'
};
var contextTypes = {
  activeTabId: PropTypes.any
};
export default function TabPane(props, context) {
  var className = props.className,
      cssModule = props.cssModule,
      tabId = props.tabId,
      Tag = props.tag,
      attributes = _objectWithoutPropertiesLoose(props, ["className", "cssModule", "tabId", "tag"]);

  var classes = mapToCssModules(classNames('tab-pane', className, {
    active: tabId === context.activeTabId
  }), cssModule);
  return React.createElement(Tag, _extends({}, attributes, {
    className: classes
  }));
}
TabPane.propTypes = propTypes;
TabPane.defaultProps = defaultProps;
TabPane.contextTypes = contextTypes;