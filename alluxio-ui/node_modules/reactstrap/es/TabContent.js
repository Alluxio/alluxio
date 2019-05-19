import _extends from "@babel/runtime/helpers/esm/extends";
import _inheritsLoose from "@babel/runtime/helpers/esm/inheritsLoose";
import React, { Component } from 'react';
import { polyfill } from 'react-lifecycles-compat';
import PropTypes from 'prop-types';
import classNames from 'classnames';
import { mapToCssModules, omit, tagPropType } from './utils';
var propTypes = {
  tag: tagPropType,
  activeTab: PropTypes.any,
  className: PropTypes.string,
  cssModule: PropTypes.object
};
var defaultProps = {
  tag: 'div'
};
var childContextTypes = {
  activeTabId: PropTypes.any
};

var TabContent =
/*#__PURE__*/
function (_Component) {
  _inheritsLoose(TabContent, _Component);

  TabContent.getDerivedStateFromProps = function getDerivedStateFromProps(nextProps, prevState) {
    if (prevState.activeTab !== nextProps.activeTab) {
      return {
        activeTab: nextProps.activeTab
      };
    }

    return null;
  };

  function TabContent(props) {
    var _this;

    _this = _Component.call(this, props) || this;
    _this.state = {
      activeTab: _this.props.activeTab
    };
    return _this;
  }

  var _proto = TabContent.prototype;

  _proto.getChildContext = function getChildContext() {
    return {
      activeTabId: this.state.activeTab
    };
  };

  _proto.render = function render() {
    var _this$props = this.props,
        className = _this$props.className,
        cssModule = _this$props.cssModule,
        Tag = _this$props.tag;
    var attributes = omit(this.props, Object.keys(propTypes));
    var classes = mapToCssModules(classNames('tab-content', className), cssModule);
    return React.createElement(Tag, _extends({}, attributes, {
      className: classes
    }));
  };

  return TabContent;
}(Component);

polyfill(TabContent);
export default TabContent;
TabContent.propTypes = propTypes;
TabContent.defaultProps = defaultProps;
TabContent.childContextTypes = childContextTypes;