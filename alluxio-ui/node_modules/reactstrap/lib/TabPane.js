"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

exports.__esModule = true;
exports.default = TabPane;

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _objectWithoutPropertiesLoose2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutPropertiesLoose"));

var _react = _interopRequireDefault(require("react"));

var _propTypes = _interopRequireDefault(require("prop-types"));

var _classnames = _interopRequireDefault(require("classnames"));

var _utils = require("./utils");

var propTypes = {
  tag: _utils.tagPropType,
  className: _propTypes.default.string,
  cssModule: _propTypes.default.object,
  tabId: _propTypes.default.any
};
var defaultProps = {
  tag: 'div'
};
var contextTypes = {
  activeTabId: _propTypes.default.any
};

function TabPane(props, context) {
  var className = props.className,
      cssModule = props.cssModule,
      tabId = props.tabId,
      Tag = props.tag,
      attributes = (0, _objectWithoutPropertiesLoose2.default)(props, ["className", "cssModule", "tabId", "tag"]);
  var classes = (0, _utils.mapToCssModules)((0, _classnames.default)('tab-pane', className, {
    active: tabId === context.activeTabId
  }), cssModule);
  return _react.default.createElement(Tag, (0, _extends2.default)({}, attributes, {
    className: classes
  }));
}

TabPane.propTypes = propTypes;
TabPane.defaultProps = defaultProps;
TabPane.contextTypes = contextTypes;