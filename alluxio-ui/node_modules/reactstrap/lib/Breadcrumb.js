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
  tag: _utils.tagPropType,
  listTag: _utils.tagPropType,
  className: _propTypes.default.string,
  listClassName: _propTypes.default.string,
  cssModule: _propTypes.default.object,
  children: _propTypes.default.node,
  'aria-label': _propTypes.default.string
};
var defaultProps = {
  tag: 'nav',
  listTag: 'ol',
  'aria-label': 'breadcrumb'
};

var Breadcrumb = function Breadcrumb(props) {
  var className = props.className,
      listClassName = props.listClassName,
      cssModule = props.cssModule,
      children = props.children,
      Tag = props.tag,
      ListTag = props.listTag,
      label = props['aria-label'],
      attributes = (0, _objectWithoutPropertiesLoose2.default)(props, ["className", "listClassName", "cssModule", "children", "tag", "listTag", "aria-label"]);
  var classes = (0, _utils.mapToCssModules)((0, _classnames.default)(className), cssModule);
  var listClasses = (0, _utils.mapToCssModules)((0, _classnames.default)('breadcrumb', listClassName), cssModule);
  return _react.default.createElement(Tag, (0, _extends2.default)({}, attributes, {
    className: classes,
    "aria-label": label
  }), _react.default.createElement(ListTag, {
    className: listClasses
  }, children));
};

Breadcrumb.propTypes = propTypes;
Breadcrumb.defaultProps = defaultProps;
var _default = Breadcrumb;
exports.default = _default;