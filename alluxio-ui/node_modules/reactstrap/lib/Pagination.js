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
  children: _propTypes.default.node,
  className: _propTypes.default.string,
  listClassName: _propTypes.default.string,
  cssModule: _propTypes.default.object,
  size: _propTypes.default.string,
  tag: _utils.tagPropType,
  listTag: _utils.tagPropType,
  'aria-label': _propTypes.default.string
};
var defaultProps = {
  tag: 'nav',
  listTag: 'ul',
  'aria-label': 'pagination'
};

var Pagination = function Pagination(props) {
  var _classNames;

  var className = props.className,
      listClassName = props.listClassName,
      cssModule = props.cssModule,
      size = props.size,
      Tag = props.tag,
      ListTag = props.listTag,
      label = props['aria-label'],
      attributes = (0, _objectWithoutPropertiesLoose2.default)(props, ["className", "listClassName", "cssModule", "size", "tag", "listTag", "aria-label"]);
  var classes = (0, _utils.mapToCssModules)((0, _classnames.default)(className), cssModule);
  var listClasses = (0, _utils.mapToCssModules)((0, _classnames.default)(listClassName, 'pagination', (_classNames = {}, _classNames["pagination-" + size] = !!size, _classNames)), cssModule);
  return _react.default.createElement(Tag, {
    className: classes,
    "aria-label": label
  }, _react.default.createElement(ListTag, (0, _extends2.default)({}, attributes, {
    className: listClasses
  })));
};

Pagination.propTypes = propTypes;
Pagination.defaultProps = defaultProps;
var _default = Pagination;
exports.default = _default;