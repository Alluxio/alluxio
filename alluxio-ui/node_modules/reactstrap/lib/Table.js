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
  className: _propTypes.default.string,
  cssModule: _propTypes.default.object,
  size: _propTypes.default.string,
  bordered: _propTypes.default.bool,
  borderless: _propTypes.default.bool,
  striped: _propTypes.default.bool,
  inverse: (0, _utils.deprecated)(_propTypes.default.bool, 'Please use the prop "dark"'),
  dark: _propTypes.default.bool,
  hover: _propTypes.default.bool,
  responsive: _propTypes.default.oneOfType([_propTypes.default.bool, _propTypes.default.string]),
  tag: _utils.tagPropType,
  responsiveTag: _utils.tagPropType,
  innerRef: _propTypes.default.oneOfType([_propTypes.default.func, _propTypes.default.string, _propTypes.default.object])
};
var defaultProps = {
  tag: 'table',
  responsiveTag: 'div'
};

var Table = function Table(props) {
  var className = props.className,
      cssModule = props.cssModule,
      size = props.size,
      bordered = props.bordered,
      borderless = props.borderless,
      striped = props.striped,
      inverse = props.inverse,
      dark = props.dark,
      hover = props.hover,
      responsive = props.responsive,
      Tag = props.tag,
      ResponsiveTag = props.responsiveTag,
      innerRef = props.innerRef,
      attributes = (0, _objectWithoutPropertiesLoose2.default)(props, ["className", "cssModule", "size", "bordered", "borderless", "striped", "inverse", "dark", "hover", "responsive", "tag", "responsiveTag", "innerRef"]);
  var classes = (0, _utils.mapToCssModules)((0, _classnames.default)(className, 'table', size ? 'table-' + size : false, bordered ? 'table-bordered' : false, borderless ? 'table-borderless' : false, striped ? 'table-striped' : false, dark || inverse ? 'table-dark' : false, hover ? 'table-hover' : false), cssModule);

  var table = _react.default.createElement(Tag, (0, _extends2.default)({}, attributes, {
    ref: innerRef,
    className: classes
  }));

  if (responsive) {
    var responsiveClassName = responsive === true ? 'table-responsive' : "table-responsive-" + responsive;
    return _react.default.createElement(ResponsiveTag, {
      className: responsiveClassName
    }, table);
  }

  return table;
};

Table.propTypes = propTypes;
Table.defaultProps = defaultProps;
var _default = Table;
exports.default = _default;