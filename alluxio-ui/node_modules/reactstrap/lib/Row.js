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
  noGutters: _propTypes.default.bool,
  className: _propTypes.default.string,
  cssModule: _propTypes.default.object,
  form: _propTypes.default.bool
};
var defaultProps = {
  tag: 'div'
};

var Row = function Row(props) {
  var className = props.className,
      cssModule = props.cssModule,
      noGutters = props.noGutters,
      Tag = props.tag,
      form = props.form,
      attributes = (0, _objectWithoutPropertiesLoose2.default)(props, ["className", "cssModule", "noGutters", "tag", "form"]);
  var classes = (0, _utils.mapToCssModules)((0, _classnames.default)(className, noGutters ? 'no-gutters' : null, form ? 'form-row' : 'row'), cssModule);
  return _react.default.createElement(Tag, (0, _extends2.default)({}, attributes, {
    className: classes
  }));
};

Row.propTypes = propTypes;
Row.defaultProps = defaultProps;
var _default = Row;
exports.default = _default;