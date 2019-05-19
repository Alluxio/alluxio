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
  type: _propTypes.default.string,
  className: _propTypes.default.string,
  cssModule: _propTypes.default.object,
  children: _propTypes.default.node
};
var defaultProps = {
  tag: 'button',
  type: 'button'
};

var NavbarToggler = function NavbarToggler(props) {
  var className = props.className,
      cssModule = props.cssModule,
      children = props.children,
      Tag = props.tag,
      attributes = (0, _objectWithoutPropertiesLoose2.default)(props, ["className", "cssModule", "children", "tag"]);
  var classes = (0, _utils.mapToCssModules)((0, _classnames.default)(className, 'navbar-toggler'), cssModule);
  return _react.default.createElement(Tag, (0, _extends2.default)({}, attributes, {
    className: classes
  }), children || _react.default.createElement("span", {
    className: (0, _utils.mapToCssModules)('navbar-toggler-icon', cssModule)
  }));
};

NavbarToggler.propTypes = propTypes;
NavbarToggler.defaultProps = defaultProps;
var _default = NavbarToggler;
exports.default = _default;