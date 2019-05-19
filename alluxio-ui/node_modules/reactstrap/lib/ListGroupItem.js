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
  active: _propTypes.default.bool,
  disabled: _propTypes.default.bool,
  color: _propTypes.default.string,
  action: _propTypes.default.bool,
  className: _propTypes.default.any,
  cssModule: _propTypes.default.object
};
var defaultProps = {
  tag: 'li'
};

var handleDisabledOnClick = function handleDisabledOnClick(e) {
  e.preventDefault();
};

var ListGroupItem = function ListGroupItem(props) {
  var className = props.className,
      cssModule = props.cssModule,
      Tag = props.tag,
      active = props.active,
      disabled = props.disabled,
      action = props.action,
      color = props.color,
      attributes = (0, _objectWithoutPropertiesLoose2.default)(props, ["className", "cssModule", "tag", "active", "disabled", "action", "color"]);
  var classes = (0, _utils.mapToCssModules)((0, _classnames.default)(className, active ? 'active' : false, disabled ? 'disabled' : false, action ? 'list-group-item-action' : false, color ? "list-group-item-" + color : false, 'list-group-item'), cssModule); // Prevent click event when disabled.

  if (disabled) {
    attributes.onClick = handleDisabledOnClick;
  }

  return _react.default.createElement(Tag, (0, _extends2.default)({}, attributes, {
    className: classes
  }));
};

ListGroupItem.propTypes = propTypes;
ListGroupItem.defaultProps = defaultProps;
var _default = ListGroupItem;
exports.default = _default;