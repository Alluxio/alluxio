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
  wrapTag: _utils.tagPropType,
  toggle: _propTypes.default.func,
  className: _propTypes.default.string,
  cssModule: _propTypes.default.object,
  children: _propTypes.default.node,
  closeAriaLabel: _propTypes.default.string,
  charCode: _propTypes.default.oneOfType([_propTypes.default.string, _propTypes.default.number]),
  close: _propTypes.default.object
};
var defaultProps = {
  tag: 'h5',
  wrapTag: 'div',
  closeAriaLabel: 'Close',
  charCode: 215
};

var ModalHeader = function ModalHeader(props) {
  var closeButton;
  var className = props.className,
      cssModule = props.cssModule,
      children = props.children,
      toggle = props.toggle,
      Tag = props.tag,
      WrapTag = props.wrapTag,
      closeAriaLabel = props.closeAriaLabel,
      charCode = props.charCode,
      close = props.close,
      attributes = (0, _objectWithoutPropertiesLoose2.default)(props, ["className", "cssModule", "children", "toggle", "tag", "wrapTag", "closeAriaLabel", "charCode", "close"]);
  var classes = (0, _utils.mapToCssModules)((0, _classnames.default)(className, 'modal-header'), cssModule);

  if (!close && toggle) {
    var closeIcon = typeof charCode === 'number' ? String.fromCharCode(charCode) : charCode;
    closeButton = _react.default.createElement("button", {
      type: "button",
      onClick: toggle,
      className: (0, _utils.mapToCssModules)('close', cssModule),
      "aria-label": closeAriaLabel
    }, _react.default.createElement("span", {
      "aria-hidden": "true"
    }, closeIcon));
  }

  return _react.default.createElement(WrapTag, (0, _extends2.default)({}, attributes, {
    className: classes
  }), _react.default.createElement(Tag, {
    className: (0, _utils.mapToCssModules)('modal-title', cssModule)
  }, children), close || closeButton);
};

ModalHeader.propTypes = propTypes;
ModalHeader.defaultProps = defaultProps;
var _default = ModalHeader;
exports.default = _default;