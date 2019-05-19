"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

exports.__esModule = true;
exports.default = void 0;

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _objectWithoutPropertiesLoose2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutPropertiesLoose"));

var _inheritsLoose2 = _interopRequireDefault(require("@babel/runtime/helpers/inheritsLoose"));

var _assertThisInitialized2 = _interopRequireDefault(require("@babel/runtime/helpers/assertThisInitialized"));

var _react = _interopRequireDefault(require("react"));

var _propTypes = _interopRequireDefault(require("prop-types"));

var _classnames = _interopRequireDefault(require("classnames"));

var _utils = require("./utils");

var propTypes = {
  tag: _utils.tagPropType,
  innerRef: _propTypes.default.oneOfType([_propTypes.default.object, _propTypes.default.func, _propTypes.default.string]),
  disabled: _propTypes.default.bool,
  active: _propTypes.default.bool,
  className: _propTypes.default.string,
  cssModule: _propTypes.default.object,
  onClick: _propTypes.default.func,
  href: _propTypes.default.any
};
var defaultProps = {
  tag: 'a'
};

var NavLink =
/*#__PURE__*/
function (_React$Component) {
  (0, _inheritsLoose2.default)(NavLink, _React$Component);

  function NavLink(props) {
    var _this;

    _this = _React$Component.call(this, props) || this;
    _this.onClick = _this.onClick.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    return _this;
  }

  var _proto = NavLink.prototype;

  _proto.onClick = function onClick(e) {
    if (this.props.disabled) {
      e.preventDefault();
      return;
    }

    if (this.props.href === '#') {
      e.preventDefault();
    }

    if (this.props.onClick) {
      this.props.onClick(e);
    }
  };

  _proto.render = function render() {
    var _this$props = this.props,
        className = _this$props.className,
        cssModule = _this$props.cssModule,
        active = _this$props.active,
        Tag = _this$props.tag,
        innerRef = _this$props.innerRef,
        attributes = (0, _objectWithoutPropertiesLoose2.default)(_this$props, ["className", "cssModule", "active", "tag", "innerRef"]);
    var classes = (0, _utils.mapToCssModules)((0, _classnames.default)(className, 'nav-link', {
      disabled: attributes.disabled,
      active: active
    }), cssModule);
    return _react.default.createElement(Tag, (0, _extends2.default)({}, attributes, {
      ref: innerRef,
      onClick: this.onClick,
      className: classes
    }));
  };

  return NavLink;
}(_react.default.Component);

NavLink.propTypes = propTypes;
NavLink.defaultProps = defaultProps;
var _default = NavLink;
exports.default = _default;