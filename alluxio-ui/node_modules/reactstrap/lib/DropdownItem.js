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
  children: _propTypes.default.node,
  active: _propTypes.default.bool,
  disabled: _propTypes.default.bool,
  divider: _propTypes.default.bool,
  tag: _utils.tagPropType,
  header: _propTypes.default.bool,
  onClick: _propTypes.default.func,
  className: _propTypes.default.string,
  cssModule: _propTypes.default.object,
  toggle: _propTypes.default.bool
};
var contextTypes = {
  toggle: _propTypes.default.func
};
var defaultProps = {
  tag: 'button',
  toggle: true
};

var DropdownItem =
/*#__PURE__*/
function (_React$Component) {
  (0, _inheritsLoose2.default)(DropdownItem, _React$Component);

  function DropdownItem(props) {
    var _this;

    _this = _React$Component.call(this, props) || this;
    _this.onClick = _this.onClick.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.getTabIndex = _this.getTabIndex.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    return _this;
  }

  var _proto = DropdownItem.prototype;

  _proto.onClick = function onClick(e) {
    if (this.props.disabled || this.props.header || this.props.divider) {
      e.preventDefault();
      return;
    }

    if (this.props.onClick) {
      this.props.onClick(e);
    }

    if (this.props.toggle) {
      this.context.toggle(e);
    }
  };

  _proto.getTabIndex = function getTabIndex() {
    if (this.props.disabled || this.props.header || this.props.divider) {
      return '-1';
    }

    return '0';
  };

  _proto.render = function render() {
    var tabIndex = this.getTabIndex();
    var role = tabIndex > -1 ? 'menuitem' : undefined;

    var _omit = (0, _utils.omit)(this.props, ['toggle']),
        className = _omit.className,
        cssModule = _omit.cssModule,
        divider = _omit.divider,
        Tag = _omit.tag,
        header = _omit.header,
        active = _omit.active,
        props = (0, _objectWithoutPropertiesLoose2.default)(_omit, ["className", "cssModule", "divider", "tag", "header", "active"]);

    var classes = (0, _utils.mapToCssModules)((0, _classnames.default)(className, {
      disabled: props.disabled,
      'dropdown-item': !divider && !header,
      active: active,
      'dropdown-header': header,
      'dropdown-divider': divider
    }), cssModule);

    if (Tag === 'button') {
      if (header) {
        Tag = 'h6';
      } else if (divider) {
        Tag = 'div';
      } else if (props.href) {
        Tag = 'a';
      }
    }

    return _react.default.createElement(Tag, (0, _extends2.default)({
      type: Tag === 'button' && (props.onClick || this.props.toggle) ? 'button' : undefined
    }, props, {
      tabIndex: tabIndex,
      role: role,
      className: classes,
      onClick: this.onClick
    }));
  };

  return DropdownItem;
}(_react.default.Component);

DropdownItem.propTypes = propTypes;
DropdownItem.defaultProps = defaultProps;
DropdownItem.contextTypes = contextTypes;
var _default = DropdownItem;
exports.default = _default;