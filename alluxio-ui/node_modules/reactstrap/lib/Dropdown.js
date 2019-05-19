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

var _reactDom = _interopRequireDefault(require("react-dom"));

var _reactPopper = require("react-popper");

var _classnames = _interopRequireDefault(require("classnames"));

var _utils = require("./utils");

/* eslint react/no-find-dom-node: 0 */
// https://github.com/yannickcr/eslint-plugin-react/blob/master/docs/rules/no-find-dom-node.md
var propTypes = {
  disabled: _propTypes.default.bool,
  dropup: (0, _utils.deprecated)(_propTypes.default.bool, 'Please use the prop "direction" with the value "up".'),
  direction: _propTypes.default.oneOf(['up', 'down', 'left', 'right']),
  group: _propTypes.default.bool,
  isOpen: _propTypes.default.bool,
  nav: _propTypes.default.bool,
  active: _propTypes.default.bool,
  addonType: _propTypes.default.oneOfType([_propTypes.default.bool, _propTypes.default.oneOf(['prepend', 'append'])]),
  size: _propTypes.default.string,
  tag: _utils.tagPropType,
  toggle: _propTypes.default.func,
  children: _propTypes.default.node,
  className: _propTypes.default.string,
  cssModule: _propTypes.default.object,
  inNavbar: _propTypes.default.bool,
  setActiveFromChild: _propTypes.default.bool
};
var defaultProps = {
  isOpen: false,
  direction: 'down',
  nav: false,
  active: false,
  addonType: false,
  inNavbar: false,
  setActiveFromChild: false
};
var childContextTypes = {
  toggle: _propTypes.default.func.isRequired,
  isOpen: _propTypes.default.bool.isRequired,
  direction: _propTypes.default.oneOf(['up', 'down', 'left', 'right']).isRequired,
  inNavbar: _propTypes.default.bool.isRequired
};

var Dropdown =
/*#__PURE__*/
function (_React$Component) {
  (0, _inheritsLoose2.default)(Dropdown, _React$Component);

  function Dropdown(props) {
    var _this;

    _this = _React$Component.call(this, props) || this;
    _this.addEvents = _this.addEvents.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.handleDocumentClick = _this.handleDocumentClick.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.handleKeyDown = _this.handleKeyDown.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.removeEvents = _this.removeEvents.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.toggle = _this.toggle.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    return _this;
  }

  var _proto = Dropdown.prototype;

  _proto.getChildContext = function getChildContext() {
    return {
      toggle: this.props.toggle,
      isOpen: this.props.isOpen,
      direction: this.props.direction === 'down' && this.props.dropup ? 'up' : this.props.direction,
      inNavbar: this.props.inNavbar
    };
  };

  _proto.componentDidMount = function componentDidMount() {
    this.handleProps();
  };

  _proto.componentDidUpdate = function componentDidUpdate(prevProps) {
    if (this.props.isOpen !== prevProps.isOpen) {
      this.handleProps();
    }
  };

  _proto.componentWillUnmount = function componentWillUnmount() {
    this.removeEvents();
  };

  _proto.getContainer = function getContainer() {
    if (this._$container) return this._$container;
    this._$container = _reactDom.default.findDOMNode(this);
    return _reactDom.default.findDOMNode(this);
  };

  _proto.getMenuCtrl = function getMenuCtrl() {
    if (this._$menuCtrl) return this._$menuCtrl;
    this._$menuCtrl = this.getContainer().querySelector('[aria-expanded]');
    return this._$menuCtrl;
  };

  _proto.getMenuItems = function getMenuItems() {
    return [].slice.call(this.getContainer().querySelectorAll('[role="menuitem"]'));
  };

  _proto.addEvents = function addEvents() {
    var _this2 = this;

    ['click', 'touchstart', 'keyup'].forEach(function (event) {
      return document.addEventListener(event, _this2.handleDocumentClick, true);
    });
  };

  _proto.removeEvents = function removeEvents() {
    var _this3 = this;

    ['click', 'touchstart', 'keyup'].forEach(function (event) {
      return document.removeEventListener(event, _this3.handleDocumentClick, true);
    });
  };

  _proto.handleDocumentClick = function handleDocumentClick(e) {
    if (e && (e.which === 3 || e.type === 'keyup' && e.which !== _utils.keyCodes.tab)) return;
    var container = this.getContainer();

    if (container.contains(e.target) && container !== e.target && (e.type !== 'keyup' || e.which === _utils.keyCodes.tab)) {
      return;
    }

    this.toggle(e);
  };

  _proto.handleKeyDown = function handleKeyDown(e) {
    var _this4 = this;

    if (/input|textarea/i.test(e.target.tagName) || _utils.keyCodes.tab === e.which && e.target.getAttribute('role') !== 'menuitem') {
      return;
    }

    e.preventDefault();
    if (this.props.disabled) return;

    if (this.getMenuCtrl() === e.target) {
      if (!this.props.isOpen && [_utils.keyCodes.space, _utils.keyCodes.enter, _utils.keyCodes.up, _utils.keyCodes.down].indexOf(e.which) > -1) {
        this.toggle(e);
        setTimeout(function () {
          return _this4.getMenuItems()[0].focus();
        });
      }
    }

    if (this.props.isOpen && e.target.getAttribute('role') === 'menuitem') {
      if ([_utils.keyCodes.tab, _utils.keyCodes.esc].indexOf(e.which) > -1) {
        this.toggle(e);
        this.getMenuCtrl().focus();
      } else if ([_utils.keyCodes.space, _utils.keyCodes.enter].indexOf(e.which) > -1) {
        e.target.click();
        this.getMenuCtrl().focus();
      } else if ([_utils.keyCodes.down, _utils.keyCodes.up].indexOf(e.which) > -1 || [_utils.keyCodes.n, _utils.keyCodes.p].indexOf(e.which) > -1 && e.ctrlKey) {
        var $menuitems = this.getMenuItems();
        var index = $menuitems.indexOf(e.target);

        if (_utils.keyCodes.up === e.which || _utils.keyCodes.p === e.which && e.ctrlKey) {
          index = index !== 0 ? index - 1 : $menuitems.length - 1;
        } else if (_utils.keyCodes.down === e.which || _utils.keyCodes.n === e.which && e.ctrlKey) {
          index = index === $menuitems.length - 1 ? 0 : index + 1;
        }

        $menuitems[index].focus();
      } else if (_utils.keyCodes.end === e.which) {
        var _$menuitems = this.getMenuItems();

        _$menuitems[_$menuitems.length - 1].focus();
      } else if (_utils.keyCodes.home === e.which) {
        var _$menuitems2 = this.getMenuItems();

        _$menuitems2[0].focus();
      } else if (e.which >= 48 && e.which <= 90) {
        var _$menuitems3 = this.getMenuItems();

        var charPressed = String.fromCharCode(e.which).toLowerCase();

        for (var i = 0; i < _$menuitems3.length; i += 1) {
          var firstLetter = _$menuitems3[i].textContent && _$menuitems3[i].textContent[0].toLowerCase();

          if (firstLetter === charPressed) {
            _$menuitems3[i].focus();

            break;
          }
        }
      }
    }
  };

  _proto.handleProps = function handleProps() {
    if (this.props.isOpen) {
      this.addEvents();
    } else {
      this.removeEvents();
    }
  };

  _proto.toggle = function toggle(e) {
    if (this.props.disabled) {
      return e && e.preventDefault();
    }

    return this.props.toggle(e);
  };

  _proto.render = function render() {
    var _classNames;

    var _omit = (0, _utils.omit)(this.props, ['toggle', 'disabled', 'inNavbar', 'direction']),
        className = _omit.className,
        cssModule = _omit.cssModule,
        dropup = _omit.dropup,
        isOpen = _omit.isOpen,
        group = _omit.group,
        size = _omit.size,
        nav = _omit.nav,
        setActiveFromChild = _omit.setActiveFromChild,
        active = _omit.active,
        addonType = _omit.addonType,
        attrs = (0, _objectWithoutPropertiesLoose2.default)(_omit, ["className", "cssModule", "dropup", "isOpen", "group", "size", "nav", "setActiveFromChild", "active", "addonType"]);

    var direction = this.props.direction === 'down' && dropup ? 'up' : this.props.direction;
    attrs.tag = attrs.tag || (nav ? 'li' : 'div');
    var subItemIsActive = false;

    if (setActiveFromChild) {
      _react.default.Children.map(this.props.children[1].props.children, function (dropdownItem) {
        if (dropdownItem && dropdownItem.props.active) subItemIsActive = true;
      });
    }

    var classes = (0, _utils.mapToCssModules)((0, _classnames.default)(className, direction !== 'down' && "drop" + direction, nav && active ? 'active' : false, setActiveFromChild && subItemIsActive ? 'active' : false, (_classNames = {}, _classNames["input-group-" + addonType] = addonType, _classNames['btn-group'] = group, _classNames["btn-group-" + size] = !!size, _classNames.dropdown = !group && !addonType, _classNames.show = isOpen, _classNames['nav-item'] = nav, _classNames)), cssModule);
    return _react.default.createElement(_reactPopper.Manager, (0, _extends2.default)({}, attrs, {
      className: classes,
      onKeyDown: this.handleKeyDown
    }));
  };

  return Dropdown;
}(_react.default.Component);

Dropdown.propTypes = propTypes;
Dropdown.defaultProps = defaultProps;
Dropdown.childContextTypes = childContextTypes;
var _default = Dropdown;
exports.default = _default;