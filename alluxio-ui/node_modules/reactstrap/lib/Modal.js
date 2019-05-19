"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

exports.__esModule = true;
exports.default = void 0;

var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread"));

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _inheritsLoose2 = _interopRequireDefault(require("@babel/runtime/helpers/inheritsLoose"));

var _assertThisInitialized2 = _interopRequireDefault(require("@babel/runtime/helpers/assertThisInitialized"));

var _react = _interopRequireDefault(require("react"));

var _propTypes = _interopRequireDefault(require("prop-types"));

var _classnames = _interopRequireDefault(require("classnames"));

var _Portal = _interopRequireDefault(require("./Portal"));

var _Fade = _interopRequireDefault(require("./Fade"));

var _utils = require("./utils");

function noop() {}

var FadePropTypes = _propTypes.default.shape(_Fade.default.propTypes);

var propTypes = {
  isOpen: _propTypes.default.bool,
  autoFocus: _propTypes.default.bool,
  centered: _propTypes.default.bool,
  size: _propTypes.default.string,
  toggle: _propTypes.default.func,
  keyboard: _propTypes.default.bool,
  role: _propTypes.default.string,
  labelledBy: _propTypes.default.string,
  backdrop: _propTypes.default.oneOfType([_propTypes.default.bool, _propTypes.default.oneOf(['static'])]),
  onEnter: _propTypes.default.func,
  onExit: _propTypes.default.func,
  onOpened: _propTypes.default.func,
  onClosed: _propTypes.default.func,
  children: _propTypes.default.node,
  className: _propTypes.default.string,
  wrapClassName: _propTypes.default.string,
  modalClassName: _propTypes.default.string,
  backdropClassName: _propTypes.default.string,
  contentClassName: _propTypes.default.string,
  external: _propTypes.default.node,
  fade: _propTypes.default.bool,
  cssModule: _propTypes.default.object,
  zIndex: _propTypes.default.oneOfType([_propTypes.default.number, _propTypes.default.string]),
  backdropTransition: FadePropTypes,
  modalTransition: FadePropTypes,
  innerRef: _propTypes.default.oneOfType([_propTypes.default.object, _propTypes.default.string, _propTypes.default.func])
};
var propsToOmit = Object.keys(propTypes);
var defaultProps = {
  isOpen: false,
  autoFocus: true,
  centered: false,
  role: 'dialog',
  backdrop: true,
  keyboard: true,
  zIndex: 1050,
  fade: true,
  onOpened: noop,
  onClosed: noop,
  modalTransition: {
    timeout: _utils.TransitionTimeouts.Modal
  },
  backdropTransition: {
    mountOnEnter: true,
    timeout: _utils.TransitionTimeouts.Fade // uses standard fade transition

  }
};

var Modal =
/*#__PURE__*/
function (_React$Component) {
  (0, _inheritsLoose2.default)(Modal, _React$Component);

  function Modal(props) {
    var _this;

    _this = _React$Component.call(this, props) || this;
    _this._element = null;
    _this._originalBodyPadding = null;
    _this.getFocusableChildren = _this.getFocusableChildren.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.handleBackdropClick = _this.handleBackdropClick.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.handleBackdropMouseDown = _this.handleBackdropMouseDown.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.handleEscape = _this.handleEscape.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.handleTab = _this.handleTab.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.onOpened = _this.onOpened.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.onClosed = _this.onClosed.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.state = {
      isOpen: props.isOpen
    };

    if (props.isOpen) {
      _this.init();
    }

    return _this;
  }

  var _proto = Modal.prototype;

  _proto.componentDidMount = function componentDidMount() {
    if (this.props.onEnter) {
      this.props.onEnter();
    }

    if (this.state.isOpen && this.props.autoFocus) {
      this.setFocus();
    }

    this._isMounted = true;
  };

  _proto.componentWillReceiveProps = function componentWillReceiveProps(nextProps) {
    if (nextProps.isOpen && !this.props.isOpen) {
      this.setState({
        isOpen: nextProps.isOpen
      });
    }
  };

  _proto.componentWillUpdate = function componentWillUpdate(nextProps, nextState) {
    if (nextState.isOpen && !this.state.isOpen) {
      this.init();
    }
  };

  _proto.componentDidUpdate = function componentDidUpdate(prevProps, prevState) {
    if (this.props.autoFocus && this.state.isOpen && !prevState.isOpen) {
      this.setFocus();
    }

    if (this._element && prevProps.zIndex !== this.props.zIndex) {
      this._element.style.zIndex = this.props.zIndex;
    }
  };

  _proto.componentWillUnmount = function componentWillUnmount() {
    if (this.props.onExit) {
      this.props.onExit();
    }

    if (this.state.isOpen) {
      this.destroy();
    }

    this._isMounted = false;
  };

  _proto.onOpened = function onOpened(node, isAppearing) {
    this.props.onOpened();
    (this.props.modalTransition.onEntered || noop)(node, isAppearing);
  };

  _proto.onClosed = function onClosed(node) {
    // so all methods get called before it is unmounted
    this.props.onClosed();
    (this.props.modalTransition.onExited || noop)(node);
    this.destroy();

    if (this._isMounted) {
      this.setState({
        isOpen: false
      });
    }
  };

  _proto.setFocus = function setFocus() {
    if (this._dialog && this._dialog.parentNode && typeof this._dialog.parentNode.focus === 'function') {
      this._dialog.parentNode.focus();
    }
  };

  _proto.getFocusableChildren = function getFocusableChildren() {
    return this._element.querySelectorAll(_utils.focusableElements.join(', '));
  };

  _proto.getFocusedChild = function getFocusedChild() {
    var currentFocus;
    var focusableChildren = this.getFocusableChildren();

    try {
      currentFocus = document.activeElement;
    } catch (err) {
      currentFocus = focusableChildren[0];
    }

    return currentFocus;
  } // not mouseUp because scrollbar fires it, shouldn't close when user scrolls
  ;

  _proto.handleBackdropClick = function handleBackdropClick(e) {
    if (e.target === this._mouseDownElement) {
      e.stopPropagation();
      if (!this.props.isOpen || this.props.backdrop !== true) return;
      var backdrop = this._dialog ? this._dialog.parentNode : null;

      if (backdrop && e.target === backdrop && this.props.toggle) {
        this.props.toggle(e);
      }
    }
  };

  _proto.handleTab = function handleTab(e) {
    if (e.which !== 9) return;
    var focusableChildren = this.getFocusableChildren();
    var totalFocusable = focusableChildren.length;
    var currentFocus = this.getFocusedChild();
    var focusedIndex = 0;

    for (var i = 0; i < totalFocusable; i += 1) {
      if (focusableChildren[i] === currentFocus) {
        focusedIndex = i;
        break;
      }
    }

    if (e.shiftKey && focusedIndex === 0) {
      e.preventDefault();
      focusableChildren[totalFocusable - 1].focus();
    } else if (!e.shiftKey && focusedIndex === totalFocusable - 1) {
      e.preventDefault();
      focusableChildren[0].focus();
    }
  };

  _proto.handleBackdropMouseDown = function handleBackdropMouseDown(e) {
    this._mouseDownElement = e.target;
  };

  _proto.handleEscape = function handleEscape(e) {
    if (this.props.isOpen && this.props.keyboard && e.keyCode === 27 && this.props.toggle) {
      e.preventDefault();
      e.stopPropagation();
      this.props.toggle(e);
    }
  };

  _proto.init = function init() {
    try {
      this._triggeringElement = document.activeElement;
    } catch (err) {
      this._triggeringElement = null;
    }

    this._element = document.createElement('div');

    this._element.setAttribute('tabindex', '-1');

    this._element.style.position = 'relative';
    this._element.style.zIndex = this.props.zIndex;
    this._originalBodyPadding = (0, _utils.getOriginalBodyPadding)();
    (0, _utils.conditionallyUpdateScrollbar)();
    document.body.appendChild(this._element);

    if (Modal.openCount === 0) {
      document.body.className = (0, _classnames.default)(document.body.className, (0, _utils.mapToCssModules)('modal-open', this.props.cssModule));
    }

    Modal.openCount += 1;
  };

  _proto.destroy = function destroy() {
    if (this._element) {
      document.body.removeChild(this._element);
      this._element = null;
    }

    if (this._triggeringElement) {
      if (this._triggeringElement.focus) this._triggeringElement.focus();
      this._triggeringElement = null;
    }

    if (Modal.openCount <= 1) {
      var modalOpenClassName = (0, _utils.mapToCssModules)('modal-open', this.props.cssModule); // Use regex to prevent matching `modal-open` as part of a different class, e.g. `my-modal-opened`

      var modalOpenClassNameRegex = new RegExp("(^| )" + modalOpenClassName + "( |$)");
      document.body.className = document.body.className.replace(modalOpenClassNameRegex, ' ').trim();
    }

    Modal.openCount -= 1;
    (0, _utils.setScrollbarWidth)(this._originalBodyPadding);
  };

  _proto.renderModalDialog = function renderModalDialog() {
    var _classNames,
        _this2 = this;

    var attributes = (0, _utils.omit)(this.props, propsToOmit);
    var dialogBaseClass = 'modal-dialog';
    return _react.default.createElement("div", (0, _extends2.default)({}, attributes, {
      className: (0, _utils.mapToCssModules)((0, _classnames.default)(dialogBaseClass, this.props.className, (_classNames = {}, _classNames["modal-" + this.props.size] = this.props.size, _classNames[dialogBaseClass + "-centered"] = this.props.centered, _classNames)), this.props.cssModule),
      role: "document",
      ref: function ref(c) {
        _this2._dialog = c;
      }
    }), _react.default.createElement("div", {
      className: (0, _utils.mapToCssModules)((0, _classnames.default)('modal-content', this.props.contentClassName), this.props.cssModule)
    }, this.props.children));
  };

  _proto.render = function render() {
    if (this.state.isOpen) {
      var _this$props = this.props,
          wrapClassName = _this$props.wrapClassName,
          modalClassName = _this$props.modalClassName,
          backdropClassName = _this$props.backdropClassName,
          cssModule = _this$props.cssModule,
          isOpen = _this$props.isOpen,
          backdrop = _this$props.backdrop,
          role = _this$props.role,
          labelledBy = _this$props.labelledBy,
          external = _this$props.external,
          innerRef = _this$props.innerRef;
      var modalAttributes = {
        onClick: this.handleBackdropClick,
        onMouseDown: this.handleBackdropMouseDown,
        onKeyUp: this.handleEscape,
        onKeyDown: this.handleTab,
        style: {
          display: 'block'
        },
        'aria-labelledby': labelledBy,
        role: role,
        tabIndex: '-1'
      };
      var hasTransition = this.props.fade;
      var modalTransition = (0, _objectSpread2.default)({}, _Fade.default.defaultProps, this.props.modalTransition, {
        baseClass: hasTransition ? this.props.modalTransition.baseClass : '',
        timeout: hasTransition ? this.props.modalTransition.timeout : 0
      });
      var backdropTransition = (0, _objectSpread2.default)({}, _Fade.default.defaultProps, this.props.backdropTransition, {
        baseClass: hasTransition ? this.props.backdropTransition.baseClass : '',
        timeout: hasTransition ? this.props.backdropTransition.timeout : 0
      });
      var Backdrop = backdrop && (hasTransition ? _react.default.createElement(_Fade.default, (0, _extends2.default)({}, backdropTransition, {
        in: isOpen && !!backdrop,
        cssModule: cssModule,
        className: (0, _utils.mapToCssModules)((0, _classnames.default)('modal-backdrop', backdropClassName), cssModule)
      })) : _react.default.createElement("div", {
        className: (0, _utils.mapToCssModules)((0, _classnames.default)('modal-backdrop', 'show', backdropClassName), cssModule)
      }));
      return _react.default.createElement(_Portal.default, {
        node: this._element
      }, _react.default.createElement("div", {
        className: (0, _utils.mapToCssModules)(wrapClassName)
      }, _react.default.createElement(_Fade.default, (0, _extends2.default)({}, modalAttributes, modalTransition, {
        in: isOpen,
        onEntered: this.onOpened,
        onExited: this.onClosed,
        cssModule: cssModule,
        className: (0, _utils.mapToCssModules)((0, _classnames.default)('modal', modalClassName), cssModule),
        innerRef: innerRef
      }), external, this.renderModalDialog()), Backdrop));
    }

    return null;
  };

  return Modal;
}(_react.default.Component);

Modal.propTypes = propTypes;
Modal.defaultProps = defaultProps;
Modal.openCount = 0;
var _default = Modal;
exports.default = _default;