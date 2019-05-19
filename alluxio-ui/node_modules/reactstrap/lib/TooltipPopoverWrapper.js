"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

exports.__esModule = true;
exports.default = exports.propTypes = void 0;

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _inheritsLoose2 = _interopRequireDefault(require("@babel/runtime/helpers/inheritsLoose"));

var _assertThisInitialized2 = _interopRequireDefault(require("@babel/runtime/helpers/assertThisInitialized"));

var _react = _interopRequireDefault(require("react"));

var _propTypes = _interopRequireDefault(require("prop-types"));

var _PopperContent = _interopRequireDefault(require("./PopperContent"));

var _utils = require("./utils");

var propTypes = {
  placement: _propTypes.default.oneOf(_utils.PopperPlacements),
  target: _utils.targetPropType.isRequired,
  container: _utils.targetPropType,
  isOpen: _propTypes.default.bool,
  disabled: _propTypes.default.bool,
  hideArrow: _propTypes.default.bool,
  boundariesElement: _propTypes.default.oneOfType([_propTypes.default.string, _utils.DOMElement]),
  className: _propTypes.default.string,
  innerClassName: _propTypes.default.string,
  arrowClassName: _propTypes.default.string,
  cssModule: _propTypes.default.object,
  toggle: _propTypes.default.func,
  autohide: _propTypes.default.bool,
  placementPrefix: _propTypes.default.string,
  delay: _propTypes.default.oneOfType([_propTypes.default.shape({
    show: _propTypes.default.number,
    hide: _propTypes.default.number
  }), _propTypes.default.number]),
  modifiers: _propTypes.default.object,
  offset: _propTypes.default.oneOfType([_propTypes.default.string, _propTypes.default.number]),
  innerRef: _propTypes.default.oneOfType([_propTypes.default.func, _propTypes.default.string, _propTypes.default.object]),
  trigger: _propTypes.default.string
};
exports.propTypes = propTypes;
var DEFAULT_DELAYS = {
  show: 0,
  hide: 250
};
var defaultProps = {
  isOpen: false,
  hideArrow: false,
  autohide: false,
  delay: DEFAULT_DELAYS,
  toggle: function toggle() {},
  trigger: 'click'
};

function isInDOMSubtree(element, subtreeRoot) {
  return subtreeRoot && (element === subtreeRoot || subtreeRoot.contains(element));
}

var TooltipPopoverWrapper =
/*#__PURE__*/
function (_React$Component) {
  (0, _inheritsLoose2.default)(TooltipPopoverWrapper, _React$Component);

  function TooltipPopoverWrapper(props) {
    var _this;

    _this = _React$Component.call(this, props) || this;
    _this._target = null;
    _this.addTargetEvents = _this.addTargetEvents.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.handleDocumentClick = _this.handleDocumentClick.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.removeTargetEvents = _this.removeTargetEvents.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.toggle = _this.toggle.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.showWithDelay = _this.showWithDelay.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.hideWithDelay = _this.hideWithDelay.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.onMouseOverTooltipContent = _this.onMouseOverTooltipContent.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.onMouseLeaveTooltipContent = _this.onMouseLeaveTooltipContent.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.show = _this.show.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.hide = _this.hide.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.onEscKeyDown = _this.onEscKeyDown.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.getRef = _this.getRef.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    return _this;
  }

  var _proto = TooltipPopoverWrapper.prototype;

  _proto.componentDidMount = function componentDidMount() {
    this.updateTarget();
  };

  _proto.componentWillUnmount = function componentWillUnmount() {
    this.removeTargetEvents();
  };

  _proto.onMouseOverTooltipContent = function onMouseOverTooltipContent() {
    if (this.props.trigger.indexOf('hover') > -1 && !this.props.autohide) {
      if (this._hideTimeout) {
        this.clearHideTimeout();
      }
    }
  };

  _proto.onMouseLeaveTooltipContent = function onMouseLeaveTooltipContent(e) {
    if (this.props.trigger.indexOf('hover') > -1 && !this.props.autohide) {
      if (this._showTimeout) {
        this.clearShowTimeout();
      }

      e.persist();
      this._hideTimeout = setTimeout(this.hide.bind(this, e), this.getDelay('hide'));
    }
  };

  _proto.onEscKeyDown = function onEscKeyDown(e) {
    if (e.key === 'Escape') {
      this.hide(e);
    }
  };

  _proto.getRef = function getRef(ref) {
    var innerRef = this.props.innerRef;

    if (innerRef) {
      if (typeof innerRef === 'function') {
        innerRef(ref);
      } else if (typeof innerRef === 'object') {
        innerRef.current = ref;
      }
    }

    this._popover = ref;
  };

  _proto.getDelay = function getDelay(key) {
    var delay = this.props.delay;

    if (typeof delay === 'object') {
      return isNaN(delay[key]) ? DEFAULT_DELAYS[key] : delay[key];
    }

    return delay;
  };

  _proto.show = function show(e) {
    if (!this.props.isOpen) {
      this.clearShowTimeout();
      this.toggle(e);
    }
  };

  _proto.showWithDelay = function showWithDelay(e) {
    if (this._hideTimeout) {
      this.clearHideTimeout();
    }

    this._showTimeout = setTimeout(this.show.bind(this, e), this.getDelay('show'));
  };

  _proto.hide = function hide(e) {
    if (this.props.isOpen) {
      this.clearHideTimeout();
      this.toggle(e);
    }
  };

  _proto.hideWithDelay = function hideWithDelay(e) {
    if (this._showTimeout) {
      this.clearShowTimeout();
    }

    this._hideTimeout = setTimeout(this.hide.bind(this, e), this.getDelay('hide'));
  };

  _proto.clearShowTimeout = function clearShowTimeout() {
    clearTimeout(this._showTimeout);
    this._showTimeout = undefined;
  };

  _proto.clearHideTimeout = function clearHideTimeout() {
    clearTimeout(this._hideTimeout);
    this._hideTimeout = undefined;
  };

  _proto.handleDocumentClick = function handleDocumentClick(e) {
    var triggers = this.props.trigger.split(' ');

    if (triggers.indexOf('legacy') > -1 && (this.props.isOpen || isInDOMSubtree(e.target, this._target))) {
      if (this._hideTimeout) {
        this.clearHideTimeout();
      }

      if (this.props.isOpen && !isInDOMSubtree(e.target, this._popover)) {
        this.hideWithDelay(e);
      } else {
        this.showWithDelay(e);
      }
    } else if (triggers.indexOf('click') > -1 && isInDOMSubtree(e.target, this._target)) {
      if (this._hideTimeout) {
        this.clearHideTimeout();
      }

      if (!this.props.isOpen) {
        this.showWithDelay(e);
      } else {
        this.hideWithDelay(e);
      }
    }
  };

  _proto.addTargetEvents = function addTargetEvents() {
    var _this2 = this;

    if (this.props.trigger) {
      var triggers = this.props.trigger.split(' ');

      if (triggers.indexOf('manual') === -1) {
        if (triggers.indexOf('click') > -1 || triggers.indexOf('legacy') > -1) {
          ['click', 'touchstart'].forEach(function (event) {
            return document.addEventListener(event, _this2.handleDocumentClick, true);
          });
        }

        if (this._target) {
          if (triggers.indexOf('hover') > -1) {
            this._target.addEventListener('mouseover', this.showWithDelay, true);

            this._target.addEventListener('mouseout', this.hideWithDelay, true);
          }

          if (triggers.indexOf('focus') > -1) {
            this._target.addEventListener('focusin', this.show, true);

            this._target.addEventListener('focusout', this.hide, true);
          }

          this._target.addEventListener('keydown', this.onEscKeyDown, true);
        }
      }
    }
  };

  _proto.removeTargetEvents = function removeTargetEvents() {
    var _this3 = this;

    if (this._target) {
      this._target.removeEventListener('mouseover', this.showWithDelay, true);

      this._target.removeEventListener('mouseout', this.hideWithDelay, true);

      this._target.removeEventListener('keydown', this.onEscKeyDown, true);

      this._target.removeEventListener('focusin', this.show, true);

      this._target.removeEventListener('focusout', this.hide, true);
    }

    ['click', 'touchstart'].forEach(function (event) {
      return document.removeEventListener(event, _this3.handleDocumentClick, true);
    });
  };

  _proto.updateTarget = function updateTarget() {
    var newTarget = (0, _utils.getTarget)(this.props.target);

    if (newTarget !== this._target) {
      this.removeTargetEvents();
      this._target = newTarget;
      this.addTargetEvents();
    }
  };

  _proto.toggle = function toggle(e) {
    if (this.props.disabled) {
      return e && e.preventDefault();
    }

    return this.props.toggle(e);
  };

  _proto.render = function render() {
    if (!this.props.isOpen) {
      return null;
    }

    this.updateTarget();
    var _this$props = this.props,
        className = _this$props.className,
        cssModule = _this$props.cssModule,
        innerClassName = _this$props.innerClassName,
        target = _this$props.target,
        isOpen = _this$props.isOpen,
        hideArrow = _this$props.hideArrow,
        boundariesElement = _this$props.boundariesElement,
        placement = _this$props.placement,
        placementPrefix = _this$props.placementPrefix,
        arrowClassName = _this$props.arrowClassName,
        container = _this$props.container,
        modifiers = _this$props.modifiers,
        offset = _this$props.offset;
    var attributes = (0, _utils.omit)(this.props, Object.keys(propTypes));
    var popperClasses = (0, _utils.mapToCssModules)(className, cssModule);
    var classes = (0, _utils.mapToCssModules)(innerClassName, cssModule);
    return _react.default.createElement(_PopperContent.default, {
      className: popperClasses,
      target: target,
      isOpen: isOpen,
      hideArrow: hideArrow,
      boundariesElement: boundariesElement,
      placement: placement,
      placementPrefix: placementPrefix,
      arrowClassName: arrowClassName,
      container: container,
      modifiers: modifiers,
      offset: offset,
      cssModule: cssModule
    }, _react.default.createElement("div", (0, _extends2.default)({}, attributes, {
      ref: this.getRef,
      className: classes,
      role: "tooltip",
      "aria-hidden": isOpen,
      onMouseOver: this.onMouseOverTooltipContent,
      onMouseLeave: this.onMouseLeaveTooltipContent,
      onKeyDown: this.onEscKeyDown
    })));
  };

  return TooltipPopoverWrapper;
}(_react.default.Component);

TooltipPopoverWrapper.propTypes = propTypes;
TooltipPopoverWrapper.defaultProps = defaultProps;
var _default = TooltipPopoverWrapper;
exports.default = _default;