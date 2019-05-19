"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

exports.__esModule = true;
exports.default = void 0;

var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread"));

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _objectWithoutPropertiesLoose2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutPropertiesLoose"));

var _inheritsLoose2 = _interopRequireDefault(require("@babel/runtime/helpers/inheritsLoose"));

var _assertThisInitialized2 = _interopRequireDefault(require("@babel/runtime/helpers/assertThisInitialized"));

var _react = _interopRequireDefault(require("react"));

var _propTypes = _interopRequireDefault(require("prop-types"));

var _classnames = _interopRequireDefault(require("classnames"));

var _reactTransitionGroup = require("react-transition-group");

var _utils = require("./utils");

var CarouselItem =
/*#__PURE__*/
function (_React$Component) {
  (0, _inheritsLoose2.default)(CarouselItem, _React$Component);

  function CarouselItem(props) {
    var _this;

    _this = _React$Component.call(this, props) || this;
    _this.state = {
      startAnimation: false
    };
    _this.onEnter = _this.onEnter.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.onEntering = _this.onEntering.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.onExit = _this.onExit.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.onExiting = _this.onExiting.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.onExited = _this.onExited.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    return _this;
  }

  var _proto = CarouselItem.prototype;

  _proto.onEnter = function onEnter(node, isAppearing) {
    this.setState({
      startAnimation: false
    });
    this.props.onEnter(node, isAppearing);
  };

  _proto.onEntering = function onEntering(node, isAppearing) {
    // getting this variable triggers a reflow
    var offsetHeight = node.offsetHeight;
    this.setState({
      startAnimation: true
    });
    this.props.onEntering(node, isAppearing);
    return offsetHeight;
  };

  _proto.onExit = function onExit(node) {
    this.setState({
      startAnimation: false
    });
    this.props.onExit(node);
  };

  _proto.onExiting = function onExiting(node) {
    this.setState({
      startAnimation: true
    });
    node.dispatchEvent(new CustomEvent('slide.bs.carousel'));
    this.props.onExiting(node);
  };

  _proto.onExited = function onExited(node) {
    node.dispatchEvent(new CustomEvent('slid.bs.carousel'));
    this.props.onExited(node);
  };

  _proto.render = function render() {
    var _this2 = this;

    var _this$props = this.props,
        isIn = _this$props.in,
        children = _this$props.children,
        cssModule = _this$props.cssModule,
        slide = _this$props.slide,
        Tag = _this$props.tag,
        className = _this$props.className,
        transitionProps = (0, _objectWithoutPropertiesLoose2.default)(_this$props, ["in", "children", "cssModule", "slide", "tag", "className"]);
    return _react.default.createElement(_reactTransitionGroup.Transition, (0, _extends2.default)({}, transitionProps, {
      enter: slide,
      exit: slide,
      in: isIn,
      onEnter: this.onEnter,
      onEntering: this.onEntering,
      onExit: this.onExit,
      onExiting: this.onExiting,
      onExited: this.onExited
    }), function (status) {
      var direction = _this2.context.direction;
      var isActive = status === _utils.TransitionStatuses.ENTERED || status === _utils.TransitionStatuses.EXITING;
      var directionClassName = (status === _utils.TransitionStatuses.ENTERING || status === _utils.TransitionStatuses.EXITING) && _this2.state.startAnimation && (direction === 'right' ? 'carousel-item-left' : 'carousel-item-right');
      var orderClassName = status === _utils.TransitionStatuses.ENTERING && (direction === 'right' ? 'carousel-item-next' : 'carousel-item-prev');
      var itemClasses = (0, _utils.mapToCssModules)((0, _classnames.default)(className, 'carousel-item', isActive && 'active', directionClassName, orderClassName), cssModule);
      return _react.default.createElement(Tag, {
        className: itemClasses
      }, children);
    });
  };

  return CarouselItem;
}(_react.default.Component);

CarouselItem.propTypes = (0, _objectSpread2.default)({}, _reactTransitionGroup.Transition.propTypes, {
  tag: _utils.tagPropType,
  in: _propTypes.default.bool,
  cssModule: _propTypes.default.object,
  children: _propTypes.default.node,
  slide: _propTypes.default.bool,
  className: _propTypes.default.string
});
CarouselItem.defaultProps = (0, _objectSpread2.default)({}, _reactTransitionGroup.Transition.defaultProps, {
  tag: 'div',
  timeout: _utils.TransitionTimeouts.Carousel,
  slide: true
});
CarouselItem.contextTypes = {
  direction: _propTypes.default.string
};
var _default = CarouselItem;
exports.default = _default;