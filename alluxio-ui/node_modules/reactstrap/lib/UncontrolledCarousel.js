"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

exports.__esModule = true;
exports.default = void 0;

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _objectWithoutPropertiesLoose2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutPropertiesLoose"));

var _inheritsLoose2 = _interopRequireDefault(require("@babel/runtime/helpers/inheritsLoose"));

var _assertThisInitialized2 = _interopRequireDefault(require("@babel/runtime/helpers/assertThisInitialized"));

var _react = _interopRequireWildcard(require("react"));

var _propTypes = _interopRequireDefault(require("prop-types"));

var _Carousel = _interopRequireDefault(require("./Carousel"));

var _CarouselItem = _interopRequireDefault(require("./CarouselItem"));

var _CarouselControl = _interopRequireDefault(require("./CarouselControl"));

var _CarouselIndicators = _interopRequireDefault(require("./CarouselIndicators"));

var _CarouselCaption = _interopRequireDefault(require("./CarouselCaption"));

var propTypes = {
  items: _propTypes.default.array.isRequired,
  indicators: _propTypes.default.bool,
  controls: _propTypes.default.bool,
  autoPlay: _propTypes.default.bool,
  defaultActiveIndex: _propTypes.default.number,
  activeIndex: _propTypes.default.number,
  next: _propTypes.default.func,
  previous: _propTypes.default.func,
  goToIndex: _propTypes.default.func
};

var UncontrolledCarousel =
/*#__PURE__*/
function (_Component) {
  (0, _inheritsLoose2.default)(UncontrolledCarousel, _Component);

  function UncontrolledCarousel(props) {
    var _this;

    _this = _Component.call(this, props) || this;
    _this.animating = false;
    _this.state = {
      activeIndex: props.defaultActiveIndex || 0
    };
    _this.next = _this.next.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.previous = _this.previous.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.goToIndex = _this.goToIndex.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.onExiting = _this.onExiting.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.onExited = _this.onExited.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    return _this;
  }

  var _proto = UncontrolledCarousel.prototype;

  _proto.onExiting = function onExiting() {
    this.animating = true;
  };

  _proto.onExited = function onExited() {
    this.animating = false;
  };

  _proto.next = function next() {
    if (this.animating) return;
    var nextIndex = this.state.activeIndex === this.props.items.length - 1 ? 0 : this.state.activeIndex + 1;
    this.setState({
      activeIndex: nextIndex
    });
  };

  _proto.previous = function previous() {
    if (this.animating) return;
    var nextIndex = this.state.activeIndex === 0 ? this.props.items.length - 1 : this.state.activeIndex - 1;
    this.setState({
      activeIndex: nextIndex
    });
  };

  _proto.goToIndex = function goToIndex(newIndex) {
    if (this.animating) return;
    this.setState({
      activeIndex: newIndex
    });
  };

  _proto.render = function render() {
    var _this2 = this;

    var _this$props = this.props,
        defaultActiveIndex = _this$props.defaultActiveIndex,
        autoPlay = _this$props.autoPlay,
        indicators = _this$props.indicators,
        controls = _this$props.controls,
        items = _this$props.items,
        goToIndex = _this$props.goToIndex,
        props = (0, _objectWithoutPropertiesLoose2.default)(_this$props, ["defaultActiveIndex", "autoPlay", "indicators", "controls", "items", "goToIndex"]);
    var activeIndex = this.state.activeIndex;
    var slides = items.map(function (item) {
      return _react.default.createElement(_CarouselItem.default, {
        onExiting: _this2.onExiting,
        onExited: _this2.onExited,
        key: item.src
      }, _react.default.createElement("img", {
        className: "d-block w-100",
        src: item.src,
        alt: item.altText
      }), _react.default.createElement(_CarouselCaption.default, {
        captionText: item.caption,
        captionHeader: item.header || item.caption
      }));
    });
    return _react.default.createElement(_Carousel.default, (0, _extends2.default)({
      activeIndex: activeIndex,
      next: this.next,
      previous: this.previous,
      ride: autoPlay ? 'carousel' : undefined
    }, props), indicators && _react.default.createElement(_CarouselIndicators.default, {
      items: items,
      activeIndex: props.activeIndex || activeIndex,
      onClickHandler: goToIndex || this.goToIndex
    }), slides, controls && _react.default.createElement(_CarouselControl.default, {
      direction: "prev",
      directionText: "Previous",
      onClickHandler: props.previous || this.previous
    }), controls && _react.default.createElement(_CarouselControl.default, {
      direction: "next",
      directionText: "Next",
      onClickHandler: props.next || this.next
    }));
  };

  return UncontrolledCarousel;
}(_react.Component);

UncontrolledCarousel.propTypes = propTypes;
UncontrolledCarousel.defaultProps = {
  controls: true,
  indicators: true,
  autoPlay: true
};
var _default = UncontrolledCarousel;
exports.default = _default;