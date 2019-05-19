"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

exports.__esModule = true;
exports.default = void 0;

var _react = _interopRequireDefault(require("react"));

var _propTypes = _interopRequireDefault(require("prop-types"));

var _classnames = _interopRequireDefault(require("classnames"));

var _utils = require("./utils");

var CarouselIndicators = function CarouselIndicators(props) {
  var items = props.items,
      activeIndex = props.activeIndex,
      cssModule = props.cssModule,
      onClickHandler = props.onClickHandler,
      className = props.className;
  var listClasses = (0, _utils.mapToCssModules)((0, _classnames.default)(className, 'carousel-indicators'), cssModule);
  var indicators = items.map(function (item, idx) {
    var indicatorClasses = (0, _utils.mapToCssModules)((0, _classnames.default)({
      active: activeIndex === idx
    }), cssModule);
    return _react.default.createElement("li", {
      key: "" + (item.key || Object.values(item).join('')),
      onClick: function onClick(e) {
        e.preventDefault();
        onClickHandler(idx);
      },
      className: indicatorClasses
    });
  });
  return _react.default.createElement("ol", {
    className: listClasses
  }, indicators);
};

CarouselIndicators.propTypes = {
  items: _propTypes.default.array.isRequired,
  activeIndex: _propTypes.default.number.isRequired,
  cssModule: _propTypes.default.object,
  onClickHandler: _propTypes.default.func.isRequired,
  className: _propTypes.default.string
};
var _default = CarouselIndicators;
exports.default = _default;