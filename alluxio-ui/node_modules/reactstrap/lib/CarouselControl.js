"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

exports.__esModule = true;
exports.default = void 0;

var _react = _interopRequireDefault(require("react"));

var _propTypes = _interopRequireDefault(require("prop-types"));

var _classnames = _interopRequireDefault(require("classnames"));

var _utils = require("./utils");

var CarouselControl = function CarouselControl(props) {
  var direction = props.direction,
      onClickHandler = props.onClickHandler,
      cssModule = props.cssModule,
      directionText = props.directionText,
      className = props.className;
  var anchorClasses = (0, _utils.mapToCssModules)((0, _classnames.default)(className, "carousel-control-" + direction), cssModule);
  var iconClasses = (0, _utils.mapToCssModules)((0, _classnames.default)("carousel-control-" + direction + "-icon"), cssModule);
  var screenReaderClasses = (0, _utils.mapToCssModules)((0, _classnames.default)('sr-only'), cssModule);
  return _react.default.createElement("a", {
    className: anchorClasses,
    role: "button",
    tabIndex: "0",
    onClick: function onClick(e) {
      e.preventDefault();
      onClickHandler();
    }
  }, _react.default.createElement("span", {
    className: iconClasses,
    "aria-hidden": "true"
  }), _react.default.createElement("span", {
    className: screenReaderClasses
  }, directionText || direction));
};

CarouselControl.propTypes = {
  direction: _propTypes.default.oneOf(['prev', 'next']).isRequired,
  onClickHandler: _propTypes.default.func.isRequired,
  cssModule: _propTypes.default.object,
  directionText: _propTypes.default.string,
  className: _propTypes.default.string
};
var _default = CarouselControl;
exports.default = _default;