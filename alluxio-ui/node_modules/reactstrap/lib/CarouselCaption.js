"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

exports.__esModule = true;
exports.default = void 0;

var _react = _interopRequireDefault(require("react"));

var _propTypes = _interopRequireDefault(require("prop-types"));

var _classnames = _interopRequireDefault(require("classnames"));

var _utils = require("./utils");

var CarouselCaption = function CarouselCaption(props) {
  var captionHeader = props.captionHeader,
      captionText = props.captionText,
      cssModule = props.cssModule,
      className = props.className;
  var classes = (0, _utils.mapToCssModules)((0, _classnames.default)(className, 'carousel-caption', 'd-none', 'd-md-block'), cssModule);
  return _react.default.createElement("div", {
    className: classes
  }, _react.default.createElement("h3", null, captionHeader), _react.default.createElement("p", null, captionText));
};

CarouselCaption.propTypes = {
  captionHeader: _propTypes.default.string,
  captionText: _propTypes.default.string.isRequired,
  cssModule: _propTypes.default.object,
  className: _propTypes.default.string
};
var _default = CarouselCaption;
exports.default = _default;