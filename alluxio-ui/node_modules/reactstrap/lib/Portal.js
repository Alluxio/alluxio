"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

exports.__esModule = true;
exports.default = void 0;

var _inheritsLoose2 = _interopRequireDefault(require("@babel/runtime/helpers/inheritsLoose"));

var _react = _interopRequireDefault(require("react"));

var _reactDom = _interopRequireDefault(require("react-dom"));

var _propTypes = _interopRequireDefault(require("prop-types"));

var _utils = require("./utils");

var propTypes = {
  children: _propTypes.default.node.isRequired,
  node: _propTypes.default.any
};

var Portal =
/*#__PURE__*/
function (_React$Component) {
  (0, _inheritsLoose2.default)(Portal, _React$Component);

  function Portal() {
    return _React$Component.apply(this, arguments) || this;
  }

  var _proto = Portal.prototype;

  _proto.componentWillUnmount = function componentWillUnmount() {
    if (this.defaultNode) {
      document.body.removeChild(this.defaultNode);
    }

    this.defaultNode = null;
  };

  _proto.render = function render() {
    if (!_utils.canUseDOM) {
      return null;
    }

    if (!this.props.node && !this.defaultNode) {
      this.defaultNode = document.createElement('div');
      document.body.appendChild(this.defaultNode);
    }

    return _reactDom.default.createPortal(this.props.children, this.props.node || this.defaultNode);
  };

  return Portal;
}(_react.default.Component);

Portal.propTypes = propTypes;
var _default = Portal;
exports.default = _default;