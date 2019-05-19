"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

exports.__esModule = true;
exports.default = void 0;

var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread"));

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _inheritsLoose2 = _interopRequireDefault(require("@babel/runtime/helpers/inheritsLoose"));

var _assertThisInitialized2 = _interopRequireDefault(require("@babel/runtime/helpers/assertThisInitialized"));

var _react = _interopRequireWildcard(require("react"));

var _propTypes = _interopRequireDefault(require("prop-types"));

var _ButtonDropdown = _interopRequireDefault(require("./ButtonDropdown"));

var _utils = require("./utils");

var omitKeys = ['defaultOpen'];

var UncontrolledButtonDropdown =
/*#__PURE__*/
function (_Component) {
  (0, _inheritsLoose2.default)(UncontrolledButtonDropdown, _Component);

  function UncontrolledButtonDropdown(props) {
    var _this;

    _this = _Component.call(this, props) || this;
    _this.state = {
      isOpen: props.defaultOpen || false
    };
    _this.toggle = _this.toggle.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    return _this;
  }

  var _proto = UncontrolledButtonDropdown.prototype;

  _proto.toggle = function toggle() {
    this.setState({
      isOpen: !this.state.isOpen
    });
  };

  _proto.render = function render() {
    return _react.default.createElement(_ButtonDropdown.default, (0, _extends2.default)({
      isOpen: this.state.isOpen,
      toggle: this.toggle
    }, (0, _utils.omit)(this.props, omitKeys)));
  };

  return UncontrolledButtonDropdown;
}(_react.Component);

exports.default = UncontrolledButtonDropdown;
UncontrolledButtonDropdown.propTypes = (0, _objectSpread2.default)({
  defaultOpen: _propTypes.default.bool
}, _ButtonDropdown.default.propTypes);