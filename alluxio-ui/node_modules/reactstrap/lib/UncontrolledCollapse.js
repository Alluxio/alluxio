"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

exports.__esModule = true;
exports.default = void 0;

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _inheritsLoose2 = _interopRequireDefault(require("@babel/runtime/helpers/inheritsLoose"));

var _assertThisInitialized2 = _interopRequireDefault(require("@babel/runtime/helpers/assertThisInitialized"));

var _react = _interopRequireWildcard(require("react"));

var _propTypes = _interopRequireDefault(require("prop-types"));

var _Collapse = _interopRequireDefault(require("./Collapse"));

var _utils = require("./utils");

var omitKeys = ['toggleEvents', 'defaultOpen'];
var propTypes = {
  defaultOpen: _propTypes.default.bool,
  toggler: _propTypes.default.string.isRequired,
  toggleEvents: _propTypes.default.arrayOf(_propTypes.default.string)
};
var defaultProps = {
  toggleEvents: _utils.defaultToggleEvents
};

var UncontrolledCollapse =
/*#__PURE__*/
function (_Component) {
  (0, _inheritsLoose2.default)(UncontrolledCollapse, _Component);

  function UncontrolledCollapse(props) {
    var _this;

    _this = _Component.call(this, props) || this;
    _this.togglers = null;
    _this.removeEventListeners = null;
    _this.toggle = _this.toggle.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.state = {
      isOpen: props.defaultOpen || false
    };
    return _this;
  }

  var _proto = UncontrolledCollapse.prototype;

  _proto.componentDidMount = function componentDidMount() {
    this.togglers = (0, _utils.findDOMElements)(this.props.toggler);

    if (this.togglers.length) {
      this.removeEventListeners = (0, _utils.addMultipleEventListeners)(this.togglers, this.toggle, this.props.toggleEvents);
    }
  };

  _proto.componentWillUnmount = function componentWillUnmount() {
    if (this.togglers.length && this.removeEventListeners) {
      this.removeEventListeners();
    }
  };

  _proto.toggle = function toggle(e) {
    this.setState(function (_ref) {
      var isOpen = _ref.isOpen;
      return {
        isOpen: !isOpen
      };
    });
    e.preventDefault();
  };

  _proto.render = function render() {
    return _react.default.createElement(_Collapse.default, (0, _extends2.default)({
      isOpen: this.state.isOpen
    }, (0, _utils.omit)(this.props, omitKeys)));
  };

  return UncontrolledCollapse;
}(_react.Component);

UncontrolledCollapse.propTypes = propTypes;
UncontrolledCollapse.defaultProps = defaultProps;
var _default = UncontrolledCollapse;
exports.default = _default;