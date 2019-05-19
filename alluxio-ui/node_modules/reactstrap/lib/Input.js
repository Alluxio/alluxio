"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

exports.__esModule = true;
exports.default = void 0;

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _objectWithoutPropertiesLoose2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutPropertiesLoose"));

var _inheritsLoose2 = _interopRequireDefault(require("@babel/runtime/helpers/inheritsLoose"));

var _assertThisInitialized2 = _interopRequireDefault(require("@babel/runtime/helpers/assertThisInitialized"));

var _react = _interopRequireDefault(require("react"));

var _propTypes = _interopRequireDefault(require("prop-types"));

var _classnames = _interopRequireDefault(require("classnames"));

var _utils = require("./utils");

/* eslint react/prefer-stateless-function: 0 */
var propTypes = {
  children: _propTypes.default.node,
  type: _propTypes.default.string,
  size: _propTypes.default.string,
  bsSize: _propTypes.default.string,
  state: (0, _utils.deprecated)(_propTypes.default.string, 'Please use the props "valid" and "invalid" to indicate the state.'),
  valid: _propTypes.default.bool,
  invalid: _propTypes.default.bool,
  tag: _utils.tagPropType,
  innerRef: _propTypes.default.oneOfType([_propTypes.default.object, _propTypes.default.func, _propTypes.default.string]),
  static: (0, _utils.deprecated)(_propTypes.default.bool, 'Please use the prop "plaintext"'),
  plaintext: _propTypes.default.bool,
  addon: _propTypes.default.bool,
  className: _propTypes.default.string,
  cssModule: _propTypes.default.object
};
var defaultProps = {
  type: 'text'
};

var Input =
/*#__PURE__*/
function (_React$Component) {
  (0, _inheritsLoose2.default)(Input, _React$Component);

  function Input(props) {
    var _this;

    _this = _React$Component.call(this, props) || this;
    _this.getRef = _this.getRef.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.focus = _this.focus.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    return _this;
  }

  var _proto = Input.prototype;

  _proto.getRef = function getRef(ref) {
    if (this.props.innerRef) {
      this.props.innerRef(ref);
    }

    this.ref = ref;
  };

  _proto.focus = function focus() {
    if (this.ref) {
      this.ref.focus();
    }
  };

  _proto.render = function render() {
    var _this$props = this.props,
        className = _this$props.className,
        cssModule = _this$props.cssModule,
        type = _this$props.type,
        bsSize = _this$props.bsSize,
        state = _this$props.state,
        valid = _this$props.valid,
        invalid = _this$props.invalid,
        tag = _this$props.tag,
        addon = _this$props.addon,
        staticInput = _this$props.static,
        plaintext = _this$props.plaintext,
        innerRef = _this$props.innerRef,
        attributes = (0, _objectWithoutPropertiesLoose2.default)(_this$props, ["className", "cssModule", "type", "bsSize", "state", "valid", "invalid", "tag", "addon", "static", "plaintext", "innerRef"]);
    var checkInput = ['radio', 'checkbox'].indexOf(type) > -1;
    var isNotaNumber = new RegExp('\\D', 'g');
    var fileInput = type === 'file';
    var textareaInput = type === 'textarea';
    var selectInput = type === 'select';
    var Tag = tag || (selectInput || textareaInput ? type : 'input');
    var formControlClass = 'form-control';

    if (plaintext || staticInput) {
      formControlClass = formControlClass + "-plaintext";
      Tag = tag || 'input';
    } else if (fileInput) {
      formControlClass = formControlClass + "-file";
    } else if (checkInput) {
      if (addon) {
        formControlClass = null;
      } else {
        formControlClass = 'form-check-input';
      }
    }

    if (state && typeof valid === 'undefined' && typeof invalid === 'undefined') {
      if (state === 'danger') {
        invalid = true;
      } else if (state === 'success') {
        valid = true;
      }
    }

    if (attributes.size && isNotaNumber.test(attributes.size)) {
      (0, _utils.warnOnce)('Please use the prop "bsSize" instead of the "size" to bootstrap\'s input sizing.');
      bsSize = attributes.size;
      delete attributes.size;
    }

    var classes = (0, _utils.mapToCssModules)((0, _classnames.default)(className, invalid && 'is-invalid', valid && 'is-valid', bsSize ? "form-control-" + bsSize : false, formControlClass), cssModule);

    if (Tag === 'input' || tag && typeof tag === 'function') {
      attributes.type = type;
    }

    if (attributes.children && !(plaintext || staticInput || type === 'select' || typeof Tag !== 'string' || Tag === 'select')) {
      (0, _utils.warnOnce)("Input with a type of \"" + type + "\" cannot have children. Please use \"value\"/\"defaultValue\" instead.");
      delete attributes.children;
    }

    return _react.default.createElement(Tag, (0, _extends2.default)({}, attributes, {
      ref: innerRef,
      className: classes
    }));
  };

  return Input;
}(_react.default.Component);

Input.propTypes = propTypes;
Input.defaultProps = defaultProps;
var _default = Input;
exports.default = _default;