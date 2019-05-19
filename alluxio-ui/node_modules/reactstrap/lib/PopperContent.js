"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

exports.__esModule = true;
exports.default = void 0;

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread"));

var _objectWithoutPropertiesLoose2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutPropertiesLoose"));

var _inheritsLoose2 = _interopRequireDefault(require("@babel/runtime/helpers/inheritsLoose"));

var _assertThisInitialized2 = _interopRequireDefault(require("@babel/runtime/helpers/assertThisInitialized"));

var _react = _interopRequireDefault(require("react"));

var _propTypes = _interopRequireDefault(require("prop-types"));

var _reactDom = _interopRequireDefault(require("react-dom"));

var _classnames = _interopRequireDefault(require("classnames"));

var _reactPopper = require("react-popper");

var _utils = require("./utils");

var propTypes = {
  children: _propTypes.default.node.isRequired,
  className: _propTypes.default.string,
  placement: _propTypes.default.string,
  placementPrefix: _propTypes.default.string,
  arrowClassName: _propTypes.default.string,
  hideArrow: _propTypes.default.bool,
  tag: _utils.tagPropType,
  isOpen: _propTypes.default.bool.isRequired,
  cssModule: _propTypes.default.object,
  offset: _propTypes.default.oneOfType([_propTypes.default.string, _propTypes.default.number]),
  fallbackPlacement: _propTypes.default.oneOfType([_propTypes.default.string, _propTypes.default.array]),
  flip: _propTypes.default.bool,
  container: _utils.targetPropType,
  target: _utils.targetPropType.isRequired,
  modifiers: _propTypes.default.object,
  boundariesElement: _propTypes.default.oneOfType([_propTypes.default.string, _utils.DOMElement])
};
var defaultProps = {
  boundariesElement: 'scrollParent',
  placement: 'auto',
  hideArrow: false,
  isOpen: false,
  offset: 0,
  fallbackPlacement: 'flip',
  flip: true,
  container: 'body',
  modifiers: {}
};
var childContextTypes = {
  popperManager: _propTypes.default.object.isRequired
};

var PopperContent =
/*#__PURE__*/
function (_React$Component) {
  (0, _inheritsLoose2.default)(PopperContent, _React$Component);

  function PopperContent(props) {
    var _this;

    _this = _React$Component.call(this, props) || this;
    _this.handlePlacementChange = _this.handlePlacementChange.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.setTargetNode = _this.setTargetNode.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.getTargetNode = _this.getTargetNode.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.getRef = _this.getRef.bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    _this.state = {};
    return _this;
  }

  var _proto = PopperContent.prototype;

  _proto.getChildContext = function getChildContext() {
    return {
      popperManager: {
        setTargetNode: this.setTargetNode,
        getTargetNode: this.getTargetNode
      }
    };
  };

  _proto.componentDidUpdate = function componentDidUpdate() {
    if (this._element && this._element.childNodes && this._element.childNodes[0] && this._element.childNodes[0].focus) {
      this._element.childNodes[0].focus();
    }
  };

  _proto.setTargetNode = function setTargetNode(node) {
    this.targetNode = node;
  };

  _proto.getTargetNode = function getTargetNode() {
    return this.targetNode;
  };

  _proto.getContainerNode = function getContainerNode() {
    return (0, _utils.getTarget)(this.props.container);
  };

  _proto.getRef = function getRef(ref) {
    this._element = ref;
  };

  _proto.handlePlacementChange = function handlePlacementChange(data) {
    if (this.state.placement !== data.placement) {
      this.setState({
        placement: data.placement
      });
    }

    return data;
  };

  _proto.renderChildren = function renderChildren() {
    var _this$props = this.props,
        cssModule = _this$props.cssModule,
        children = _this$props.children,
        isOpen = _this$props.isOpen,
        flip = _this$props.flip,
        target = _this$props.target,
        offset = _this$props.offset,
        fallbackPlacement = _this$props.fallbackPlacement,
        placementPrefix = _this$props.placementPrefix,
        _arrowClassName = _this$props.arrowClassName,
        hideArrow = _this$props.hideArrow,
        className = _this$props.className,
        tag = _this$props.tag,
        container = _this$props.container,
        modifiers = _this$props.modifiers,
        boundariesElement = _this$props.boundariesElement,
        attrs = (0, _objectWithoutPropertiesLoose2.default)(_this$props, ["cssModule", "children", "isOpen", "flip", "target", "offset", "fallbackPlacement", "placementPrefix", "arrowClassName", "hideArrow", "className", "tag", "container", "modifiers", "boundariesElement"]);
    var arrowClassName = (0, _utils.mapToCssModules)((0, _classnames.default)('arrow', _arrowClassName), cssModule);
    var placement = (this.state.placement || attrs.placement).split('-')[0];
    var popperClassName = (0, _utils.mapToCssModules)((0, _classnames.default)(className, placementPrefix ? placementPrefix + "-" + placement : placement), this.props.cssModule);
    var extendedModifiers = (0, _objectSpread2.default)({
      offset: {
        offset: offset
      },
      flip: {
        enabled: flip,
        behavior: fallbackPlacement
      },
      preventOverflow: {
        boundariesElement: boundariesElement
      },
      update: {
        enabled: true,
        order: 950,
        fn: this.handlePlacementChange
      }
    }, modifiers);
    return _react.default.createElement(_reactPopper.Popper, (0, _extends2.default)({
      modifiers: extendedModifiers
    }, attrs, {
      component: tag,
      className: popperClassName,
      "x-placement": this.state.placement || attrs.placement
    }), children, !hideArrow && _react.default.createElement(_reactPopper.Arrow, {
      className: arrowClassName
    }));
  };

  _proto.render = function render() {
    this.setTargetNode((0, _utils.getTarget)(this.props.target));

    if (this.props.isOpen) {
      return this.props.container === 'inline' ? this.renderChildren() : _reactDom.default.createPortal(_react.default.createElement("div", {
        ref: this.getRef
      }, this.renderChildren()), this.getContainerNode());
    }

    return null;
  };

  return PopperContent;
}(_react.default.Component);

PopperContent.propTypes = propTypes;
PopperContent.defaultProps = defaultProps;
PopperContent.childContextTypes = childContextTypes;
var _default = PopperContent;
exports.default = _default;