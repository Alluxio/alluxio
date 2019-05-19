import _extends from "@babel/runtime/helpers/esm/extends";
import _objectSpread from "@babel/runtime/helpers/esm/objectSpread";
import _objectWithoutPropertiesLoose from "@babel/runtime/helpers/esm/objectWithoutPropertiesLoose";
import _inheritsLoose from "@babel/runtime/helpers/esm/inheritsLoose";
import _assertThisInitialized from "@babel/runtime/helpers/esm/assertThisInitialized";
import React from 'react';
import PropTypes from 'prop-types';
import ReactDOM from 'react-dom';
import classNames from 'classnames';
import { Arrow, Popper as ReactPopper } from 'react-popper';
import { getTarget, targetPropType, mapToCssModules, DOMElement, tagPropType } from './utils';
var propTypes = {
  children: PropTypes.node.isRequired,
  className: PropTypes.string,
  placement: PropTypes.string,
  placementPrefix: PropTypes.string,
  arrowClassName: PropTypes.string,
  hideArrow: PropTypes.bool,
  tag: tagPropType,
  isOpen: PropTypes.bool.isRequired,
  cssModule: PropTypes.object,
  offset: PropTypes.oneOfType([PropTypes.string, PropTypes.number]),
  fallbackPlacement: PropTypes.oneOfType([PropTypes.string, PropTypes.array]),
  flip: PropTypes.bool,
  container: targetPropType,
  target: targetPropType.isRequired,
  modifiers: PropTypes.object,
  boundariesElement: PropTypes.oneOfType([PropTypes.string, DOMElement])
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
  popperManager: PropTypes.object.isRequired
};

var PopperContent =
/*#__PURE__*/
function (_React$Component) {
  _inheritsLoose(PopperContent, _React$Component);

  function PopperContent(props) {
    var _this;

    _this = _React$Component.call(this, props) || this;
    _this.handlePlacementChange = _this.handlePlacementChange.bind(_assertThisInitialized(_assertThisInitialized(_this)));
    _this.setTargetNode = _this.setTargetNode.bind(_assertThisInitialized(_assertThisInitialized(_this)));
    _this.getTargetNode = _this.getTargetNode.bind(_assertThisInitialized(_assertThisInitialized(_this)));
    _this.getRef = _this.getRef.bind(_assertThisInitialized(_assertThisInitialized(_this)));
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
    return getTarget(this.props.container);
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
        attrs = _objectWithoutPropertiesLoose(_this$props, ["cssModule", "children", "isOpen", "flip", "target", "offset", "fallbackPlacement", "placementPrefix", "arrowClassName", "hideArrow", "className", "tag", "container", "modifiers", "boundariesElement"]);

    var arrowClassName = mapToCssModules(classNames('arrow', _arrowClassName), cssModule);
    var placement = (this.state.placement || attrs.placement).split('-')[0];
    var popperClassName = mapToCssModules(classNames(className, placementPrefix ? placementPrefix + "-" + placement : placement), this.props.cssModule);

    var extendedModifiers = _objectSpread({
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

    return React.createElement(ReactPopper, _extends({
      modifiers: extendedModifiers
    }, attrs, {
      component: tag,
      className: popperClassName,
      "x-placement": this.state.placement || attrs.placement
    }), children, !hideArrow && React.createElement(Arrow, {
      className: arrowClassName
    }));
  };

  _proto.render = function render() {
    this.setTargetNode(getTarget(this.props.target));

    if (this.props.isOpen) {
      return this.props.container === 'inline' ? this.renderChildren() : ReactDOM.createPortal(React.createElement("div", {
        ref: this.getRef
      }, this.renderChildren()), this.getContainerNode());
    }

    return null;
  };

  return PopperContent;
}(React.Component);

PopperContent.propTypes = propTypes;
PopperContent.defaultProps = defaultProps;
PopperContent.childContextTypes = childContextTypes;
export default PopperContent;