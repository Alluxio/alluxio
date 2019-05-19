"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

exports.__esModule = true;
exports.default = void 0;

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _objectWithoutPropertiesLoose2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutPropertiesLoose"));

var _inheritsLoose2 = _interopRequireDefault(require("@babel/runtime/helpers/inheritsLoose"));

var _assertThisInitialized2 = _interopRequireDefault(require("@babel/runtime/helpers/assertThisInitialized"));

var _objectSpread2 = _interopRequireDefault(require("@babel/runtime/helpers/objectSpread"));

var _react = _interopRequireWildcard(require("react"));

var _propTypes = _interopRequireDefault(require("prop-types"));

var _classnames = _interopRequireDefault(require("classnames"));

var _reactTransitionGroup = require("react-transition-group");

var _utils = require("./utils");

var _transitionStatusToCl;

var propTypes = (0, _objectSpread2.default)({}, _reactTransitionGroup.Transition.propTypes, {
  isOpen: _propTypes.default.bool,
  children: _propTypes.default.oneOfType([_propTypes.default.arrayOf(_propTypes.default.node), _propTypes.default.node]),
  tag: _utils.tagPropType,
  className: _propTypes.default.node,
  navbar: _propTypes.default.bool,
  cssModule: _propTypes.default.object,
  innerRef: _propTypes.default.oneOfType([_propTypes.default.func, _propTypes.default.string, _propTypes.default.object])
});
var defaultProps = (0, _objectSpread2.default)({}, _reactTransitionGroup.Transition.defaultProps, {
  isOpen: false,
  appear: false,
  enter: true,
  exit: true,
  tag: 'div',
  timeout: _utils.TransitionTimeouts.Collapse
});
var transitionStatusToClassHash = (_transitionStatusToCl = {}, _transitionStatusToCl[_utils.TransitionStatuses.ENTERING] = 'collapsing', _transitionStatusToCl[_utils.TransitionStatuses.ENTERED] = 'collapse show', _transitionStatusToCl[_utils.TransitionStatuses.EXITING] = 'collapsing', _transitionStatusToCl[_utils.TransitionStatuses.EXITED] = 'collapse', _transitionStatusToCl);

function getTransitionClass(status) {
  return transitionStatusToClassHash[status] || 'collapse';
}

function getHeight(node) {
  return node.scrollHeight;
}

var Collapse =
/*#__PURE__*/
function (_Component) {
  (0, _inheritsLoose2.default)(Collapse, _Component);

  function Collapse(props) {
    var _this;

    _this = _Component.call(this, props) || this;
    _this.state = {
      height: null
    };
    ['onEntering', 'onEntered', 'onExit', 'onExiting', 'onExited'].forEach(function (name) {
      _this[name] = _this[name].bind((0, _assertThisInitialized2.default)((0, _assertThisInitialized2.default)(_this)));
    });
    return _this;
  }

  var _proto = Collapse.prototype;

  _proto.onEntering = function onEntering(node, isAppearing) {
    this.setState({
      height: getHeight(node)
    });
    this.props.onEntering(node, isAppearing);
  };

  _proto.onEntered = function onEntered(node, isAppearing) {
    this.setState({
      height: null
    });
    this.props.onEntered(node, isAppearing);
  };

  _proto.onExit = function onExit(node) {
    this.setState({
      height: getHeight(node)
    });
    this.props.onExit(node);
  };

  _proto.onExiting = function onExiting(node) {
    // getting this variable triggers a reflow
    var _unused = node.offsetHeight; // eslint-disable-line no-unused-vars

    this.setState({
      height: 0
    });
    this.props.onExiting(node);
  };

  _proto.onExited = function onExited(node) {
    this.setState({
      height: null
    });
    this.props.onExited(node);
  };

  _proto.render = function render() {
    var _this2 = this;

    var _this$props = this.props,
        Tag = _this$props.tag,
        isOpen = _this$props.isOpen,
        className = _this$props.className,
        navbar = _this$props.navbar,
        cssModule = _this$props.cssModule,
        children = _this$props.children,
        innerRef = _this$props.innerRef,
        otherProps = (0, _objectWithoutPropertiesLoose2.default)(_this$props, ["tag", "isOpen", "className", "navbar", "cssModule", "children", "innerRef"]);
    var height = this.state.height;
    var transitionProps = (0, _utils.pick)(otherProps, _utils.TransitionPropTypeKeys);
    var childProps = (0, _utils.omit)(otherProps, _utils.TransitionPropTypeKeys);
    return _react.default.createElement(_reactTransitionGroup.Transition, (0, _extends2.default)({}, transitionProps, {
      in: isOpen,
      onEntering: this.onEntering,
      onEntered: this.onEntered,
      onExit: this.onExit,
      onExiting: this.onExiting,
      onExited: this.onExited
    }), function (status) {
      var collapseClass = getTransitionClass(status);
      var classes = (0, _utils.mapToCssModules)((0, _classnames.default)(className, collapseClass, navbar && 'navbar-collapse'), cssModule);
      var style = height === null ? null : {
        height: height
      };
      return _react.default.createElement(Tag, (0, _extends2.default)({}, childProps, {
        style: (0, _objectSpread2.default)({}, childProps.style, style),
        className: classes,
        ref: _this2.props.innerRef
      }), children);
    });
  };

  return Collapse;
}(_react.Component);

Collapse.propTypes = propTypes;
Collapse.defaultProps = defaultProps;
var _default = Collapse;
exports.default = _default;