"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

exports.__esModule = true;
exports.default = void 0;

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _inheritsLoose2 = _interopRequireDefault(require("@babel/runtime/helpers/inheritsLoose"));

var _react = _interopRequireWildcard(require("react"));

var _reactLifecyclesCompat = require("react-lifecycles-compat");

var _propTypes = _interopRequireDefault(require("prop-types"));

var _classnames = _interopRequireDefault(require("classnames"));

var _utils = require("./utils");

var propTypes = {
  tag: _utils.tagPropType,
  activeTab: _propTypes.default.any,
  className: _propTypes.default.string,
  cssModule: _propTypes.default.object
};
var defaultProps = {
  tag: 'div'
};
var childContextTypes = {
  activeTabId: _propTypes.default.any
};

var TabContent =
/*#__PURE__*/
function (_Component) {
  (0, _inheritsLoose2.default)(TabContent, _Component);

  TabContent.getDerivedStateFromProps = function getDerivedStateFromProps(nextProps, prevState) {
    if (prevState.activeTab !== nextProps.activeTab) {
      return {
        activeTab: nextProps.activeTab
      };
    }

    return null;
  };

  function TabContent(props) {
    var _this;

    _this = _Component.call(this, props) || this;
    _this.state = {
      activeTab: _this.props.activeTab
    };
    return _this;
  }

  var _proto = TabContent.prototype;

  _proto.getChildContext = function getChildContext() {
    return {
      activeTabId: this.state.activeTab
    };
  };

  _proto.render = function render() {
    var _this$props = this.props,
        className = _this$props.className,
        cssModule = _this$props.cssModule,
        Tag = _this$props.tag;
    var attributes = (0, _utils.omit)(this.props, Object.keys(propTypes));
    var classes = (0, _utils.mapToCssModules)((0, _classnames.default)('tab-content', className), cssModule);
    return _react.default.createElement(Tag, (0, _extends2.default)({}, attributes, {
      className: classes
    }));
  };

  return TabContent;
}(_react.Component);

(0, _reactLifecyclesCompat.polyfill)(TabContent);
var _default = TabContent;
exports.default = _default;
TabContent.propTypes = propTypes;
TabContent.defaultProps = defaultProps;
TabContent.childContextTypes = childContextTypes;