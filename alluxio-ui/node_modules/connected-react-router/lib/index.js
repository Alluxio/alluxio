"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
Object.defineProperty(exports, "LOCATION_CHANGE", {
  enumerable: true,
  get: function get() {
    return _actions.LOCATION_CHANGE;
  }
});
Object.defineProperty(exports, "CALL_HISTORY_METHOD", {
  enumerable: true,
  get: function get() {
    return _actions.CALL_HISTORY_METHOD;
  }
});
Object.defineProperty(exports, "onLocationChanged", {
  enumerable: true,
  get: function get() {
    return _actions.onLocationChanged;
  }
});
Object.defineProperty(exports, "push", {
  enumerable: true,
  get: function get() {
    return _actions.push;
  }
});
Object.defineProperty(exports, "replace", {
  enumerable: true,
  get: function get() {
    return _actions.replace;
  }
});
Object.defineProperty(exports, "go", {
  enumerable: true,
  get: function get() {
    return _actions.go;
  }
});
Object.defineProperty(exports, "goBack", {
  enumerable: true,
  get: function get() {
    return _actions.goBack;
  }
});
Object.defineProperty(exports, "goForward", {
  enumerable: true,
  get: function get() {
    return _actions.goForward;
  }
});
Object.defineProperty(exports, "routerActions", {
  enumerable: true,
  get: function get() {
    return _actions.routerActions;
  }
});
Object.defineProperty(exports, "routerMiddleware", {
  enumerable: true,
  get: function get() {
    return _middleware.default;
  }
});
exports.createMatchSelector = exports.getSearch = exports.getHash = exports.getAction = exports.getLocation = exports.connectRouter = exports.ConnectedRouter = void 0;

var _ConnectedRouter = _interopRequireDefault(require("./ConnectedRouter"));

var _reducer = _interopRequireDefault(require("./reducer"));

var _selectors = _interopRequireDefault(require("./selectors"));

var _plain = _interopRequireDefault(require("./structure/plain"));

var _actions = require("./actions");

var _middleware = _interopRequireDefault(require("./middleware"));

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

var ConnectedRouter =
/*#__PURE__*/
(0, _ConnectedRouter.default)(_plain.default);
exports.ConnectedRouter = ConnectedRouter;
var connectRouter =
/*#__PURE__*/
(0, _reducer.default)(_plain.default);
exports.connectRouter = connectRouter;

var _createSelectors =
/*#__PURE__*/
(0, _selectors.default)(_plain.default),
    getLocation = _createSelectors.getLocation,
    getAction = _createSelectors.getAction,
    getHash = _createSelectors.getHash,
    getSearch = _createSelectors.getSearch,
    createMatchSelector = _createSelectors.createMatchSelector;

exports.createMatchSelector = createMatchSelector;
exports.getSearch = getSearch;
exports.getHash = getHash;
exports.getAction = getAction;
exports.getLocation = getLocation;