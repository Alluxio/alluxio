"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;

var _actions = require("./actions");

var createConnectRouter = function createConnectRouter(structure) {
  var fromJS = structure.fromJS,
      merge = structure.merge;

  var createRouterReducer = function createRouterReducer(history) {
    var initialRouterState = fromJS({
      location: history.location,
      action: history.action
    });
    /*
    * This reducer will update the state with the most recent location history
    * has transitioned to.
    */

    return function () {
      var state = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : initialRouterState;

      var _ref = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {},
          type = _ref.type,
          payload = _ref.payload;

      if (type === _actions.LOCATION_CHANGE) {
        var location = payload.location,
            action = payload.action,
            isFirstRendering = payload.isFirstRendering; // Don't update the state ref for the first rendering
        // to prevent the double-rendering issue on initilization

        return isFirstRendering ? state : merge(state, {
          location: fromJS(location),
          action: action
        });
      }

      return state;
    };
  };

  return createRouterReducer;
};

var _default = createConnectRouter;
exports.default = _default;