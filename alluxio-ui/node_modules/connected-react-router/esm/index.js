import createConnectedRouter from "./ConnectedRouter";
import createConnectRouter from "./reducer";
import createSelectors from "./selectors";
import plainStructure from "./structure/plain";
export { LOCATION_CHANGE, CALL_HISTORY_METHOD, onLocationChanged, push, replace, go, goBack, goForward, routerActions } from "./actions";
export { default as routerMiddleware } from "./middleware";
export var ConnectedRouter =
/*#__PURE__*/
createConnectedRouter(plainStructure);
export var connectRouter =
/*#__PURE__*/
createConnectRouter(plainStructure);

var _createSelectors =
/*#__PURE__*/
createSelectors(plainStructure),
    getLocation = _createSelectors.getLocation,
    getAction = _createSelectors.getAction,
    getHash = _createSelectors.getHash,
    getSearch = _createSelectors.getSearch,
    createMatchSelector = _createSelectors.createMatchSelector;

export { getLocation, getAction, getHash, getSearch, createMatchSelector };