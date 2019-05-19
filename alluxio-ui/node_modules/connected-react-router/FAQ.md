# Frequently Asked Questions
----------------------------
- [How to navigate with Redux action](#how-to-navigate-with-redux-action)
- [How to get the current browser location (URL)](#how-to-get-the-current-browser-location-url)
- [How to set Router props e.g. basename, initialEntries, etc.](#how-to-set-router-props-eg-basename-initialentries-etc)
- [How to hot reload functional components](#how-to-hot-reload-functional-components)
- [How to hot reload reducers](#how-to-hot-reload-reducers)
- [How to support Immutable.js](#how-to-support-immutablejs)
- [How to migrate from v4 to v5](#how-to-migrate-from-v4-to-v5)
- [How to use your own context with react-redux](#how-to-use-your-own-context-with-react-redux)

### How to navigate with Redux action
#### with store.dispatch
```js
import { push } from 'connected-react-router'

store.dispatch(push('/path/to/somewhere'))
```

#### with react-redux
```js
import { push } from 'connected-react-router'

// in component render:
<div onClick={() => {

  /** do something before redirection */
  props.push('/home');

}}>login</div>

// connect the action:
export default connect(null, { push })(Component);
```

#### in redux thunk
```js
import { push } from 'connected-react-router'

export const login = (username, password) => (dispatch) => {

  /* do something before redirection */

  dispatch(push('/home'))
}

```
#### in redux saga
```js
import { push } from 'connected-react-router'
import { put, call } from 'redux-saga/effects'

export function* login(username, password) {

  /* do something before redirection */

  yield put(push('/home'))
}
```

### How to get the current browser location (URL)
The current browser location can be accessed directly from the router state with `react-redux`'s `connect`.
The location object is comprised of pathname, search (query string), and hash.
```js
import { connect } from 'react-redux'

const Child = ({ pathname, search, hash }) => (
  <div>
    Child receives
    <div>
      pathname: {pathname}
    </div>
    <div>
      search: {search}
    </div>
    <div>
      hash: {hash}
    </div>
  </div>
)

const mapStateToProps = state => ({
  pathname: state.router.location.pathname,
  search: state.router.location.search,
  hash: state.router.location.hash,
})

export default connect(mapStateToProps)(Child)
```

### How to set Router props (e.g. basename, initialEntries, etc.)
You can pass props to the `create*History` functions of your choice (`createBrowserHistory`, `createHashHistory`, `createMemoryHistory`)

```js
import { createBrowserHistory } from 'history'

const history = createBrowserHistory({
  basename: '/prefix/',
})
```

```js
import { createHashHistory } from 'history'

const history = createHashHistory({
  hashType: 'slash',
  getUserConfirmation: (message, callback) => callback(window.confirm(message))
})
```

```js
import { createMemoryHistory } from 'history'

const history = createMemoryHistory({
  initialEntries: [ '/one', '/two', { pathname: '/three' } ],
  initialIndex: 1
})
```

### How to hot reload functional components
1) Save the main app component in its own file.

`App.js`
``` js
import React from 'react'
import { Route, Switch } from 'react-router' /* react-router v4 */
import { ConnectedRouter } from 'connected-react-router'

const App = ({ history }) => ( /* receive history object via props */
  <ConnectedRouter history={history}>
    <div>
      <Switch>
        <Route exact path="/" render={() => (<div>Match</div>)} />
        <Route render={() => (<div>Miss</div>)} />
      </Switch>
    </div>
  </ConnectedRouter>
)

export default App
```

2) Wrap the `App` component with `AppContainer` from `react-hot-loader` v3 as a top-level container.

`index.js`
```js
import React from 'react'
import ReactDOM from 'react-dom'
import { Provider } from 'react-redux'
import { AppContainer } from 'react-hot-loader' /* react-hot-loader v3 */
import App from './App'
...
const render = () => { // this function will be reused
  ReactDOM.render(
    <AppContainer> { /* AppContainer for hot reloading v3 */ }
      <Provider store={store}>
        <App history={history} /> { /* pass history object as props */ }
      </Provider>
    </AppContainer>,
    document.getElementById('react-root')
  )
}

render()
```

3) Detect change and re-render with hot reload.

`index.js`
``` js
...
if (module.hot) {
  module.hot.accept('./App', () => {
    /* For Webpack 2.x
       Need to disable babel ES2015 modules transformation in .babelrc
       presets: [
         ["es2015", { "modules": false }]
       ]
    */
    render()

    /* For Webpack 1.x
    const NextApp = require('./App').default
    renderWithHotReload(NextApp)
    */
  })
}
```
Now, when you change any component that `App` depends on, it will trigger hot reloading without losing redux state. Thanks [react-hot-loader v3](https://github.com/gaearon/react-hot-loader/tree/next)!

### How to hot reload reducers
Detect change and replace with a new root reducer with router state

`index.js`
``` js
...
if (module.hot) {
  module.hot.accept('./reducers', () => {
    /* For Webpack 2.x
       Need to disable babel ES2015 modules transformation in .babelrc
       presets: [
         ["es2015", { "modules": false }]
       ]
    */
    store.replaceReducer(rootReducer(history))

    /* For Webpack 1.x
    const nextRootReducer = require('./reducers').default
    store.replaceReducer(nextRootReducer(history))
    */
  })
}
```

### How to support Immutable.js
1) Create your root reducer as a function that takes `history` and returns reducer. Use `combineReducers` from `redux-immutable` to return the root reducer.

2) Import `connectRouter` from `connected-react-router/immutable` and add router reducer to root reducer
```js
import { combineReducers } from 'redux-immutable'
import { connectRouter } from 'connected-react-router/immutable'
...
const rootReducer = (history) => combineReducers({
  router: connectRouter(history),
  ...
})
...
```

2) Import `ConnectedRouter` and `routerMiddleware` from `connected-react-router/immutable` instead of `connected-react-router`.
```js
import { ConnectedRouter, routerMiddleware } from 'connected-react-router/immutable'
```

3) Create your root reducer with router reducer by passing `history` to `rootReducer` function
```js
const store = createStore(
  rootReducer(history),
  initialState,
  ...
)
```

4) (Optional) Initialize state with `Immutable.Map()`
```js
import Immutable from 'immutable'
...
const initialState = Immutable.Map()
...
const store = createStore(
  rootReducer(history),
  initialState,
  ...
)
```

### How to migrate from v4 to v5
It's easy to migrate from v4 to v5.
1. In your root reducer file, instead of exporting a root reducer, you need to export a function accepting a `history` object and returning a root reducer with `router` key. The value of the `router` key is `connectedRouter(history)`.

```diff
  // reducers.js

  import { combineReducers } from 'redux'
+ import { connectRouter } from 'connected-react-router'

- export default combineReducers({
+ export default (history) => combineReducers({
+   router: connectRouter(history),
    ...
  })
```

2. In `createStore` function, change to use the new function creating a root reducer.
```diff
  // configureStore.js
  ...
  import { createBrowserHistory } from 'history'
  import { applyMiddleware, compose, createStore } from 'redux'
- import { connectRouter, routerMiddleware } from 'connected-react-router'
+ import { routerMiddleware } from 'connected-react-router'
- import rootReducer from './reducers'
+ import createRootReducer from './reducers'

  const history = createBrowserHistory()

  const store = createStore(
-   connectRouter(history)(rootReducer),
+   createRootReducer(history),
    initialState,
    compose(
      applyMiddleware(
        routerMiddleware(history),
      ),
    ),
  )
```

3. For reducers hot reloading, similarly, change to use the new function creating a root reducer.
```diff
  // For Webpack 2.x
- store.replaceReducer(connectRouter(history)(rootReducer))
+ store.replaceReducer(createRootReducer(history))

  // For Webpack 1.x
- const nextRootReducer = require('./reducers').default
- store.replaceReducer(connectRouter(history)(nextRootReducer))
+ const nextCreateRootReducer = require('./reducers').default
+ store.replaceReducer(nextCreateRootReducer(history))
```

### How to Use Your Own Context with react-redux
With react-redux v6.0.0, you can pass your own context to `<Provider>` component. So, you need to pass the same context as props to `<ConnectedRouter>` component.
```js
const customContext = React.createContext(null) // your own context

ReactDOM.render(
  <Provider store={store} context={customContext}>
    <ConnectedRouter history={history} context={customContext}>
      ...
    </ConnectedRouter>
  </Provider>
)
```
