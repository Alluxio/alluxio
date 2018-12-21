import 'babel-polyfill';
import 'raf/polyfill';

import {Action, createBrowserHistory, Location} from 'history';
import React from 'react';
import ReactDOM from 'react-dom';
import {Helmet} from 'react-helmet';

import configureStore from './configureStore';
import {App} from './containers';

// Order matters for the following files, so disable alphabetization
// tslint:disable:ordered-imports
import 'source-sans-pro/source-sans-pro.css';
import 'bootstrap/dist/css/bootstrap.min.css';
import 'bootstrap/dist/css/bootstrap-grid.min.css';
import 'bootstrap/dist/css/bootstrap-reboot.min.css';
import './index.css';
// tslint:enable:ordered-imports

const history = createBrowserHistory();
history.listen((loc: Location, action: Action) => {
  // Don't scroll to top if user presses back
  // - if (loc.action === 'POP' || loc.action === 'REPLACE') is an option
  if (action === 'POP') {
    return;
  }

  // Allow the client to control scroll-to-top using location.state
  if (loc.state && loc.state.scroll !== undefined && !loc.state.scroll) {
    return;
  }

  // 200ms delay hack (for Firefox?)
  setTimeout(() => {
    window.scrollTo(0, 0);
  }, 200);
});

const initialState = window.initialReduxState;
const store = configureStore(history, initialState);

ReactDOM.render(
  <React.Fragment>
    <Helmet>
      {/*IE*/}
      <meta http-equiv="X-UA-Compatible" content="IE=edge"/>
      <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
      <title>Alluxio Master</title>
    </Helmet>
    <App store={store} history={history}/>
  </React.Fragment>,
  document.getElementById('root') as HTMLElement
);
