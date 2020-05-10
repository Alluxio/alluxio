/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

import 'babel-polyfill';
import 'raf/polyfill';

import { Action, createBrowserHistory, Location } from 'history';
import React from 'react';
import ReactDOM from 'react-dom';
import { Provider } from 'react-redux';

import configureStore from './configureStore';
import { App } from './containers';
import { initialState } from './store';

import 'source-sans-pro/source-sans-pro.css';
import 'source-serif-pro/source-serif-pro.css';
import '@openfonts/anonymous-pro_all';

import './index.css';
import { IAppProps } from './containers/App/App';

const history = createBrowserHistory();
history.listen((loc: Location, action: Action) => {
  // Don't scroll to top if user presses back
  // - if (loc.action === 'POP' || loc.action === 'REPLACE') is an option
  if (action === 'POP') {
    return;
  }

  // Allow the client to control scroll-to-top using location.state
  const l = (loc as unknown) as { state: { scroll: boolean } };
  if (l.state && l.state.scroll !== undefined && !l.state.scroll) {
    return;
  }

  // 200ms delay hack (for Firefox?)
  setTimeout(() => {
    window.scrollTo(0, 0);
  }, 200);
});

const store = configureStore(history, initialState);

ReactDOM.render(
  <Provider store={store}>{React.createElement(App as React.ComponentType<IAppProps>, { history: history })}</Provider>,
  document.getElementById('root') as HTMLElement,
);
