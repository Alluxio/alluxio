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

import {ConnectedRouter} from 'connected-react-router';
import React from 'react';
import {connect, Provider} from 'react-redux';
import {StaticContext} from 'react-router';
import {Redirect, Route, RouteComponentProps, Switch} from 'react-router-dom';
import {Dispatch, Store} from 'redux';

import {Footer, Header} from '@alluxio/common-ui/src/components';
import {getRoutedViewRenderer} from '@alluxio/common-ui/src/utilities';
import {
  Browse, Configuration, Data, Logs, Metrics, Overview, Workers
} from '..';
import {footerNavigationData, headerNavigationData} from '../../constants';
import {IApplicationState} from '../../store';

import './App.css';

interface IPropsFromDispatch {
  [key: string]: any;
}

interface IAppProps {
  store: Store<IApplicationState>;
  history: History;
}

interface IAppState {
  refreshValue: boolean;
}

type AllProps = IPropsFromDispatch & IAppProps;

class App extends React.Component<AllProps, IAppState> {
  private readonly refreshInterval = 30000;
  private intervalHandle: any;

  constructor(props: AllProps) {
    super(props);

    this.flipRefreshValue = this.flipRefreshValue.bind(this);
    this.setAutoRefresh = this.setAutoRefresh.bind(this);

    this.state = {
      refreshValue: false
    };
  }

  public render() {
    const {store, history} = this.props;
    const {refreshValue} = this.state;

    return (
      <Provider store={store}>
        <ConnectedRouter history={history as any}>
          <div className="App pt-5 pb-4">
            <div className="container-fluid sticky-top header-wrapper">
              <Header history={history} data={headerNavigationData} autoRefreshCallback={this.setAutoRefresh}/>
            </div>
            <div className="pages container-fluid mt-3">
              <Switch>
                <Route exact={true} path="/" render={this.redirectToOverview}/>
                <Route path="/overview" exact={true} render={getRoutedViewRenderer(Overview, {refreshValue})}/>
                <Route path="/browse" exact={true} render={getRoutedViewRenderer(Browse, {refreshValue})}/>
                <Route path="/config" exact={true} render={getRoutedViewRenderer(Configuration, {refreshValue})}/>
                <Route path="/data" exact={true} render={getRoutedViewRenderer(Data, {refreshValue})}/>
                <Route path="/logs" exact={true} render={getRoutedViewRenderer(Logs, {refreshValue})}/>
                <Route path="/metrics" exact={true} render={getRoutedViewRenderer(Metrics, {refreshValue})}/>
                <Route path="/workers" exact={true} render={getRoutedViewRenderer(Workers, {refreshValue})}/>
                <Route render={this.redirectToOverview}/>
              </Switch>
            </div>
            <div className="container-fluid footer-wrapper">
              <Footer data={footerNavigationData}/>
            </div>
          </div>
        </ConnectedRouter>
      </Provider>
    );
  }

  private redirectToOverview(routerProps: RouteComponentProps<any, StaticContext, any>) {
    return (
      <Redirect to="/overview"/>
    );
  }

  private flipRefreshValue() {
    this.setState({refreshValue: !this.state.refreshValue});
  }

  private setAutoRefresh(shouldAutoRefresh: boolean) {
    if (shouldAutoRefresh && !this.intervalHandle) {
      this.intervalHandle = setInterval(this.flipRefreshValue, this.refreshInterval);
    } else {
      if (this.intervalHandle) {
        clearInterval(this.intervalHandle);
        this.intervalHandle = null;
      }
    }
  }
}

const mapStateToProps = () => ({});

const mapDispatchToProps = (dispatch: Dispatch) => ({});

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(App);
