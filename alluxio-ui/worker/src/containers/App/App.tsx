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
import {connect} from 'react-redux';
import {StaticContext} from 'react-router';
import {Redirect, Route, RouteComponentProps, Switch} from 'react-router-dom';
import {Dispatch} from 'redux';

import {Footer, Header} from '@alluxio/common-ui/src/components';
import {triggerRefresh} from '@alluxio/common-ui/src/store/refresh/actions';
import {BlockInfo, Logs, Metrics, Overview} from '..';
import {footerNavigationData, headerNavigationData} from '../../constants';

import './App.css';

interface IPropsFromDispatch {
  [key: string]: any;
}

interface IAppProps {
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
    const {history} = this.props;

    return (
      <ConnectedRouter history={history as any}>
        <div className="App pt-5 pb-4">
          <div className="container-fluid sticky-top header-wrapper">
            <Header history={history} data={headerNavigationData} autoRefreshCallback={this.setAutoRefresh}/>
          </div>
          <div className="pages container-fluid mt-3">
            <Switch>
              <Route exact={true} path="/" render={this.redirectToOverview}/>
              <Route path="/overview" exact={true} component={Overview}/>
              <Route path="/blockInfo" exact={true} component={BlockInfo}/>
              <Route path="/logs" exact={true} component={Logs}/>
              <Route path="/metrics" exact={true} component={Metrics}/>
              <Route render={this.redirectToOverview}/>
            </Switch>
          </div>
          <div className="container-fluid footer-wrapper">
            <Footer data={footerNavigationData}/>
          </div>
        </div>
      </ConnectedRouter>
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
      this.intervalHandle = setInterval(this.props.triggerRefresh, this.refreshInterval);
    } else {
      if (this.intervalHandle) {
        clearInterval(this.intervalHandle);
        this.intervalHandle = null;
      }
    }
  }
}

const mapStateToProps = () => ({});

const mapDispatchToProps = (dispatch: Dispatch) => ({
  triggerRefresh: () => dispatch(triggerRefresh())
});

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(App);
