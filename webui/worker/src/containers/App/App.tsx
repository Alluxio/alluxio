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

import { ConnectedRouter } from 'connected-react-router';
import { History, LocationState } from 'history';
import React from 'react';
import { connect } from 'react-redux';
import { Redirect, Route, Switch } from 'react-router-dom';
import { AnyAction, compose, Dispatch } from 'redux';

import {
  Footer,
  withErrors,
  withLoadingMessage,
  Header,
  withFetchData,
  SlackButton,
} from '@alluxio/common-ui/src/components';
import { triggerRefresh } from '@alluxio/common-ui/src/store/refresh/actions';
import { BlockInfo, WorkerLogs, Metrics, Overview } from '..';
import { footerNavigationData, headerNavigationData, routePaths } from '../../constants';
import { IApplicationState } from '../../store';
import { fetchRequest } from '../../store/init/actions';
import { IInit } from '../../store/init/types';

import './App.css';
import { AutoRefresh, createAlertErrors, IAutoRefresh } from '@alluxio/common-ui/src/utilities';
import { ICommonState } from '@alluxio/common-ui/src/constants';

interface IPropsFromState extends ICommonState {
  init: IInit;
}

interface IPropsFromDispatch {
  triggerRefresh: typeof triggerRefresh;
}

export interface IAppProps {
  history: History<LocationState>;
}

export type AllProps = IPropsFromState & IPropsFromDispatch & IAppProps;

export class App extends React.Component<AllProps> {
  private autoRefresh: IAutoRefresh;

  constructor(props: AllProps) {
    super(props);

    this.autoRefresh = new AutoRefresh(props.triggerRefresh, props.init.refreshInterval);
  }

  public render(): JSX.Element {
    const { init, history } = this.props;

    return (
      <ConnectedRouter history={history}>
        <div className="App h-100">
          <div className="w-100 sticky-top header-wrapper">
            <Header
              history={history}
              data={headerNavigationData}
              callbackParameters={{
                masterHostname: init.masterHostname,
                masterPort: init.masterPort,
              }}
              autoRefreshCallback={this.autoRefresh.setAutoRefresh}
            />
          </div>
          <div className="w-100 pt-5 mt-3 pb-4 mb-2">
            <Switch>
              <Route path={routePaths.root} exact={true} render={this.redirectToOverview} />
              <Route path={routePaths.overview} exact={true} render={this.renderView(Overview, undefined)} />
              <Route path={routePaths.blockInfo} exact={true} render={this.renderView(BlockInfo, undefined)} />
              <Route path={routePaths.logs} exact={true} render={this.renderView(WorkerLogs, { history })} />
              <Route path={routePaths.metrics} exact={true} render={this.renderView(Metrics, undefined)} />
              <Route render={this.redirectToOverview} />
            </Switch>
          </div>
          <div className="w-100 footer-wrapper">
            <Footer
              data={footerNavigationData}
              callbackParameters={{
                masterHostname: init.masterHostname,
                masterPort: init.masterPort,
              }}
            />
          </div>
          <SlackButton />
        </div>
      </ConnectedRouter>
    );
  }

  private renderView(Container: typeof React.Component, props?: {}): (routerProps: {}) => React.ReactNode {
    return (routerProps: {}): React.ReactNode => {
      return <Container {...routerProps} {...props} />;
    };
  }

  private redirectToOverview(): JSX.Element {
    return <Redirect to={routePaths.overview} />;
  }
}

const mapStateToProps = ({ init, refresh }: IApplicationState): IPropsFromState => ({
  init: init.data,
  errors: createAlertErrors(init.errors !== undefined),
  loading: init.loading,
  refresh: refresh.data,
  class: 'App',
});

const mapDispatchToProps = (
  dispatch: Dispatch,
): { fetchRequest: () => AnyAction; triggerRefresh: () => AnyAction } => ({
  fetchRequest: (): AnyAction => dispatch(fetchRequest()),
  triggerRefresh: (): AnyAction => dispatch(triggerRefresh()),
});

export default compose(
  connect(
    mapStateToProps,
    mapDispatchToProps,
  ),
  withFetchData,
  withErrors,
  withLoadingMessage,
)(App);
