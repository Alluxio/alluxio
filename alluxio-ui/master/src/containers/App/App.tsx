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

import {AxiosResponse} from 'axios';
import {ConnectedRouter} from 'connected-react-router';
import {History, LocationState} from 'history';
import React from 'react';
import {connect} from 'react-redux';
import {StaticContext} from 'react-router';
import {Redirect, Route, RouteComponentProps, Switch} from 'react-router-dom';
import {Alert} from 'reactstrap';
import {Dispatch} from 'redux';

import {Footer, Header, LoadingMessage} from '@alluxio/common-ui/src/components';
import {triggerRefresh} from '@alluxio/common-ui/src/store/refresh/actions';
import {
  Browse, Configuration, Data, Logs, Metrics, Overview, Workers
} from '..';
import {footerNavigationData, headerNavigationData} from '../../constants';
import {IApplicationState} from '../../store';
import {fetchRequest} from '../../store/init/actions';
import {IInit} from '../../store/init/types';

import './App.css';

interface IPropsFromState {
  init: IInit;
  errors?: AxiosResponse;
  loading: boolean;
  refresh: boolean;
}

interface IPropsFromDispatch {
  fetchRequest: typeof fetchRequest;
  triggerRefresh: typeof triggerRefresh;
}

interface IAppProps {
  history: History<LocationState>;
}

export type AllProps = IPropsFromState & IPropsFromDispatch & IAppProps;

export class App extends React.Component<AllProps> {
  private intervalHandle: any;

  constructor(props: AllProps) {
    super(props);

    this.setAutoRefresh = this.setAutoRefresh.bind(this);
  }

  public componentDidUpdate(prevProps: AllProps) {
    if (this.props.refresh !== prevProps.refresh) {
      this.props.fetchRequest();
    }
  }

  public componentWillMount() {
    this.props.fetchRequest && this.props.fetchRequest();
  }

  public render() {
    const {errors, init, loading, history} = this.props;

    if (errors) {
      return (
        <Alert color="danger">
          Unable to reach the api endpoint for this page.
        </Alert>
      );
    }

    if (!init && loading) {
      return (
        <LoadingMessage/>
      );
    }

    return (
      <ConnectedRouter history={history}>
        <div className="App pt-5 pb-4">
          <div className="container-fluid sticky-top header-wrapper">
            <Header history={history} data={headerNavigationData} autoRefreshCallback={this.setAutoRefresh}/>
          </div>
          <div className="pages container-fluid mt-3">
            <Switch>
              <Route exact={true} path="/" render={this.redirectToOverview}/>
              <Route path="/overview" exact={true} component={Overview}/>
              <Route path="/browse" exact={true} component={Browse}/>
              <Route path="/config" exact={true} component={Configuration}/>
              <Route path="/data" exact={true} component={Data}/>
              <Route path="/logs" exact={true} component={Logs}/>
              <Route path="/metrics" exact={true} component={Metrics}/>
              <Route path="/workers" exact={true} component={Workers}/>
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

  private setAutoRefresh(shouldAutoRefresh: boolean) {
    const {init} = this.props;
    if (shouldAutoRefresh && !this.intervalHandle) {
      this.intervalHandle = setInterval(this.props.triggerRefresh, init.refreshInterval);
    } else {
      if (this.intervalHandle) {
        clearInterval(this.intervalHandle);
        this.intervalHandle = null;
      }
    }
  }
}

const mapStateToProps = ({init, refresh}: IApplicationState) => ({
  init: init.data,
  errors: init.errors,
  loading: init.loading,
  refresh: refresh.data
});

const mapDispatchToProps = (dispatch: Dispatch) => ({
  fetchRequest: () => dispatch(fetchRequest()),
  triggerRefresh: () => dispatch(triggerRefresh())
});

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(App);
