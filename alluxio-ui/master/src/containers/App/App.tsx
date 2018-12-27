import {ConnectedRouter} from 'connected-react-router';
import React from 'react';
import {connect, Provider} from 'react-redux';
import {StaticContext} from 'react-router';
import {Redirect, Route, RouteComponentProps, Switch} from 'react-router-dom';
import {Dispatch, Store} from 'redux';

import {Footer, Header} from '@alluxio/common-ui/src/components';
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

    this.renderError = this.renderError.bind(this);
    this.redirectToOverview = this.redirectToOverview.bind(this);
    this.flipRefreshValue = this.flipRefreshValue.bind(this);
    this.setAutoRefresh = this.setAutoRefresh.bind(this);
    this.renderOverview = this.renderOverview.bind(this);
    this.renderBrowse = this.renderBrowse.bind(this);
    this.renderConfiguration = this.renderConfiguration.bind(this);
    this.renderData = this.renderData.bind(this);
    this.renderLogs = this.renderLogs.bind(this);
    this.renderMetrics = this.renderMetrics.bind(this);
    this.renderWorkers = this.renderWorkers.bind(this);

    this.state = {
      refreshValue: false
    };
  }

  public render() {
    const {store, history} = this.props;

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
                <Route path="/overview" exact={true} render={this.renderOverview}/>
                <Route path="/browse" exact={true} render={this.renderBrowse}/>
                <Route path="/config" exact={true} component={this.renderConfiguration}/>
                <Route path="/data" exact={true} component={this.renderData}/>
                <Route path="/logs" exact={true} component={this.renderLogs}/>
                <Route path="/metrics" exact={true} component={this.renderMetrics}/>
                <Route path="/workers" exact={true} component={this.renderWorkers}/>
                <Route render={this.renderError}/>
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

  private renderOverview(routerProps: RouteComponentProps<any, StaticContext, any>) {
    return (
      <Overview {...routerProps} refreshValue={this.state.refreshValue}/>
    );
  }

  private renderBrowse(routerProps: RouteComponentProps<any, StaticContext, any>) {
    return (
      <Browse {...routerProps} refreshValue={this.state.refreshValue}/>
    );
  }

  private renderConfiguration(routerProps: RouteComponentProps<any, StaticContext, any>) {
    return (
      <Configuration {...routerProps} refreshValue={this.state.refreshValue}/>
    );
  }

  private renderData(routerProps: RouteComponentProps<any, StaticContext, any>) {
    return (
      <Data {...routerProps} refreshValue={this.state.refreshValue}/>
    );
  }

  private renderLogs(routerProps: RouteComponentProps<any, StaticContext, any>) {
    return (
      <Logs {...routerProps} refreshValue={this.state.refreshValue}/>
    );
  }

  private renderMetrics(routerProps: RouteComponentProps<any, StaticContext, any>) {
    return (
      <Metrics {...routerProps} refreshValue={this.state.refreshValue}/>
    );
  }

  private renderWorkers(routerProps: RouteComponentProps<any, StaticContext, any>) {
    return (
      <Workers {...routerProps} refreshValue={this.state.refreshValue}/>
    );
  }

  private renderError(routerProps: RouteComponentProps<any, StaticContext, any>) {
    return null;
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
