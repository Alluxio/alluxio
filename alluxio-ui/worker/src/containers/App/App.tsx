import {ConnectedRouter} from 'connected-react-router';
import React from 'react';
import {connect, Provider} from 'react-redux';
import {StaticContext} from 'react-router';
import {Redirect, Route, RouteComponentProps, Switch} from 'react-router-dom';
import {Dispatch, Store} from 'redux';

import {Footer, Header} from '@alluxio/common-ui/src/components';
import {getRoutedViewRenderer} from '@alluxio/common-ui/src/utilities';
import {BlockInfo, Logs, Metrics, Overview} from '..';
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
                <Route path="/blockInfo" exact={true} render={getRoutedViewRenderer(BlockInfo, {refreshValue})}/>
                <Route path="/logs" exact={true} render={getRoutedViewRenderer(Logs, {refreshValue})}/>
                <Route path="/metrics" exact={true} render={getRoutedViewRenderer(Metrics, {refreshValue})}/>
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
