import {ConnectedRouter} from 'connected-react-router';
import React from 'react';
import {connect, Provider} from 'react-redux';
import {StaticContext} from 'react-router';
import {Redirect, Route, RouteComponentProps, Switch} from 'react-router-dom';
import {Dispatch, Store} from 'redux';

import {Footer, Header} from '@alluxio/common-ui/src/components';
import {
  Browse, Configuration, Data, Logs, Overview, Workers
} from '..';
import {footerNavigationData, headerNavigationData} from '../../constants';
import {IApplicationState, IConnectedReduxProps} from '../../store';

import './App.css';

// tslint:disable:no-empty-interface // TODO: remove this line

interface IPropsFromState {
}

interface IPropsFromDispatch {
  [key: string]: any;
}

interface IAppProps {
  store: Store<IApplicationState>;
  history: History;
}

interface IAppState {
  isAtTop: boolean;
}

type AllProps = IPropsFromState & IPropsFromDispatch & IAppProps & IConnectedReduxProps;

class App extends React.Component<AllProps, IAppState> {
  constructor(props: AllProps) {
    super(props);

    this.renderError = this.renderError.bind(this);
    this.redirectToOverview = this.redirectToOverview.bind(this);
  }

  public render() {
    const {store, history} = this.props;

    return (
      <Provider store={store}>
        <ConnectedRouter history={history as any}>
          <div className="App">
            <div className="container-fluid sticky-top header-wrapper">
              <Header history={history} data={headerNavigationData}/>
            </div>
            <div className="pages container-fluid mt-3 mb-3">
              <Switch>
                <Route exact={true} path="/" render={this.redirectToOverview}/>
                <Route path="/overview" exact={true} component={Overview}/>
                <Route path="/browse" exact={true} component={Browse}/>
                <Route path="/config" exact={true} component={Configuration}/>
                <Route path="/data" exact={true} component={Data}/>
                <Route path="/logs" exact={true} component={Logs}/>
                <Route path="/workers" exact={true} component={Workers}/>
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

  private renderError(routerProps: RouteComponentProps<any, StaticContext, any>) {
    return null;
  }
}

const mapStateToProps = () => ({});

const mapDispatchToProps = (dispatch: Dispatch) => ({});

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(App);
