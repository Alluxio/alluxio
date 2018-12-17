import React from 'react';
import {connect} from 'react-redux';
import {Dispatch} from 'redux';

import {IApplicationState, IConnectedReduxProps} from '../../../store';

import './Configuration.css';

// tslint:disable:no-empty-interface // TODO: remove this line

interface IPropsFromState {
}

interface IPropsFromDispatch {
}

type AllProps = IPropsFromState & IPropsFromDispatch & IConnectedReduxProps;

class Configuration extends React.Component<AllProps> {
  public render() {

    return (
      <div className="configuration-page">
        <div className="container-fluid">
          <div className="row">
            <div className="col-12">
              Configuration
            </div>
          </div>
        </div>
      </div>
    );
  }
}

const mapStateToProps = ({}: IApplicationState) => ({
});

const mapDispatchToProps = (dispatch: Dispatch) => ({
});

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(Configuration);
