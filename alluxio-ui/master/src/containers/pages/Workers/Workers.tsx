import React from 'react';
import {connect} from 'react-redux';
import {Dispatch} from 'redux';

import {IApplicationState, IConnectedReduxProps} from '../../../store';

import './Workers.css';

// tslint:disable:no-empty-interface // TODO: remove this line

interface IPropsFromState {
}

interface IPropsFromDispatch {
}

type AllProps = IPropsFromState & IPropsFromDispatch & IConnectedReduxProps;

class Workers extends React.Component<AllProps> {
  public render() {

    return (
      <div className="workers-page">
        <div className="conteiner-fluid">
          <div className="row">
            <div className="col-12">
              Workers
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
)(Workers);
