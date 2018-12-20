import React from 'react';
import {connect} from 'react-redux';
import {Dispatch} from 'redux';

import {IApplicationState} from '../../../store';

import './Logs.css';

// tslint:disable:no-empty-interface // TODO: remove this line

interface IPropsFromState {
}

interface IPropsFromDispatch {
}

type AllProps = IPropsFromState & IPropsFromDispatch;

class Logs extends React.Component<AllProps> {
  public render() {

    return (
      <div className="logs-page">
        <div className="container-fluid">
          <div className="row">
            <div className="col-12">
              Logs
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
)(Logs);
