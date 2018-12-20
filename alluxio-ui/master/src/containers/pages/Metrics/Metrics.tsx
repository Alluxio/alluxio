import React from 'react';
import {connect} from 'react-redux';
import {Dispatch} from 'redux';

import {IApplicationState} from '../../../store';

import './Metrics.css';

// tslint:disable:no-empty-interface // TODO: remove this line

interface IPropsFromState {
}

interface IPropsFromDispatch {
}

type AllProps = IPropsFromState & IPropsFromDispatch;

class Metrics extends React.Component<AllProps> {
  public render() {

    return (
      <div className="metrics-page">
        <div className="container-fluid">
          <div className="row">
            <div className="col-12">
              Metrics
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
)(Metrics);
