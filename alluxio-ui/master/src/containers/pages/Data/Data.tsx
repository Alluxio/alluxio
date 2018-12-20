import React from 'react';
import {connect} from 'react-redux';
import {Dispatch} from 'redux';

import {IApplicationState} from '../../../store';

import './Data.css';

// tslint:disable:no-empty-interface // TODO: remove this line

interface IPropsFromState {
}

interface IPropsFromDispatch {
}

type AllProps = IPropsFromState & IPropsFromDispatch;

class Data extends React.Component<AllProps> {
  public render() {

    return (
      <div className="data-page">
        <div className="container-fluid">
          <div className="row">
            <div className="col-12">
              Data
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
)(Data);
