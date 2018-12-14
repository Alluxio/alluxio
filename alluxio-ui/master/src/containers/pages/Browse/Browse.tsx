import React from 'react';
import {connect} from 'react-redux';
import {Dispatch} from 'redux';

import {IApplicationState, IConnectedReduxProps} from '../../../store';

import './Browse.css';

// tslint:disable:no-empty-interface // TODO: remove this line

interface IPropsFromState {
}

interface IPropsFromDispatch {
}

type AllProps = IPropsFromState & IPropsFromDispatch & IConnectedReduxProps;

class Browse extends React.Component<AllProps> {
  public render() {

    return (
      <div className="browse-page">
        <div className="conteiner-fluid">
          <div className="row">
            <div className="col-12">
              Browse
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
)(Browse);
