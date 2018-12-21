import React from 'react';
import {connect} from 'react-redux';
import {Dispatch} from 'redux';

import {LoadingMessage} from '@alluxio/common-ui/src/components';
import {IApplicationState} from '../../../store';
import {fetchRequest} from '../../../store/overview/actions';
import {IOverview} from '../../../store/overview/types';

import './Overview.css';

interface IPropsFromState {
  errors: string;
  loading: boolean;
  overview: IOverview;
}

interface IPropsFromDispatch {
  fetchRequest: typeof fetchRequest;
}

type AllProps = IPropsFromState & IPropsFromDispatch;

class Overview extends React.Component<AllProps> {
  public componentWillMount() {
    this.props.fetchRequest();
  }

  public render() {
    const {loading} = this.props;

    if (loading) {
      return (
        <LoadingMessage/>
      );
    }

    return (
      <div className="overview-page">
        <div className="container-fluid">
          <div className="row">
            <div className="col-12">
              <h5>Storage Usage Summary</h5>
              Overview
            </div>
          </div>
        </div>
      </div>
    );
  }
}

const mapStateToProps = ({overview}: IApplicationState) => ({
  errors: overview.errors,
  loading: overview.loading,
  overview: overview.overview
});

const mapDispatchToProps = (dispatch: Dispatch) => ({
  fetchRequest: () => dispatch(fetchRequest())
});

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(Overview);
