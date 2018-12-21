import React from 'react';
import {connect} from 'react-redux';
import {Dispatch} from 'redux';

import {LoadingMessage} from '@alluxio/common-ui/src/components';
import {IApplicationState} from '../../../store';
import {fetchRequest} from '../../../store/metrics/actions';
import {IMetrics} from '../../../store/metrics/types';

import './Metrics.css';

interface IPropsFromState {
  errors: string;
  loading: boolean;
  metrics: IMetrics;
}

interface IPropsFromDispatch {
  fetchRequest: typeof fetchRequest;
}

type AllProps = IPropsFromState & IPropsFromDispatch;

class Metrics extends React.Component<AllProps> {
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

const mapStateToProps = ({metrics}: IApplicationState) => ({
  errors: metrics.errors,
  loading: metrics.loading,
  metrics: metrics.metrics
});

const mapDispatchToProps = (dispatch: Dispatch) => ({
  fetchRequest: () => dispatch(fetchRequest())
});

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(Metrics);
