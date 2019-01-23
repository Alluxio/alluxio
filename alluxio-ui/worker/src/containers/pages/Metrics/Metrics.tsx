/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

import {AxiosResponse} from 'axios';
import React from 'react';
import {connect} from 'react-redux';
import {Alert, Progress, Table} from 'reactstrap';
import {Dispatch} from 'redux';

import {IApplicationState} from '../../../store';
import {fetchRequest} from '../../../store/metrics/actions';
import {IMetrics} from '../../../store/metrics/types';

interface IPropsFromState {
  data: IMetrics;
  errors?: AxiosResponse;
  loading: boolean;
  refresh: boolean;
}

interface IPropsFromDispatch {
  fetchRequest: typeof fetchRequest;
}

export type AllProps = IPropsFromState & IPropsFromDispatch;

export class Metrics extends React.Component<AllProps> {
  public componentDidUpdate(prevProps: AllProps) {
    if (this.props.refresh !== prevProps.refresh) {
      this.props.fetchRequest();
    }
  }

  public componentWillMount() {
    this.props.fetchRequest();
  }

  public render() {
    const {errors, data} = this.props;

    if (errors) {
      return (
        <Alert color="danger">
          Unable to reach the api endpoint for this page.
        </Alert>
      );
    }

    return (
      <div className="metrics-page">
        <div className="container-fluid">
          <div className="row">
            <div className="col-12">
              <h5>Worker Gauges</h5>
              <Table hover={true}>
                <tbody>
                <tr>
                  <th scope="row">Worker Capacity</th>
                  <td>
                    <Progress multi={true}>
                      <Progress bar={true} color="success"
                                value={`${data.workerCapacityFreePercentage}`}>{data.workerCapacityFreePercentage}%
                        Free</Progress>
                      <Progress bar={true} color="danger"
                                value={`${data.workerCapacityUsedPercentage}`}>{data.workerCapacityUsedPercentage}%
                        Used</Progress>
                    </Progress>
                  </td>
                </tr>
                </tbody>
              </Table>
            </div>
            <div className="col-12">
              <h5>Logical Operations</h5>
              <Table hover={true}>
                <tbody>
                <tr>
                  <th>Blocks Accessed</th>
                  <td>{data.operationMetrics && data.operationMetrics.BlocksAccessed ? data.operationMetrics.BlocksAccessed.count : 0}</td>
                  <th>Blocks Cached</th>
                  <td>{data.operationMetrics && data.operationMetrics.BlocksCached ? data.operationMetrics.BlocksCached.count : 0}</td>
                </tr>
                <tr>
                  <th>Blocks Canceled</th>
                  <td>{data.operationMetrics && data.operationMetrics.BlocksCanceled ? data.operationMetrics.BlocksCanceled.count : 0}</td>
                  <th>Blocks Deleted</th>
                  <td>{data.operationMetrics && data.operationMetrics.BlocksDeleted ? data.operationMetrics.BlocksDeleted.count : 0}</td>
                </tr>
                <tr>
                  <th>Blocks Evicted</th>
                  <td>{data.operationMetrics && data.operationMetrics.BlocksEvicted ? data.operationMetrics.BlocksEvicted.count : 0}</td>
                  <th>Blocks Promoted</th>
                  <td>{data.operationMetrics && data.operationMetrics.BlocksPromoted ? data.operationMetrics.BlocksPromoted.count : 0}</td>
                </tr>
                <tr>
                  <th>Bytes Read Remotely</th>
                  <td>{data.operationMetrics && data.operationMetrics.BytesReadRemote ? data.operationMetrics.BytesReadRemote.count : 0}</td>
                  <th>Bytes Written Remotely</th>
                  <td>{data.operationMetrics && data.operationMetrics.BytesWrittenRemote ? data.operationMetrics.BytesWrittenRemote.count : 0}</td>
                </tr>
                </tbody>
              </Table>
            </div>
          </div>
        </div>
      </div>
    );
  }
}

const mapStateToProps = ({metrics, refresh}: IApplicationState) => ({
  data: metrics.data,
  errors: metrics.errors,
  loading: metrics.loading,
  refresh: refresh.data
});

const mapDispatchToProps = (dispatch: Dispatch) => ({
  fetchRequest: () => dispatch(fetchRequest())
});

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(Metrics);
