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

import React from 'react';
import { connect } from 'react-redux';
import { Progress, Table } from 'reactstrap';
import { AnyAction, compose, Dispatch } from 'redux';

import { withErrors, withFluidContainer, withLoadingMessage, withFetchData } from '@alluxio/common-ui/src/components';
import { IApplicationState } from '../../../store';
import { fetchRequest } from '../../../store/metrics/actions';
import { IMetrics } from '../../../store/metrics/types';
import { createAlertErrors } from '@alluxio/common-ui/src/utilities';
import { ICommonState } from '@alluxio/common-ui/src/constants';

interface IPropsFromState extends ICommonState {
  data: IMetrics;
}

interface IPropsFromDispatch {
  fetchRequest: typeof fetchRequest;
}

export type AllProps = IPropsFromState & IPropsFromDispatch;

export class MetricsPresenter extends React.Component<AllProps> {
  public render(): JSX.Element {
    const { data } = this.props;

    return (
      <React.Fragment>
        <div className="col-12">
          <h5>Worker Gauges</h5>
          <Table hover={true}>
            <tbody>
              <tr>
                <th scope="row">Worker Capacity</th>
                <td>
                  <Progress className="h-50 mt-1" multi={true}>
                    <Progress bar={true} color="dark" value={`${data.workerCapacityFreePercentage}`}>
                      {data.workerCapacityFreePercentage}% Free
                    </Progress>
                    <Progress bar={true} color="secondary" value={`${data.workerCapacityUsedPercentage}`}>
                      {data.workerCapacityUsedPercentage}% Used
                    </Progress>
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
                <td>
                  {data.operationMetrics && data.operationMetrics.BlocksAccessed
                    ? data.operationMetrics.BlocksAccessed.count
                    : 0}
                </td>
                <th>Blocks Cached</th>
                <td>
                  {data.operationMetrics && data.operationMetrics.BlocksCached
                    ? data.operationMetrics.BlocksCached.count
                    : 0}
                </td>
              </tr>
              <tr>
                <th>Blocks Canceled</th>
                <td>
                  {data.operationMetrics && data.operationMetrics.BlocksCanceled
                    ? data.operationMetrics.BlocksCanceled.count
                    : 0}
                </td>
                <th>Blocks Deleted</th>
                <td>
                  {data.operationMetrics && data.operationMetrics.BlocksDeleted
                    ? data.operationMetrics.BlocksDeleted.count
                    : 0}
                </td>
              </tr>
              <tr>
                <th>Blocks Evicted</th>
                <td>
                  {data.operationMetrics && data.operationMetrics.BlocksEvicted
                    ? data.operationMetrics.BlocksEvicted.count
                    : 0}
                </td>
                <th>Blocks Promoted</th>
                <td>
                  {data.operationMetrics && data.operationMetrics.BlocksPromoted
                    ? data.operationMetrics.BlocksPromoted.count
                    : 0}
                </td>
              </tr>
              <tr>
                <th>Bytes Read Remotely</th>
                <td>
                  {data.operationMetrics && data.operationMetrics.BytesReadRemote
                    ? data.operationMetrics.BytesReadRemote.count
                    : 0}
                </td>
                <th>Bytes Written Remotely</th>
                <td>
                  {data.operationMetrics && data.operationMetrics.BytesWrittenRemote
                    ? data.operationMetrics.BytesWrittenRemote.count
                    : 0}
                </td>
              </tr>
            </tbody>
          </Table>
        </div>
      </React.Fragment>
    );
  }
}

const mapStateToProps = ({ metrics, refresh }: IApplicationState): IPropsFromState => ({
  data: metrics.data,
  errors: createAlertErrors(metrics.errors !== undefined),
  loading: metrics.loading,
  refresh: refresh.data,
  class: 'metrics-page',
});

const mapDispatchToProps = (dispatch: Dispatch): { fetchRequest: () => AnyAction } => ({
  fetchRequest: (): AnyAction => dispatch(fetchRequest()),
});

export default compose(
  connect(
    mapStateToProps,
    mapDispatchToProps,
  ),
  withFetchData,
  withErrors,
  withLoadingMessage,
  withFluidContainer,
)(MetricsPresenter) as typeof React.Component;
