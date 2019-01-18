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

import {INodeInfo} from '../../../constants';
import {IApplicationState} from '../../../store';
import {fetchRequest} from '../../../store/workers/actions';
import {IWorkers} from '../../../store/workers/types';

interface IPropsFromState {
  data: IWorkers;
  errors?: AxiosResponse;
  loading: boolean;
  refresh: boolean;
}

interface IPropsFromDispatch {
  fetchRequest: typeof fetchRequest;
}

export type AllProps = IPropsFromState & IPropsFromDispatch;

export class Workers extends React.Component<AllProps> {
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

    console.log(data);

    return (
      <div className="workers-page">
        <div className="container-fluid">
          <div className="row">
            <div className="col-12">
              <h5>Live Workers</h5>
              <Table hover={true}>
                <thead>
                <tr>
                  <th>Node Name</th>
                  <th>[D]Worker Id</th>
                  <th>[D]Uptime</th>
                  <th>Last Heartbeat</th>
                  <th>State</th>
                  <th>Workers Capacity</th>
                  <th>Space Used</th>
                  <th>Space Usage</th>
                </tr>
                </thead>
                <tbody>
                {data.normalNodeInfos.map((nodeInfo: INodeInfo) => (
                  <tr key={nodeInfo.workerId}>
                    <td><a href={`//${nodeInfo.host}:30000`} target="_blank">{nodeInfo.host}</a></td>
                    <td>{nodeInfo.workerId}</td>
                    <td>{nodeInfo.uptimeClockTime}</td>
                    <td>{nodeInfo.lastHeartbeat}</td>
                    <td>{nodeInfo.state}</td>
                    <td>{nodeInfo.capacity}</td>
                    <td>{nodeInfo.usedMemory}</td>
                    <td>
                      <Progress multi={true}>
                        <Progress bar={true} color="success"
                                  value={`${nodeInfo.freeSpacePercent}`}>{nodeInfo.freeSpacePercent}% Free</Progress>
                        <Progress bar={true} color="danger"
                                  value={`${nodeInfo.usedSpacePercent}`}>{nodeInfo.usedSpacePercent}% Used</Progress>
                      </Progress>
                    </td>
                  </tr>
                ))}
                </tbody>
              </Table>
            </div>
          </div>
          <div className="row">
            <div className="col-12">
              <h5>Alluxio Configuration</h5>
              <Table hover={true}>
                <thead>
                <tr>
                  <th>Node Name</th>
                  <th>[D]Worker Id</th>
                  <th>[D]Uptime</th>
                  <th>Last Heartbeat</th>
                  <th>Workers Capacity</th>
                </tr>
                </thead>
                <tbody>
                {data.failedNodeInfos.map((nodeInfo: INodeInfo) => (
                  <tr key={nodeInfo.workerId}>
                    <td>{nodeInfo.host}</td>
                    <td>{nodeInfo.workerId}</td>
                    <td>{nodeInfo.uptimeClockTime}</td>
                    <td>{nodeInfo.lastHeartbeat}</td>
                    <td>{nodeInfo.capacity}</td>
                  </tr>
                ))}
                </tbody>
              </Table>
            </div>
          </div>
        </div>
      </div>
    );
  }
}

const mapStateToProps = ({refresh, workers}: IApplicationState) => ({
  data: workers.data,
  errors: workers.errors,
  loading: workers.loading,
  refresh: refresh.refresh
});

const mapDispatchToProps = (dispatch: Dispatch) => ({
  fetchRequest: () => dispatch(fetchRequest())
});

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(Workers);
