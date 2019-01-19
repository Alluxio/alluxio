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
import {IInit} from '../../../store/init/types';

interface IPropsFromState {
  initData: IInit;
  initErrors?: AxiosResponse;
  initLoading?: boolean;
  refresh: boolean;
  workersData: IWorkers;
  workersErrors?: AxiosResponse;
  workersLoading: boolean;
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
    const {initData, initErrors, workersErrors, workersData} = this.props;

    if (initErrors || workersErrors) {
      return (
        <Alert color="danger">
          Unable to reach the api endpoint for this page.
        </Alert>
      );
    }

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
                  {initData.debug && (
                    <React.Fragment>
                      <th>[D]Worker Id</th>
                      <th>[D]Uptime</th>
                    </React.Fragment>
                  )}
                  <th>Last Heartbeat</th>
                  <th>State</th>
                  <th>Workers Capacity</th>
                  <th>Space Used</th>
                  <th>Space Usage</th>
                </tr>
                </thead>
                <tbody>
                {workersData.normalNodeInfos.map((nodeInfo: INodeInfo) => (
                  <tr key={nodeInfo.workerId}>
                    <td><a href={`//${nodeInfo.host}:${initData.workerPort}`} target="_blank">{nodeInfo.host}</a></td>
                    {initData.debug && (
                      <React.Fragment>
                        <td>{nodeInfo.workerId}</td>
                        <td>{nodeInfo.uptimeClockTime}</td>
                      </React.Fragment>
                    )}
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
                  {initData.debug && (
                    <React.Fragment>
                      <th>[D]Worker Id</th>
                      <th>[D]Uptime</th>
                    </React.Fragment>
                  )}
                  <th>Last Heartbeat</th>
                  <th>Workers Capacity</th>
                </tr>
                </thead>
                <tbody>
                {workersData.failedNodeInfos.map((nodeInfo: INodeInfo) => (
                  <tr key={nodeInfo.workerId}>
                    <td>{nodeInfo.host}</td>
                    {initData.debug && (
                      <React.Fragment>
                        <td>{nodeInfo.workerId}</td>
                        <td>{nodeInfo.uptimeClockTime}</td>
                      </React.Fragment>
                    )}
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

const mapStateToProps = ({init, refresh, workers}: IApplicationState) => ({
  initData: init.data,
  initErrors: init.errors,
  initLoading: init.loading,
  refresh: refresh.data,
  workersData: workers.data,
  workersErrors: workers.errors,
  workersLoading: workers.loading
});

const mapDispatchToProps = (dispatch: Dispatch) => ({
  fetchRequest: () => dispatch(fetchRequest())
});

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(Workers);
