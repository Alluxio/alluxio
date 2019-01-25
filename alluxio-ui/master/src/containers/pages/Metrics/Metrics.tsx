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
              <h5>Master Gauges</h5>
              <Table hover={true}>
                <tbody>
                <tr>
                  <th scope="row">Master Capacity</th>
                  <td>
                    <Progress multi={true}>
                      <Progress bar={true} color="success"
                                value={`${data.masterCapacityFreePercentage}`}>{data.masterCapacityFreePercentage}%
                        Free</Progress>
                      <Progress bar={true} color="danger"
                                value={`${data.masterCapacityUsedPercentage}`}>{data.masterCapacityUsedPercentage}%
                        Used</Progress>
                    </Progress>
                  </td>
                </tr>
                <tr>
                  <th scope="row">Master UnderFS Capacity</th>
                  <td>
                    <Progress multi={true}>
                      <Progress bar={true} color="success"
                                value={`${data.masterUnderfsCapacityFreePercentage}`}>{data.masterUnderfsCapacityFreePercentage}%
                        Free</Progress>
                      <Progress bar={true} color="danger"
                                value={`${data.masterUnderfsCapacityUsedPercentage}`}>{data.masterUnderfsCapacityUsedPercentage}%
                        Used</Progress>
                    </Progress>
                  </td>
                </tr>
                </tbody>
              </Table>
            </div>
            <div className="col-12">
              <h5>Total IO Size</h5>
              <Table hover={true}>
                <tbody>
                <tr>
                  <th>Short-circuit Read</th>
                  <td>{data.totalBytesReadLocal}</td>
                  <th>From Remote Instances</th>
                  <td>{data.totalBytesReadRemote}</td>
                </tr>
                <tr>
                  <th>Under Filesystem Read</th>
                  <td>{data.totalBytesReadUfs}</td>
                </tr>
                <tr>
                  <th>Alluxio Write</th>
                  <td>{data.totalBytesWrittenAlluxio}</td>
                  <th>Under Filesystem Write</th>
                  <td>{data.totalBytesWrittenUfs}</td>
                </tr>
                </tbody>
              </Table>
            </div>
            <div className="col-12">
              <h5>Total IO Throughput (Last Minute)</h5>
              <Table hover={true}>
                <tbody>
                <tr>
                  <th>Short-circuit Read</th>
                  <td>{data.totalBytesReadLocalThroughput}</td>
                  <th>From Remote Instances</th>
                  <td>{data.totalBytesReadRemoteThroughput}</td>
                </tr>
                <tr>
                  <th>Under Filesystem Read</th>
                  <td>{data.totalBytesReadUfsThroughput}</td>
                </tr>
                <tr>
                  <th>Alluxio Write</th>
                  <td>{data.totalBytesWrittenAlluxioThroughput}</td>
                  <th>Under Filesystem Write</th>
                  <td>{data.totalBytesWrittenUfsThroughput}</td>
                </tr>
                </tbody>
              </Table>
            </div>
            <div className="col-12">
              <h5>Cache Hit Rate (Percentage)</h5>
              <Table hover={true}>
                <tbody>
                <tr>
                  <th>Alluxio Local</th>
                  <td>{data.cacheHitLocal}</td>
                  <th>Alluxio Remote</th>
                  <td>{data.cacheHitRemote}</td>
                </tr>
                <tr>
                  <th>Miss</th>
                  <td>{data.cacheMiss}</td>
                </tr>
                </tbody>
              </Table>
            </div>
            <div className="col-12">
              <h5>Mounted Under FileSystem Read</h5>
              <Table hover={true}>
                <tbody>
                <tr>
                  <th>Under FileSystem</th>
                  <th>Size</th>
                </tr>
                {Object.keys(data.ufsReadSize).map((key: string) => (
                  <tr key={key}>
                    <td>{key}</td>
                    <td>{data.ufsReadSize[key]}</td>
                  </tr>
                ))}
                </tbody>
              </Table>
            </div>
            <div className="col-12">
              <h5>Mounted Under FileSystem Write</h5>
              <Table hover={true}>
                <tbody>
                <tr>
                  <th>Under FileSystem</th>
                  <th>Size</th>
                </tr>
                {Object.keys(data.ufsWriteSize).map((key: string) => (
                  <tr key={key}>
                    <td>{key}</td>
                    <td>{data.ufsWriteSize[key]}</td>
                  </tr>
                ))}
                </tbody>
              </Table>
            </div>
            <div className="col-12">
              <h5>Logical Operations</h5>
              <Table hover={true}>
                <tbody>
                <tr>
                  <th>Directories Created</th>
                  <td>{data.operationMetrics.DirectoriesCreated && data.operationMetrics.DirectoriesCreated.count}</td>
                  <th>File Block Infos Got</th>
                  <td>{data.operationMetrics.FileBlockInfosGot && data.operationMetrics.FileBlockInfosGot.count}</td>
                </tr>
                <tr>
                  <th>File Infos Got</th>
                  <td>{data.operationMetrics.FileInfosGot && data.operationMetrics.FileInfosGot.count}</td>
                  <th>Files Completed</th>
                  <td>{data.operationMetrics.FilesCompleted && data.operationMetrics.FilesCompleted.count}</td>
                </tr>
                <tr>
                  <th>Files Created</th>
                  <td>{data.operationMetrics.FilesCreated && data.operationMetrics.FilesCreated.count}</td>
                  <th>Files Freed</th>
                  <td>{data.operationMetrics.FilesFreed && data.operationMetrics.FilesFreed.count}</td>
                </tr>
                <tr>
                  <th>Files Persisted</th>
                  <td>{data.operationMetrics.FilesPersisted && data.operationMetrics.FilesPersisted.count}</td>
                  <th>Files Pinned</th>
                  <td>{data.operationMetrics.FilesPinned && data.operationMetrics.FilesPinned.value}</td>
                </tr>
                <tr>
                  <th>New Blocks Got</th>
                  <td>{data.operationMetrics.NewBlocksGot && data.operationMetrics.NewBlocksGot.count}</td>
                  <th>Paths Deleted</th>
                  <td>{data.operationMetrics.PathsDeleted && data.operationMetrics.PathsDeleted.count}</td>
                </tr>
                <tr>
                  <th>Paths Mounted</th>
                  <td>{data.operationMetrics.PathsMounted && data.operationMetrics.PathsMounted.count}</td>
                  <th>Paths Renamed</th>
                  <td>{data.operationMetrics.PathsRenamed && data.operationMetrics.PathsRenamed.count}</td>
                </tr>
                <tr>
                  <th>Paths Unmounted</th>
                  <td>{data.operationMetrics.PathsUnmounted && data.operationMetrics.PathsUnmounted.count}</td>
                  <th/>
                  <td/>
                </tr>
                </tbody>
              </Table>
            </div>
            <div className="col-12">
              <h5>RPC Invocations</h5>
              <Table hover={true}>
                <tbody>
                <tr>
                  <th>CompleteFile Operations</th>
                  <td>{data.rpcInvocationMetrics.CompleteFileOps && data.rpcInvocationMetrics.CompleteFileOps.count}</td>
                  <th>CreateDirectory Operations</th>
                  <td>{data.rpcInvocationMetrics.CreateDirectoryOps && data.rpcInvocationMetrics.CreateDirectoryOps.count}</td>
                </tr>
                <tr>
                  <th>CreateFile Operations</th>
                  <td>{data.rpcInvocationMetrics.CreateFileOps && data.rpcInvocationMetrics.CreateFileOps.count}</td>
                  <th>DeletePath Operations</th>
                  <td>{data.rpcInvocationMetrics.DeletePathOps && data.rpcInvocationMetrics.DeletePathOps.count}</td>
                </tr>
                <tr>
                  <th>FreeFile Operations</th>
                  <td>{data.rpcInvocationMetrics.FreeFileOps && data.rpcInvocationMetrics.FreeFileOps.count}</td>
                  <th>GetFileBlockInfo Operations</th>
                  <td>{data.rpcInvocationMetrics.GetFileBlockInfoOps && data.rpcInvocationMetrics.GetFileBlockInfoOps.count}</td>
                </tr>
                <tr>
                  <th>GetFileInfo Operations</th>
                  <td>{data.rpcInvocationMetrics.GetFileInfoOps && data.rpcInvocationMetrics.GetFileInfoOps.count}</td>
                  <th>GetNewBlock Operations</th>
                  <td>{data.rpcInvocationMetrics.GetNewBlockOps && data.rpcInvocationMetrics.GetNewBlockOps.count}</td>
                </tr>
                <tr>
                  <th>Mount Operations</th>
                  <td>{data.rpcInvocationMetrics.MountOps && data.rpcInvocationMetrics.MountOps.count}</td>
                  <th>RenamePath Operations</th>
                  <td>{data.rpcInvocationMetrics.RenamePathOps && data.rpcInvocationMetrics.RenamePathOps.count}</td>
                </tr>
                <tr>
                  <th>SetAcl Operations</th>
                  <td>{data.rpcInvocationMetrics.SetAclOps && data.rpcInvocationMetrics.SetAclOps.count}</td>
                  <th>SetAttribute Operations</th>
                  <td>{data.rpcInvocationMetrics.SetAttributeOps && data.rpcInvocationMetrics.SetAttributeOps.count}</td>
                </tr>
                <tr>
                  <th>Unmount Operations</th>
                  <td>{data.rpcInvocationMetrics.UnmountOps && data.rpcInvocationMetrics.UnmountOps.count}</td>
                </tr>
                </tbody>
              </Table>
            </div>
            {Object.keys(data.ufsOps).map((key: string) => (
              <div key={key} className="col-12">
                <h5>Under FileSystem Operations of {key}</h5>
                <Table hover={true}>
                  <tbody>
                  {Object.keys(data.ufsOps[key]).map((innerKey: string) => (
                    <tr key={innerKey}>
                      <th>{innerKey}</th>
                      <td>{data.ufsOps[key][innerKey]}</td>
                    </tr>
                  ))}
                  </tbody>
                </Table>
              </div>
            ))}
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
