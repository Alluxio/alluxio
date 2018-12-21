import React from 'react';
import {connect} from 'react-redux';
import {Progress, Table} from 'reactstrap';
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
    const {loading, metrics} = this.props;

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
              <h5>Master Gauges</h5>
              <Table hover={true}>
                <tbody>
                <tr>
                  <th scope="row">Master Capacity</th>
                  <td>
                    <Progress multi={true}>
                      <Progress bar={true} color="success"
                                value={`${metrics.masterCapacityFreePercentage}`}>{metrics.masterCapacityFreePercentage}%
                        Free</Progress>
                      <Progress bar={true} color="danger"
                                value={`${metrics.masterCapacityUsedPercentage}`}>{metrics.masterCapacityUsedPercentage}%
                        Used</Progress>
                    </Progress>
                  </td>
                </tr>
                <tr>
                  <th scope="row">Master UnderFS Capacity</th>
                  <td>
                    <Progress multi={true}>
                      <Progress bar={true} color="success"
                                value={`${metrics.masterUnderfsCapacityFreePercentage}`}>{metrics.masterUnderfsCapacityFreePercentage}%
                        Free</Progress>
                      <Progress bar={true} color="danger"
                                value={`${metrics.masterUnderfsCapacityUsedPercentage}`}>{metrics.masterUnderfsCapacityUsedPercentage}%
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
                  <td>{metrics.totalBytesReadLocalThroughput}</td>
                  <th>From Remote Instances</th>
                  <td>{metrics.totalBytesReadRemoteThroughput}</td>
                </tr>
                <tr>
                  <th>Under Filesystem Read</th>
                  <td>{metrics.totalBytesReadUfsThroughput}</td>
                </tr>
                <tr>
                  <th>Alluxio Write</th>
                  <td>{metrics.totalBytesWrittenAlluxioThroughput}</td>
                  <th>Under Filesystem Write</th>
                  <td>{metrics.totalBytesWrittenUfsThroughput}</td>
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
                  <td>{metrics.totalBytesReadLocalThroughput}</td>
                  <th>From Remote Instances</th>
                  <td>{metrics.totalBytesReadRemoteThroughput}</td>
                </tr>
                <tr>
                  <th>Under Filesystem Read</th>
                  <td>{metrics.totalBytesReadUfsThroughput}</td>
                </tr>
                <tr>
                  <th>Alluxio Write</th>
                  <td>{metrics.totalBytesWrittenAlluxioThroughput}</td>
                  <th>Under Filesystem Write</th>
                  <td>{metrics.totalBytesWrittenUfsThroughput}</td>
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
                  <td>{metrics.cacheHitLocal}</td>
                  <th>Alluxio Remote</th>
                  <td>{metrics.cacheHitRemote}</td>
                </tr>
                <tr>
                  <th>Miss</th>
                  <td>{metrics.cacheMiss}</td>
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
                {Object.keys(metrics.ufsReadSize).map((key: string) => (
                  <tr key={key}>
                    <td>{key}</td>
                    <td>{metrics.ufsReadSize[key]}</td>
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
                {Object.keys(metrics.ufsWriteSize).map((key: string) => (
                  <tr key={key}>
                    <td>{key}</td>
                    <td>{metrics.ufsWriteSize[key]}</td>
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
                  <td>{metrics.operationMetrics.DirectoriesCreated && metrics.operationMetrics.DirectoriesCreated.count}</td>
                  <th>File Block Infos Got</th>
                  <td>{metrics.operationMetrics.FileBlockInfosGot && metrics.operationMetrics.FileBlockInfosGot.count}</td>
                </tr>
                <tr>
                  <th>File Infos Got</th>
                  <td>{metrics.operationMetrics.FileInfosGot && metrics.operationMetrics.FileInfosGot.count}</td>
                  <th>Files Completed</th>
                  <td>{metrics.operationMetrics.FilesCompleted && metrics.operationMetrics.FilesCompleted.count}</td>
                </tr>
                <tr>
                  <th>Files Created</th>
                  <td>{metrics.operationMetrics.FilesCreated && metrics.operationMetrics.FilesCreated.count}</td>
                  <th>Files Freed</th>
                  <td>{metrics.operationMetrics.FilesFreed && metrics.operationMetrics.FilesFreed.count}</td>
                </tr>
                <tr>
                  <th>Files Persisted</th>
                  <td>{metrics.operationMetrics.FilesPersisted && metrics.operationMetrics.FilesPersisted.count}</td>
                  <th>Files Pinned</th>
                  <td>{metrics.operationMetrics.FilesPinned && metrics.operationMetrics.FilesPinned.count}</td>
                </tr>
                <tr>
                  <th>New Blocks Got</th>
                  <td>{metrics.operationMetrics.NewBlocksGot && metrics.operationMetrics.NewBlocksGot.count}</td>
                  <th>Paths Deleted</th>
                  <td>{metrics.operationMetrics.PathsDeleted && metrics.operationMetrics.PathsDeleted.count}</td>
                </tr>
                <tr>
                  <th>Paths Mounted</th>
                  <td>{metrics.operationMetrics.PathsMounted && metrics.operationMetrics.PathsMounted.count}</td>
                  <th>Paths Renamed</th>
                  <td>{metrics.operationMetrics.PathsRenamed && metrics.operationMetrics.PathsRenamed.count}</td>
                </tr>
                <tr>
                  <th>Paths Unmounted</th>
                  <td>{metrics.operationMetrics.PathsUnmounted && metrics.operationMetrics.PathsUnmounted.count}</td>
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
                  <th>{metrics.rpcInvocationMetrics.CompleteFileOps && metrics.rpcInvocationMetrics.CompleteFileOps.count}</th>
                  <th>CreateDirectory Operations</th>
                  <th>{metrics.rpcInvocationMetrics.CreateDirectoryOps && metrics.rpcInvocationMetrics.CreateDirectoryOps.count}</th>
                </tr>
                <tr>
                  <th>CreateFile Operations</th>
                  <th>{metrics.rpcInvocationMetrics.CreateFileOps && metrics.rpcInvocationMetrics.CreateFileOps.count}</th>
                  <th>DeletePath Operations</th>
                  <th>{metrics.rpcInvocationMetrics.DeletePathOps && metrics.rpcInvocationMetrics.DeletePathOps.count}</th>
                </tr>
                <tr>
                  <th>FreeFile Operations</th>
                  <th>{metrics.rpcInvocationMetrics.FreeFileOps && metrics.rpcInvocationMetrics.FreeFileOps.count}</th>
                  <th>GetFileBlockInfo Operations</th>
                  <th>{metrics.rpcInvocationMetrics.GetFileBlockInfoOps && metrics.rpcInvocationMetrics.GetFileBlockInfoOps.count}</th>
                </tr>
                <tr>
                  <th>GetFileInfo Operations</th>
                  <th>{metrics.rpcInvocationMetrics.GetFileInfoOps && metrics.rpcInvocationMetrics.GetFileInfoOps.count}</th>
                  <th>GetNewBlock Operations</th>
                  <th>{metrics.rpcInvocationMetrics.GetNewBlockOps && metrics.rpcInvocationMetrics.GetNewBlockOps.count}</th>
                </tr>
                <tr>
                  <th>Mount Operations</th>
                  <th>{metrics.rpcInvocationMetrics.MountOps && metrics.rpcInvocationMetrics.MountOps.count}</th>
                  <th>RenamePath Operations</th>
                  <th>{metrics.rpcInvocationMetrics.RenamePathOps && metrics.rpcInvocationMetrics.RenamePathOps.count}</th>
                </tr>
                <tr>
                  <th>SetAcl Operations</th>
                  <th>{metrics.rpcInvocationMetrics.SetAclOps && metrics.rpcInvocationMetrics.SetAclOps.count}</th>
                  <th>SetAttribute Operations</th>
                  <th>{metrics.rpcInvocationMetrics.SetAttributeOps && metrics.rpcInvocationMetrics.SetAttributeOps.count}</th>
                </tr>
                <tr>
                  <th>Unmount Operations</th>
                  <th>{metrics.rpcInvocationMetrics.UnmountOps && metrics.rpcInvocationMetrics.UnmountOps.count}</th>
                </tr>
                </tbody>
              </Table>
            </div>
            {Object.keys(metrics.ufsOps).map((key: string) => (
              <div key={key} className="col-12">
                <h5>Under FileSystem Operations of {key}</h5>
                <Table hover={true}>
                  <tbody>
                  {Object.keys(metrics.ufsOps[key]).map((innerKey: string) => (
                    <tr key={innerKey}>
                      <th>{innerKey}</th>
                      <td>{metrics.ufsOps[key][innerKey]}</td>
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
