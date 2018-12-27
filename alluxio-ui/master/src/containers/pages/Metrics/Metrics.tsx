import React from 'react';
import {connect} from 'react-redux';
import {Alert, Progress, Table} from 'reactstrap';
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

interface IMetricsProps {
  refreshValue: boolean;
}

type AllProps = IPropsFromState & IPropsFromDispatch & IMetricsProps;

class Metrics extends React.Component<AllProps> {
  public componentWillReceiveProps(props: AllProps) {
    const {refreshValue} = this.props;
    if (props.refreshValue !== refreshValue) {
      this.props.fetchRequest();
    }
  }

  public componentWillMount() {
    this.props.fetchRequest();
  }

  public render() {
    const {errors, loading, metrics} = this.props;

    if (loading) {
      return (
        <LoadingMessage/>
      );
    }

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
                  <td>{metrics.totalBytesReadLocal}</td>
                  <th>From Remote Instances</th>
                  <td>{metrics.totalBytesReadRemote}</td>
                </tr>
                <tr>
                  <th>Under Filesystem Read</th>
                  <td>{metrics.totalBytesReadUfs}</td>
                </tr>
                <tr>
                  <th>Alluxio Write</th>
                  <td>{metrics.totalBytesWrittenAlluxio}</td>
                  <th>Under Filesystem Write</th>
                  <td>{metrics.totalBytesWrittenUfs}</td>
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
                  <td>{metrics.rpcInvocationMetrics['Master.CompleteFileOps'] && metrics.rpcInvocationMetrics['Master.CompleteFileOps'].count}</td>
                  <th>CreateDirectory Operations</th>
                  <td>{metrics.rpcInvocationMetrics['Master.CreateDirectoryOps'] && metrics.rpcInvocationMetrics['Master.CreateDirectoryOps'].count}</td>
                </tr>
                <tr>
                  <th>CreateFile Operations</th>
                  <td>{metrics.rpcInvocationMetrics['Master.CreateFileOps'] && metrics.rpcInvocationMetrics['Master.CreateFileOps'].count}</td>
                  <th>DeletePath Operations</th>
                  <td>{metrics.rpcInvocationMetrics['Master.DeletePathOps'] && metrics.rpcInvocationMetrics['Master.DeletePathOps'].count}</td>
                </tr>
                <tr>
                  <th>FreeFile Operations</th>
                  <td>{metrics.rpcInvocationMetrics['Master.FreeFileOps'] && metrics.rpcInvocationMetrics['Master.FreeFileOps'].count}</td>
                  <th>GetFileBlockInfo Operations</th>
                  <td>{metrics.rpcInvocationMetrics['Master.GetFileBlockInfoOps'] && metrics.rpcInvocationMetrics['Master.GetFileBlockInfoOps'].count}</td>
                </tr>
                <tr>
                  <th>GetFileInfo Operations</th>
                  <td>{metrics.rpcInvocationMetrics['Master.GetFileInfoOps'] && metrics.rpcInvocationMetrics['Master.GetFileInfoOps'].count}</td>
                  <th>GetNewBlock Operations</th>
                  <td>{metrics.rpcInvocationMetrics['Master.GetNewBlockOps'] && metrics.rpcInvocationMetrics['Master.GetNewBlockOps'].count}</td>
                </tr>
                <tr>
                  <th>Mount Operations</th>
                  <td>{metrics.rpcInvocationMetrics['Master.MountOps'] && metrics.rpcInvocationMetrics['Master.MountOps'].count}</td>
                  <th>RenamePath Operations</th>
                  <td>{metrics.rpcInvocationMetrics['Master.RenamePathOps'] && metrics.rpcInvocationMetrics['Master.RenamePathOps'].count}</td>
                </tr>
                <tr>
                  <th>SetAcl Operations</th>
                  <td>{metrics.rpcInvocationMetrics['Master.SetAclOps'] && metrics.rpcInvocationMetrics['Master.SetAclOps'].count}</td>
                  <th>SetAttribute Operations</th>
                  <td>{metrics.rpcInvocationMetrics['Master.SetAttributeOps'] && metrics.rpcInvocationMetrics['Master.SetAttributeOps'].count}</td>
                </tr>
                <tr>
                  <th>Unmount Operations</th>
                  <td>{metrics.rpcInvocationMetrics['Master.UnmountOps'] && metrics.rpcInvocationMetrics['Master.UnmountOps'].count}</td>
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
