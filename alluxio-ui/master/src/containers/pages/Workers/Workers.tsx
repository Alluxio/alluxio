import React from 'react';
import {connect} from 'react-redux';
import {Table} from 'reactstrap';
import {Dispatch} from 'redux';

import {LoadingMessage} from '@alluxio/common-ui/src/components';
import {INodeInfo} from '../../../constants';
import {IApplicationState} from '../../../store';
import {fetchRequest} from '../../../store/workers/actions';
import {IWorkers} from '../../../store/workers/types';

import './Workers.css';

interface IPropsFromState {
  errors: string;
  loading: boolean;
  workers: IWorkers;
}

interface IPropsFromDispatch {
  fetchRequest: typeof fetchRequest;
}

type AllProps = IPropsFromState & IPropsFromDispatch;

class Workers extends React.Component<AllProps> {
  public componentWillMount() {
    this.props.fetchRequest();
  }

  public render() {
    const {loading, workers} = this.props;

    if (loading) {
      return (
        <LoadingMessage/>
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
                  <th>[D]Worker Id</th>
                  <th>[D]Uptime</th>
                  <th>Last Heartbeat</th>
                  <th>State</th>
                  <th>Workers Capacity</th>
                  <th>Space Used</th>
                </tr>
                </thead>
                <tbody>
                {workers.normalNodeInfos.map((nodeInfo: INodeInfo) => (
                  <tr key={nodeInfo.workerId}>
                    <td><a href={`//${nodeInfo.host}:30000`} target="_blank">{nodeInfo.host}</a></td>
                    <td>{nodeInfo.workerId}</td>
                    <td>{nodeInfo.uptimeClockTime}</td>
                    <td>{nodeInfo.lastHeartbeat}</td>
                    <td>{nodeInfo.state}</td>
                    <td>{nodeInfo.capacity}</td>
                    <td>{nodeInfo.usedMemory}</td>
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
                {workers.failedNodeInfos.map((nodeInfo: INodeInfo) => (
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

const mapStateToProps = ({workers}: IApplicationState) => ({
  errors: workers.errors,
  loading: workers.loading,
  workers: workers.workers
});

const mapDispatchToProps = (dispatch: Dispatch) => ({
  fetchRequest: () => dispatch(fetchRequest())
});

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(Workers);
