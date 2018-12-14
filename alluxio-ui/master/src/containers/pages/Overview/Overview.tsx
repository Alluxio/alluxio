import React from 'react';
import {connect} from 'react-redux';
import {Progress, Table} from 'reactstrap';
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
    const {loading, overview} = this.props;

    if (loading) {
      return (
        <LoadingMessage/>
      );
    }

    let freeCapacity = Math.round((overview.capacity.total - overview.capacity.used) / overview.capacity.total * 100);
    let usedCapacity = 100 - freeCapacity;
    if (freeCapacity < 0) {
      freeCapacity = 0;
    }
    if (usedCapacity > 100) {
      usedCapacity = 100;
    }

    return (
      <div className="overview-page">
        <div className="conteiner-fluid">
          <div className="row">
            <div className="col-md-6">
              <h5>Alluxio Summary</h5>
              <Table hover={true}>
                <tbody>
                <tr>
                  <th scope="row">Master Address</th>
                  <td>{overview.masterNodeAddress}</td>
                </tr>
                <tr>
                  <th scope="row">Started</th>
                  <td>{overview.startTime}</td>
                </tr>
                <tr>
                  <th scope="row">Uptime</th>
                  <td>{overview.uptime}</td>
                </tr>
                <tr>
                  <th scope="row">Version</th>
                  <td>{overview.version}</td>
                </tr>
                <tr>
                  <th scope="row">Running Workers</th>
                  <td>{overview.liveWorkerNodes}</td>
                </tr>
                <tr>
                  <th scope="row">Startup Consistency Check</th>
                  <td>{overview.consistencyCheckStatus}</td>
                </tr>
                <tr>
                  <th scope="row">Server Configuration Check</th>
                  <td>{overview.configCheckStatus}</td>
                </tr>
                </tbody>
              </Table>
            </div>
            <div className="col-md-6">
              <h5>Cluster Usage Summary</h5>
              <Table hover={true}>
                <tbody>
                <tr>
                  <th scope="row">Workers Capacity</th>
                  <td>{overview.freeCapacity}</td>
                </tr>
                <tr>
                  <th scope="row">Workers Free / Used</th>
                  <td>{overview.freeCapacity} / {overview.usedCapacity}</td>
                </tr>
                <tr>
                  <th scope="row">UnderFS Capacity</th>
                  <td>{overview.diskCapacity}</td>
                </tr>
                <tr>
                  <th scope="row">UnderFS Free / Used</th>
                  <td>{overview.diskCapacity} / {overview.diskUsedCapacity}</td>
                </tr>
                </tbody>
              </Table>
            </div>
            <div className="col-md-12">
              <h5>Storage Usage Summary</h5>
              <Table hover={true}>
                <thead>
                <tr>
                  <th>Storage Alias</th>
                  <th>Space Capacity</th>
                  <th>Space Used</th>
                  <th>Space Usage</th>
                </tr>
                </thead>
                <tbody>
                <tr>
                  <td>?</td>
                  <td>{overview.diskFreeCapacity}</td>
                  <td>{overview.usedCapacity}</td>
                  <td>
                    <Progress multi={true}>
                      <Progress bar={true} color="success" value={`${freeCapacity}`}>{freeCapacity}% Free</Progress>
                      <Progress bar={true} color="danger" value={`${usedCapacity}`}>{usedCapacity}% Used</Progress>
                    </Progress>
                  </td>
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
