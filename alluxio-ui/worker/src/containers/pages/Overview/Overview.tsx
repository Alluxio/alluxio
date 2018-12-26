import React from 'react';
import {connect} from 'react-redux';
import {Progress, Table} from 'reactstrap';
import {Dispatch} from 'redux';

import {LoadingMessage} from '@alluxio/common-ui/src/components';
import {bytesToString} from '@alluxio/common-ui/src/utilities';
import {IStorageTierInfo} from '../../../constants';
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

    return (
      <div className="overview-page">
        <div className="container-fluid">
          <div className="row">
            <div className="col-md-6">
              <h5>Alluxio Summary</h5>
              <Table hover={true}>
                <tbody>
                <tr>
                  <th scope="row">Master Address</th>
                  <td>{overview.workerInfo.workerAddress}</td>
                </tr>
                <tr>
                  <th scope="row">Started</th>
                  <td>{overview.workerInfo.startTime}</td>
                </tr>
                <tr>
                  <th scope="row">Uptime</th>
                  <td>{overview.workerInfo.uptime}</td>
                </tr>
                <tr>
                  <th scope="row">Version</th>
                  <td>{overview.version}</td>
                </tr>
                </tbody>
              </Table>
            </div>
            <div className="col-md-6">
              <h5>Cluster Usage Summary</h5>
              <Table hover={true}>
                <tbody>
                <tr>
                  <th scope="row">Total Capacity / Used</th>
                  <td>{overview.capacityBytes} / {overview.usedBytes}</td>
                </tr>
                {overview.usageOnTiers.map((info: IStorageTierInfo) => (
                  <tr key={info.tierAlias}>
                    <th scope="row">{info.tierAlias} Capacity / Used</th>
                    <td>{bytesToString(info.capacityBytes)} / {bytesToString(info.usedBytes)}</td>
                  </tr>
                ))}
                </tbody>
              </Table>
            </div>
            <div className="col-md-12">
              <h5>Storage Usage Summary</h5>
              <Table hover={true}>
                <thead>
                <tr>
                  <th>Alias</th>
                  <th>Path</th>
                  <th>Capacity</th>
                  <th>Space Used</th>
                  <th>Space Usage</th>
                </tr>
                </thead>
                <tbody>
                {overview.storageDirs.map((info: IStorageTierInfo) => {
                  const used = Math.round(info.usedBytes / info.capacityBytes * 10000) / 100;
                  const free = 100 - used;
                  return (
                    <tr key={info.tierAlias}>
                      <td>{info.tierAlias}</td>
                      <td>{info.dirPath}</td>
                      <td>{bytesToString(info.capacityBytes)}</td>
                      <td>{bytesToString(info.usedBytes)}</td>
                      <td>
                        <Progress multi={true}>
                          <Progress bar={true} color="success" value={`${free}`}>{free}%
                            Free</Progress>
                          <Progress bar={true} color="danger" value={`${used}`}>{used}%
                            Used</Progress>
                        </Progress>
                      </td>
                    </tr>
                  );
                })}
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
