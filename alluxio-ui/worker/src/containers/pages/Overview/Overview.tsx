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

import {bytesToString} from '@alluxio/common-ui/src/utilities';
import {IStorageTierInfo} from '../../../constants';
import {IApplicationState} from '../../../store';
import {fetchRequest} from '../../../store/overview/actions';
import {IOverview} from '../../../store/overview/types';

interface IPropsFromState {
  data: IOverview;
  errors?: AxiosResponse;
  loading: boolean;
  refresh: boolean;
}

interface IPropsFromDispatch {
  fetchRequest: typeof fetchRequest;
}

export type AllProps = IPropsFromState & IPropsFromDispatch;

export class Overview extends React.Component<AllProps> {
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
      <div className="overview-page">
        <div className="container-fluid">
          <div className="row">
            <div className="col-md-6">
              <h5>Alluxio Summary</h5>
              <Table hover={true}>
                <tbody>
                <tr>
                  <th scope="row">Worker Address</th>
                  <td>{data.workerInfo.workerAddress}</td>
                </tr>
                <tr>
                  <th scope="row">Started</th>
                  <td>{data.workerInfo.startTime}</td>
                </tr>
                <tr>
                  <th scope="row">Uptime</th>
                  <td>{data.workerInfo.uptime}</td>
                </tr>
                <tr>
                  <th scope="row">Version</th>
                  <td>{data.version}</td>
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
                  <td>{data.capacityBytes} / {data.usedBytes}</td>
                </tr>
                {data.usageOnTiers.map((info: IStorageTierInfo) => (
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
                {data.storageDirs.map((info: IStorageTierInfo) => {
                  const used = Math.round(info.usedBytes / info.capacityBytes * 10000) / 100;
                  const free = 100 - used;
                  return (
                    <tr key={info.tierAlias}>
                      <td>{info.tierAlias}</td>
                      <td>{info.dirPath}</td>
                      <td>{bytesToString(info.capacityBytes)}</td>
                      <td>{bytesToString(info.usedBytes)}</td>
                      <td>
                        <Progress className="h-50 mt-1" multi={true}>
                          <Progress bar={true} color="dark" value={`${free}`}>{free}%
                            Free</Progress>
                          <Progress bar={true} color="secondary" value={`${used}`}>{used}%
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

const mapStateToProps = ({overview, refresh}: IApplicationState) => ({
  data: overview.data,
  errors: overview.errors,
  loading: overview.loading,
  refresh: refresh.data
});

const mapDispatchToProps = (dispatch: Dispatch) => ({
  fetchRequest: () => dispatch(fetchRequest())
});

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(Overview);
