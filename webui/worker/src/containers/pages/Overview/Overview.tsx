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
import { bytesToString, createAlertErrors } from '@alluxio/common-ui/src/utilities';
import { IStorageTierInfo } from '../../../constants';
import { IApplicationState } from '../../../store';
import { fetchRequest } from '../../../store/overview/actions';
import { IOverview } from '../../../store/overview/types';
import { ICommonState } from '@alluxio/common-ui/src/constants';

interface IPropsFromState extends ICommonState {
  data: IOverview;
}

interface IPropsFromDispatch {
  fetchRequest: typeof fetchRequest;
}

export type AllProps = IPropsFromState & IPropsFromDispatch;

export class OverviewPresenter extends React.Component<AllProps> {
  public render(): JSX.Element {
    const { data } = this.props;

    return (
      <React.Fragment>
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
                <td>
                  {data.capacityBytes} / {data.usedBytes}
                </td>
              </tr>
              {data.usageOnTiers.map((info: IStorageTierInfo) => (
                <tr key={info.tierAlias}>
                  <th scope="row">{info.tierAlias} Capacity / Used</th>
                  <td>
                    {bytesToString(info.capacityBytes)} / {bytesToString(info.usedBytes)}
                  </td>
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
                const used = Math.round((info.usedBytes / info.capacityBytes) * 10000) / 100;
                const free = 100 - used;
                return (
                  <tr key={info.tierAlias}>
                    <td>{info.tierAlias}</td>
                    <td>{info.dirPath}</td>
                    <td>{bytesToString(info.capacityBytes)}</td>
                    <td>{bytesToString(info.usedBytes)}</td>
                    <td>
                      <Progress className="h-50 mt-1" multi={true}>
                        <Progress bar={true} color="dark" value={`${free}`}>
                          {free}% Free
                        </Progress>
                        <Progress bar={true} color="secondary" value={`${used}`}>
                          {used}% Used
                        </Progress>
                      </Progress>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </Table>
        </div>
      </React.Fragment>
    );
  }
}

const mapStateToProps = ({ overview, refresh }: IApplicationState): IPropsFromState => ({
  data: overview.data,
  errors: createAlertErrors(overview.errors !== undefined),
  loading: overview.loading,
  refresh: refresh.data,
  class: 'overview-page',
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
)(OverviewPresenter) as typeof React.Component;
