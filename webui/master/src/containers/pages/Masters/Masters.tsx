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
import { Table } from 'reactstrap';
import { AnyAction, compose, Dispatch } from 'redux';

import { withErrors, withLoadingMessage, withFetchData } from '@alluxio/common-ui/src/components';
import { IMasterInfo } from '../../../constants';
import { IApplicationState } from '../../../store';
import { fetchRequest } from '../../../store/masters/actions';
import { IMasters } from '../../../store/masters/types';
import { IInit } from '../../../store/init/types';
import { createAlertErrors } from '@alluxio/common-ui/src/utilities';
import { ICommonState } from '@alluxio/common-ui/src/constants';

interface IPropsFromState extends ICommonState {
  initData: IInit;
  mastersData: IMasters;
}

interface IPropsFromDispatch {
  fetchRequest: typeof fetchRequest;
}

export type AllProps = IPropsFromState & IPropsFromDispatch;

export class MastersPresenter extends React.Component<AllProps> {
  public render(): JSX.Element {
    const { initData, mastersData } = this.props;

    return (
      <div className="masters-page">
        <div className="container-fluid">
          <div className="row">
            <div className="col-12">
              <h5>Leader Master</h5>
              <Table hover={true}>
                <thead>
                  <tr>
                    <th>Master Host</th>
                    <th>Master Port</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td>{mastersData.leaderMasterInfo.address.host}</td>
                    <td>{mastersData.leaderMasterInfo.address.rpcPort}</td>
                  </tr>
                </tbody>
              </Table>
            </div>
          </div>
          <div className="row">
            <div className="col-12">
              <h5>Standby Masters</h5>
              <Table hover={true}>
                <thead>
                  <tr>
                    {initData.debug ? <th>[D]Master Id</th> : null}
                    <th>Master Host</th>
                    <th>Master Port</th>
                    <th>Last Heartbeat</th>
                  </tr>
                </thead>
                <tbody>
                  {mastersData.normalMasterInfos.map((masterInfo: IMasterInfo) => (
                    <tr key={masterInfo.id}>
                      {initData.debug ? <td>{masterInfo.id}</td> : null}
                      <td>{masterInfo.address.host}</td>
                      <td>{masterInfo.address.rpcPort}</td>
                      <td>{new Date(masterInfo.lastUpdatedTimeMs).toTimeString()}</td>
                    </tr>
                  ))}
                </tbody>
              </Table>
            </div>
          </div>
          <div className="row">
            <div className="col-12">
              <h5>Lost Masters</h5>
              <Table hover={true}>
                <thead>
                  <tr>
                    {initData.debug ? <th>[D]Master Id</th> : null}
                    <th>Master Host</th>
                    <th>Master Port</th>
                    <th>Last Heartbeat</th>
                  </tr>
                </thead>
                <tbody>
                  {mastersData.failedMasterInfos.map((masterInfo: IMasterInfo) => (
                    <tr key={masterInfo.id}>
                      {initData.debug ? <td>{masterInfo.id}</td> : null}
                      <td>{masterInfo.address.host}</td>
                      <td>{masterInfo.address.rpcPort}</td>
                      <td>{new Date(masterInfo.lastUpdatedTimeMs).toTimeString()}</td>
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

const mapStateToProps = ({ init, refresh, masters }: IApplicationState): IPropsFromState => ({
  initData: init.data,
  errors: createAlertErrors(init.errors !== undefined || masters.errors !== undefined),
  loading: init.loading || masters.loading,
  refresh: refresh.data,
  mastersData: masters.data,
  class: 'masters-page',
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
)(MastersPresenter) as typeof React.Component;
