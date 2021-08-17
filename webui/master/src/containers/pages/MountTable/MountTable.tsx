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
import { IApplicationState } from '../../../store';
import { fetchRequest } from '../../../store/mounttable/actions';
import { IMountPointInfo, IMountTable } from '../../../store/mounttable/types';
import { IInit } from '../../../store/init/types';
import { createAlertErrors } from '@alluxio/common-ui/src/utilities';
import { ICommonState } from '@alluxio/common-ui/src/constants';

interface IPropsFromState extends ICommonState {
  initData: IInit;
  mountTableData: IMountTable;
}

interface IPropsFromDispatch {
  fetchRequest: typeof fetchRequest;
}

export type AllProps = IPropsFromState & IPropsFromDispatch;

export class MountTablePresenter extends React.Component<AllProps> {
  public render(): JSX.Element {
    const { mountTableData } = this.props;

    return (
      <div className="mounttable-page">
        <div className="container-fluid">
          <div className="row">
            <div className="col-12">
              <h5>Mount table</h5>
              <Table hover={true}>
                <thead>
                  <tr>
                    <th>Mount Point</th>
                    <th>UFS URI</th>
                    <th>UFS Type</th>
                    <th>UFS Capacity Bytes</th>
                    <th>UFS Used Bytes</th>
                    <th>ReadOnly</th>
                    <th>Shared</th>
                    <th>Properties</th>
                  </tr>
                </thead>
                <tbody>
                  {mountTableData.mountPointInfos.map((mountPointInfo: IMountPointInfo) => (
                    <tr key={mountPointInfo.mountPoint}>
                      <td>{mountPointInfo.mountPoint}</td>
                      <td>{mountPointInfo.ufsUri}</td>
                      <td>{mountPointInfo.ufsType}</td>
                      <td>{mountPointInfo.ufsCapacityBytes}</td>
                      <td>{mountPointInfo.ufsUsedBytes}</td>
                      <td>{mountPointInfo.readOnly ? 'YES' : 'NO'}</td>
                      <td>{mountPointInfo.shared ? 'YES' : 'NO'}</td>
                      <td>{mountPointInfo.properties}</td>
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

const mapStateToProps = ({ init, refresh, mountTable }: IApplicationState): IPropsFromState => ({
  initData: init.data,
  errors: createAlertErrors(init.errors !== undefined || mountTable.errors !== undefined),
  loading: init.loading || mountTable.loading,
  refresh: refresh.data,
  mountTableData: mountTable.data,
  class: 'mounttable-page',
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
)(MountTablePresenter) as typeof React.Component;
