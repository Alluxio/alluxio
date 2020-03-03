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

import {
  withErrors,
  withFetchDataFromPath,
  withFluidContainer,
  withLoadingMessage,
  Paginator,
} from '@alluxio/common-ui/src/components';
import { IAlertErrors, ICommonState, IFileInfo, IRequest } from '@alluxio/common-ui/src/constants';
import { IApplicationState } from '../../../store';
import { fetchRequest } from '../../../store/data/actions';
import { IData } from '../../../store/data/types';
import { createAlertErrors } from '@alluxio/common-ui/src/utilities';
import { routePaths } from '../../../constants';

interface IPropsFromState extends ICommonState {
  data: IData;
}

interface IPropsFromDispatch {
  fetchRequest: typeof fetchRequest;
}

interface IDataProps {
  request: IRequest;
}

export type AllProps = IPropsFromState & IPropsFromDispatch & IDataProps;

export class DataPresenter extends React.Component<AllProps> {
  public render(): JSX.Element {
    const {
      request: { offset, limit },
      data,
    } = this.props;

    return (
      <div className="col-12">
        {this.renderFileListing(data.fileInfos)}
        <Paginator baseUrl={routePaths.data} total={data.inAlluxioFileNum} offset={offset} limit={limit} />
      </div>
    );
  }

  private renderFileListing(fileInfos: IFileInfo[]): JSX.Element {
    return (
      <Table hover={true}>
        <thead>
          <tr>
            <th>File Path</th>
            <th>Size</th>
            <th>Block Size</th>
            <th>Permission</th>
            <th>Owner</th>
            <th>Group</th>
            <th>Pin</th>
            <th>Creation Time</th>
            <th>Modification Time</th>
          </tr>
        </thead>
        <tbody>
          {fileInfos &&
            fileInfos.map((fileInfo: IFileInfo) => (
              <tr key={fileInfo.absolutePath}>
                <td>{fileInfo.absolutePath}</td>
                <td>{fileInfo.size}</td>
                <td>{fileInfo.blockSizeBytes}</td>
                <td>
                  <pre className="mb-0">
                    <code>{fileInfo.mode}</code>
                  </pre>
                </td>
                <td>{fileInfo.owner}</td>
                <td>{fileInfo.group}</td>
                <td>{fileInfo.pinned ? 'YES' : 'NO'}</td>
                <td>{fileInfo.creationTime}</td>
                <td>{fileInfo.modificationTime}</td>
              </tr>
            ))}
        </tbody>
      </Table>
    );
  }
}

const mapStateToProps = ({ data, refresh }: IApplicationState): IPropsFromState => {
  const allErrors: IAlertErrors = createAlertErrors(data.errors != undefined, [
    data.data.permissionError,
    data.data.fatalError,
  ]);

  return {
    data: data.data,
    errors: allErrors,
    loading: data.loading,
    refresh: refresh.data,
    class: 'data-page',
  };
};

const mapDispatchToProps = (dispatch: Dispatch): { fetchRequest: (request: IRequest) => AnyAction } => ({
  fetchRequest: (request: IRequest): AnyAction => dispatch(fetchRequest(request)),
});

export default compose(
  connect(
    mapStateToProps,
    mapDispatchToProps,
  ),
  withFetchDataFromPath,
  withErrors,
  withLoadingMessage,
  withFluidContainer,
)(DataPresenter) as typeof React.Component;
