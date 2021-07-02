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

import { History, LocationState } from 'history';
import React from 'react';
import { Table } from 'reactstrap';
import {
  FileView,
  withErrors,
  withFetchDataFromPath,
  withFluidContainer,
  withLoadingMessage,
  withTextAreaResize,
} from '..';
import { IAlertErrors, ICommonState, IFileInfo, IRequest } from '../../constants';
import { createAlertErrors, renderFileNameLink } from '../../utilities';
import { ILogs, ILogsState } from '../../store/logs/types';
import { IRefreshState } from '../../store/refresh/types';
import { AnyAction, compose, Dispatch } from 'redux';
import { fetchRequest } from '../../store/logs/actions';

export interface IPropsFromState extends ICommonState {
  data: ILogs;
}

interface IPropsFromDispatch {
  fetchRequest: typeof fetchRequest;
}

interface ILogsProps {
  history: History<LocationState>;
  location: { search: string };
  queryStringSuffix: string;
  request: IRequest;
  textAreaHeight: number;
  updateRequestParameter: (reqParam: string, value: string | undefined) => void;
}

export type AllProps = IPropsFromState & ILogsProps & IPropsFromDispatch;

export class LogsPresenter extends React.Component<AllProps> {
  public render(): JSX.Element {
    const { data, queryStringSuffix } = this.props;

    return (
      <div className="col-12">
        {data.fileData !== null
          ? this.renderFileView(data, queryStringSuffix)
          : this.renderDirectoryListing(data.fileInfos || [])}
      </div>
    );
  }

  private renderFileView(logs: ILogs, queryStringSuffix: string): JSX.Element {
    const {
      textAreaHeight,
      request: { end, offset, path },
      history,
    } = this.props;
    const offsetInputHandler = this.createInputChangeHandler('offset').bind(this);
    const beginInputHandler = this.createButtonHandler('end', undefined).bind(this);
    const endInputHandler = this.createButtonHandler('end', '1').bind(this);
    return (
      <FileView
        beginInputHandler={beginInputHandler}
        end={end}
        endInputHandler={endInputHandler}
        offset={offset}
        offsetInputHandler={offsetInputHandler}
        path={path}
        queryStringPrefix="/logs"
        queryStringSuffix={queryStringSuffix}
        textAreaHeight={textAreaHeight}
        viewData={logs}
        history={history}
      />
    );
  }

  private renderDirectoryListing(fileInfos: IFileInfo[]): JSX.Element {
    return (
      <Table hover={true}>
        <thead>
          <tr>
            <th>File Name</th>
            <th>Size</th>
            <th>Block Size</th>
            <th>In-Alluxio</th>
            <th>Persistence State</th>
            <th>Pin</th>
            <th>Modification Time</th>
          </tr>
        </thead>
        <tbody>
          {fileInfos &&
            fileInfos.map((fileInfo: IFileInfo) => (
              <tr key={fileInfo.absolutePath}>
                <td>{renderFileNameLink(fileInfo.absolutePath, `/logs?path=`)}</td>
                <td>{fileInfo.size}</td>
                <td>{fileInfo.blockSizeBytes}</td>
                <td>{fileInfo.inAlluxioPercentage}%</td>
                <td>{fileInfo.persistenceState}</td>
                <td>{fileInfo.pinned ? 'YES' : 'NO'}</td>
                <td>{fileInfo.modificationTime}</td>
              </tr>
            ))}
        </tbody>
      </Table>
    );
  }

  private createInputChangeHandler(reqParam: string): (e: React.ChangeEvent<HTMLInputElement>) => void {
    return (event: React.ChangeEvent<HTMLInputElement>): void => {
      this.props.updateRequestParameter(reqParam, event.target.value);
    };
  }

  private createButtonHandler(reqParam: string, value: string | undefined): () => void {
    return (): void => {
      this.props.updateRequestParameter(reqParam, value);
    };
  }
}

export function getLogPropsFromState(logs: ILogsState, refresh: IRefreshState): IPropsFromState {
  const errors: IAlertErrors = createAlertErrors(logs.errors != undefined, [
    logs.data.invalidPathError,
    logs.data.fatalError,
  ]);
  return {
    data: logs.data,
    errors: errors,
    loading: logs.loading,
    refresh: refresh.data,
    class: 'logs-page',
  };
}

export const mapDispatchToLogProps = (dispatch: Dispatch): { fetchRequest: (request: IRequest) => AnyAction } => ({
  fetchRequest: (request: IRequest): AnyAction => dispatch(fetchRequest(request)),
});

export default compose(
  withFetchDataFromPath,
  withErrors,
  withLoadingMessage,
  withTextAreaResize,
  withFluidContainer,
)(LogsPresenter) as React.ComponentType<AllProps>;
