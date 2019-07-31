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

import {History, LocationState} from 'history';
import React from 'react';
import {Table} from 'reactstrap';
import {
  FileView,
  hasErrors,
  hasFetchDataWithPath, hasFluidContainer,
  hasLoader,
  hasTextAreaResize,
  IFetchDataPathType,
  ITextAreaResizeState
} from '..';
import {IAlertErrors, IFileInfo, IRequest} from '../../constants';
import {createAlertErrors, renderFileNameLink} from '../../utilities';
import {ILogs, ILogsState, ILogsStateToProps} from '../../store/logs/types';
import {IRefreshState} from "../../store/refresh/types";
import {compose, Dispatch} from "redux";
import {fetchRequest} from "../../store/logs/actions";

interface IPropsFromState {
  data: ILogs;
  location: {
    search: string;
  };
  refresh: boolean;
}

interface ILogsProps {
  history: History<LocationState>;
}

export type AllProps = IPropsFromState & ILogsProps & ITextAreaResizeState & IFetchDataPathType;

class Logs extends React.Component<AllProps> {
  public render() {
    const {data, queryStringSuffix} = this.props;

    return data.fileData !== null
      ? this.renderFileView(data, queryStringSuffix)
      : this.renderDirectoryListing(data.fileInfos);

  }

  private renderFileView(logs: ILogs, queryStringSuffix: string) {
    const {textAreaHeight, path, offset, end, history, createInputChangeHandler, createButtonHandler} = this.props;
    const offsetInputHandler = createInputChangeHandler('offset', value => value);//.bind(this);
    const beginInputHandler = createButtonHandler('end', value => undefined);//.bind(this);
    const endInputHandler = createButtonHandler('end', value => '1');//.bind(this);
    return (
      <div className="col-12">
        <FileView beginInputHandler={beginInputHandler} end={end} endInputHandler={endInputHandler}
                  offset={offset} offsetInputHandler={offsetInputHandler} path={path}
                  queryStringPrefix="/logs" queryStringSuffix={queryStringSuffix} textAreaHeight={textAreaHeight}
                  viewData={logs} history={history}/>
      </div>
    );
  }

  private renderDirectoryListing(fileInfos: IFileInfo[]) {
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
        {fileInfos && fileInfos.map((fileInfo: IFileInfo) => (
          <tr key={fileInfo.absolutePath}>
            <td>
              {renderFileNameLink(fileInfo.absolutePath, `/logs?path=`)}
            </td>
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
    )
  }
}

export function getLogPropsFromState(logs: ILogsState, refresh: IRefreshState): ILogsStateToProps {
  const errors: IAlertErrors = createAlertErrors(
      logs.errors != undefined,
      [logs.data.invalidPathError, logs.data.fatalError]
  );
  return {
    data: logs.data,
    errors: errors,
    loading: logs.loading,
    refresh: refresh.data,
    class: 'logs-page'
  }
}

export const mapDispatchToLogProps = (dispatch: Dispatch) => ({
  fetchRequest: (request: IRequest) => dispatch(fetchRequest(request))
});

export default compose(
  hasFetchDataWithPath,
  hasErrors,
  hasLoader,
  hasTextAreaResize,
  hasFluidContainer
)(Logs) as React.ComponentType<AllProps>;
