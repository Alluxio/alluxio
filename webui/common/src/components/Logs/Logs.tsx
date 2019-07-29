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
import {History, LocationState} from 'history';
import React from 'react';
import {Alert, Table} from 'reactstrap';
import {FileView, LoadingMessage} from '..';
import {IFileInfo} from '../../constants';
import {getDebouncedFunction, parseQuerystring, renderFileNameLink} from '../../utilities';
import {fetchRequest} from '../../store/logs/actions';
import {ILogs} from '../../store/logs/types';

interface IPropsFromState {
  data: ILogs;
  errors?: AxiosResponse;
  loading: boolean;
  location: {
    search: string;
  };
  refresh: boolean;
}

interface IPropsFromDispatch {
  fetchRequest: typeof fetchRequest;
}

interface ILogsState {
  end?: string;
  limit?: string;
  offset?: string;
  path?: string;
  textAreaHeight?: number;
}

interface ILogsProps {
  history: History<LocationState>;
}

export type AllProps = IPropsFromState & IPropsFromDispatch & ILogsProps;

export class Logs extends React.Component<AllProps, ILogsState> {
  private readonly textAreaResizeMs = 100;
  private readonly debouncedUpdateTextAreaHeight = getDebouncedFunction(this.updateTextAreaHeight.bind(this), this.textAreaResizeMs, true);

  constructor(props: AllProps) {
    super(props);

    let {path, offset, limit, end} = parseQuerystring(this.props.location.search);
    offset = offset || '0';
    this.state = {end, limit, offset, path};
  }

  public componentDidUpdate(prevProps: AllProps) {
    const {refresh, location: {search}} = this.props;
    const {refresh: prevRefresh, location: {search: prevSearch}} = prevProps;
    if (search !== prevSearch) {
      let {path, offset, limit, end} = parseQuerystring(search);
      offset = offset || '0';
      this.setState({path, offset, limit, end});
      this.fetchData(path, offset, limit, end);
    }
    if (refresh !== prevRefresh) {
      const {path, offset, limit, end} = this.state;
      this.fetchData(path, offset, limit, end);
    }
  }

  public componentWillMount() {
    const {path, offset, limit, end} = this.state;
    this.fetchData(path, offset, limit, end);
    this.updateTextAreaHeight();
  }

  public componentDidMount() {
    window.addEventListener('resize', this.debouncedUpdateTextAreaHeight);
  }

  public componentWillUnmount() {
    window.removeEventListener('resize', this.debouncedUpdateTextAreaHeight);
  }

  public render() {
    const {errors, data, loading} = this.props;
    let queryStringSuffix = Object.entries(this.state)
      .filter((obj: any[]) => ['offset', 'limit', 'end'].includes(obj[0]) && obj[1] != undefined)
      .map((obj: any) => `${obj[0]}=${obj[1]}`).join('&');
    queryStringSuffix = queryStringSuffix ? '&' + queryStringSuffix : queryStringSuffix;

    if (errors || data.invalidPathError || data.fatalError) {
      return (
        <Alert color="danger">
          {errors && <div>Unable to reach the api endpoint for this page.</div>}
          {data.invalidPathError && <div>{data.invalidPathError}</div>}
          {data.fatalError && <div>{data.fatalError}</div>}
        </Alert>
      );
    }

    if (loading) {
      return (
        <div className="logs-page">
          <LoadingMessage/>
        </div>
      );
    }

    return (
      <div className="logs-page">
        <div className="container-fluid">
          <div className="row">
            <div className="col-12">
              {data.fileData !== null
                  ? this.renderFileView(data, queryStringSuffix)
                  : this.renderDirectoryListing(data.fileInfos)}
            </div>
          </div>
        </div>
      </div>
    );
  }

  private renderFileView(logs: ILogs, queryStringSuffix: string) {
    const {textAreaHeight, path, offset, end} = this.state;
    const {history} = this.props;
    const offsetInputHandler = this.createInputChangeHandler('offset', value => value).bind(this);
    const beginInputHandler = this.createButtonHandler('end', value => undefined).bind(this);
    const endInputHandler = this.createButtonHandler('end', value => '1').bind(this);
    return (
      <FileView beginInputHandler={beginInputHandler} end={end} endInputHandler={endInputHandler}
                offset={offset} offsetInputHandler={offsetInputHandler} path={path}
                queryStringPrefix="/logs" queryStringSuffix={queryStringSuffix} textAreaHeight={textAreaHeight}
                viewData={logs} history={history}/>
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

  private fetchData(path?: string, offset?: string, limit?: string, end?: string) {
    this.props.fetchRequest(path, offset, limit, end);
  }

  private createInputChangeHandler(stateKey: string, stateValueCallback: (value: string) => string | undefined) {
    return (event: React.ChangeEvent<HTMLInputElement>) => {
      const value = event.target.value;
      this.setState({...this.state, [stateKey]: stateValueCallback(value)});
    };
  }

  private createButtonHandler(stateKey: string, stateValueCallback: (value?: string) => string | undefined) {
    return (event: React.MouseEvent<HTMLButtonElement>) => {
      this.setState({...this.state, [stateKey]: stateValueCallback()});
    };
  }

  private updateTextAreaHeight() {
    this.setState({textAreaHeight: window.innerHeight / 2});
  }
}
