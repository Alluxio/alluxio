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
import {Link} from 'react-router-dom';
import {Alert, Table} from 'reactstrap';
import {Dispatch} from 'redux';

import {FileView} from '@alluxio/common-ui/src/components';
import {IFileInfo} from '@alluxio/common-ui/src/constants';
import {getDebouncedFunction, parseQuerystring} from '@alluxio/common-ui/src/utilities';
import {IApplicationState} from '../../../store';
import {fetchRequest} from '../../../store/logs/actions';
import {ILogs} from '../../../store/logs/types';

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
  lastFetched: {
    end?: string;
    limit?: string;
    offset?: string;
    path?: string;
  };
  textAreaHeight?: number;
}

export type AllProps = IPropsFromState & IPropsFromDispatch;

export class Logs extends React.Component<AllProps, ILogsState> {
  private readonly textAreaResizeMs = 100;
  private readonly debouncedUpdateTextAreaHeight = getDebouncedFunction(this.updateTextAreaHeight.bind(this), this.textAreaResizeMs, true);

  constructor(props: AllProps) {
    super(props);

    const {path, offset, limit, end} = parseQuerystring(this.props.location.search);
    this.state = {end, limit, offset, path, lastFetched: {}};
  }

  public componentDidUpdate(prevProps: AllProps) {
    const {refresh, location: {search}} = this.props;
    const {refresh: prevRefresh, location: {search: prevSearch}} = prevProps;
    if (search !== prevSearch) {
      const {path, offset, limit, end} = parseQuerystring(search);
      this.setState({path, offset, limit, end});
      this.fetchData(path, offset, limit, end);
    } else if (refresh !== prevRefresh) {
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
    const {errors, data} = this.props;
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

    return (
      <div className="logs-page">
        <div className="container-fluid">
          <div className="row">
            <div className="col-12">
              {data.fileData && this.renderFileView(data, queryStringSuffix)}
              {!data.fileData && this.renderDirectoryListing(data.fileInfos, queryStringSuffix)}
            </div>
          </div>
        </div>
      </div>
    );
  }

  private renderFileView(logs: ILogs, queryStringSuffix: string) {
    const {textAreaHeight, path, offset, end, lastFetched} = this.state;
    const offsetInputHandler = this.createInputHandler('offset', value => value).bind(this);
    const beginInputHandler = this.createButtonHandler('end', value => undefined).bind(this);
    const endInputHandler = this.createButtonHandler('end', value => '1').bind(this);
    return (
      <FileView beginInputHandler={beginInputHandler} end={end} endInputHandler={endInputHandler}
                lastFetched={lastFetched} offset={offset} offsetInputHandler={offsetInputHandler} path={path}
                queryStringPrefix="/logs" queryStringSuffix={queryStringSuffix} textAreaHeight={textAreaHeight}
                viewData={logs}/>
    );
  }

  private renderDirectoryListing(fileInfos: IFileInfo[], queryStringSuffix: string) {
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
          <th>Creation Time</th>
          <th>Modification Time</th>
          <th>[D]DepID</th>
          <th>[D]INumber</th>
          <th>[D]UnderfsPath</th>
          <th>[D]File Locations</th>
        </tr>
        </thead>
        <tbody>
        {fileInfos && fileInfos.map((fileInfo: IFileInfo) => (
          <tr key={fileInfo.absolutePath}>
            <td>
              {this.renderFileNameLink(fileInfo.absolutePath, queryStringSuffix)}
            </td>
            <td>{fileInfo.size}</td>
            <td>{fileInfo.blockSizeBytes}</td>
            <td>{fileInfo.inAlluxioPercentage}%</td>
            <td>{fileInfo.persistenceState}</td>
            <td>{fileInfo.pinned ? 'YES' : 'NO'}</td>
            <td>{fileInfo.creationTime}</td>
            <td>{fileInfo.modificationTime}</td>
            <td>{fileInfo.id}</td>
            <td>
              {fileInfo.fileLocations.map((location: string) => <div key={location}>location</div>)}
            </td>
            <td>{fileInfo.absolutePath}</td>
            <td>
              {fileInfo.fileLocations.map((location: string) => (
                <div key={location}>{location}</div>
              ))}
            </td>
          </tr>
        ))}
        </tbody>
      </Table>
    )
  }

  private renderFileNameLink(filePath: string, queryStringSuffix: string) {
    const {lastFetched} = this.state;
    if (filePath === lastFetched.path) {
      return (
        filePath
      );
    }

    return (
      <Link to={`/logs?path=${filePath}${queryStringSuffix}`}>
        {filePath}
      </Link>
    );
  }

  private fetchData(path?: string, offset?: string, limit?: string, end?: string) {
    this.setState({lastFetched: {path, offset, limit, end}});
    this.props.fetchRequest(path, offset, limit, end);
  }

  private createInputHandler(stateKey: string, stateValueCallback: (value: string) => string | undefined) {
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

const mapStateToProps = ({logs, refresh}: IApplicationState) => ({
  data: logs.data,
  errors: logs.errors,
  loading: logs.loading,
  refresh: refresh.data
});

const mapDispatchToProps = (dispatch: Dispatch) => ({
  fetchRequest: (path?: string, offset?: string, limit?: string, end?: string) => dispatch(fetchRequest(path, offset, limit, end))
});

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(Logs);
