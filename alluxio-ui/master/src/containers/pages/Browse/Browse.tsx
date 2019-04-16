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

import {faFile, faFolder} from '@fortawesome/free-regular-svg-icons'
import {FontAwesomeIcon} from '@fortawesome/react-fontawesome'
import {AxiosResponse} from 'axios';
import {History, LocationState} from 'history';
import React from 'react';
import {connect} from 'react-redux';
import {Link} from 'react-router-dom';
import {Alert, Button, Form, FormGroup, Input, Label, Table} from 'reactstrap';
import {Dispatch} from 'redux';

import {FileView, LoadingMessage, Paginator} from '@alluxio/common-ui/src/components';
import {IFileBlockInfo, IFileInfo} from '@alluxio/common-ui/src/constants';
import {
  disableFormSubmit,
  getDebouncedFunction,
  parseQuerystring,
  renderFileNameLink
} from '@alluxio/common-ui/src/utilities';
import {IApplicationState} from '../../../store';
import {fetchRequest} from '../../../store/browse/actions';
import {IBrowse} from '../../../store/browse/types';
import {IInit} from '../../../store/init/types';

import './Browse.css';

interface IPropsFromState {
  browseData: IBrowse;
  browseErrors?: AxiosResponse;
  browseLoading: boolean;
  initData: IInit;
  initErrors?: AxiosResponse;
  initLoading: boolean;
  location: {
    search: string;
  };
  refresh: boolean;
}

interface IPropsFromDispatch {
  fetchRequest: typeof fetchRequest;
}

interface IBrowseState {
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

interface IBrowseProps {
  history: History<LocationState>;
}

export type AllProps = IPropsFromState & IPropsFromDispatch & IBrowseProps;

export class Browse extends React.Component<AllProps, IBrowseState> {
  private readonly textAreaResizeMs = 100;
  private readonly debouncedUpdateTextAreaHeight = getDebouncedFunction(this.updateTextAreaHeight.bind(this), this.textAreaResizeMs, true);

  constructor(props: AllProps) {
    super(props);

    let {path, offset, limit, end} = parseQuerystring(this.props.location.search);
    offset = offset || '0';
    this.state = {end, limit, offset, path: path || '/', lastFetched: {}};
  }

  public componentDidUpdate(prevProps: AllProps) {
    const {refresh, location: {search}} = this.props;
    const {refresh: prevRefresh, location: {search: prevSearch}} = prevProps;
    if (search !== prevSearch) {
      const {path, offset, limit, end} = parseQuerystring(this.props.location.search);
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
    const {browseErrors, browseData, browseLoading, initData, initLoading, initErrors} = this.props;
    let queryStringSuffix = Object.entries(this.state)
      .filter((obj: any[]) => ['offset', 'limit', 'end'].includes(obj[0]) && obj[1] != undefined)
      .map((obj: any) => `${obj[0]}=${obj[1]}`).join('&');
    queryStringSuffix = queryStringSuffix ? '&' + queryStringSuffix : queryStringSuffix;

    if (initErrors || browseErrors || browseData && (browseData.accessControlException || browseData.fatalError ||
      browseData.fileDoesNotExistException || browseData.invalidPathError || browseData.invalidPathException)) {
      return (
        <Alert color="danger">
          {browseErrors && <div>Unable to reach the api endpoint for this page.</div>}
          {browseData.accessControlException && <div>{browseData.accessControlException}</div>}
          {browseData.fatalError && <div>{browseData.fatalError}</div>}
          {browseData.fileDoesNotExistException && <div>{browseData.fileDoesNotExistException}</div>}
          {browseData.invalidPathError && <div>{browseData.invalidPathError}</div>}
          {browseData.invalidPathException && <div>{browseData.invalidPathException}</div>}
        </Alert>
      );
    }

    if (initLoading || browseLoading) {
      return (
        <div className="browse-page">
          <LoadingMessage/>
        </div>
      );
    }

    return (
      <div className="browse-page">
        <div className="container-fluid">
          <div className="row">
            <div className="col-12">
              {!browseData.currentDirectory.isDirectory && this.renderFileView(browseData, queryStringSuffix, initData)}
              {browseData.currentDirectory.isDirectory && this.renderDirectoryListing(initData, browseData, queryStringSuffix)}
            </div>
          </div>
        </div>
      </div>
    );
  }

  private renderFileView(browseData: IBrowse, queryStringSuffix: string, initData: IInit) {
    const {textAreaHeight, path, offset, end, lastFetched} = this.state;
    const {history} = this.props;
    const offsetInputHandler = this.createInputChangeHandler('offset', value => value).bind(this);
    const beginInputHandler = this.createButtonHandler('end', value => undefined).bind(this);
    const endInputHandler = this.createButtonHandler('end', value => '1').bind(this);
    return (
      <React.Fragment>
        <FileView allowDownload={true} beginInputHandler={beginInputHandler} end={end} endInputHandler={endInputHandler}
                  lastFetched={lastFetched} offset={offset || '0'} offsetInputHandler={offsetInputHandler} path={path}
                  queryStringPrefix="/browse" queryStringSuffix={queryStringSuffix} textAreaHeight={textAreaHeight}
                  viewData={browseData} history={history} proxyDownloadApiUrl={initData.proxyDownloadFileApiUrl}/>
        <hr/>
        <h6>Detailed blocks information (block capacity is {browseData.blockSizeBytes} Bytes):</h6>
        <Table hover={true}>
          <thead>
          <tr>
            <th>ID</th>
            <th>Size (Byte)</th>
            <th>In {browseData.highestTierAlias}</th>
            <th>Locations</th>
          </tr>
          </thead>
          <tbody>
          {browseData.fileBlocks.map((fileBlock: IFileBlockInfo) => (
            <tr key={fileBlock.id}>
              <td>{fileBlock.id}</td>
              <td>{fileBlock.blockLength}</td>
              <td>
                {fileBlock.isInHighestTier ? 'YES' : 'NO'}
              </td>
              <td>
                {fileBlock.locations.map((location: string) => (
                  <div key={location}>{location}</div>
                ))}
              </td>
            </tr>
          ))}
          </tbody>
        </Table>
      </React.Fragment>
    );
  }

  private renderDirectoryListing(initData: IInit, browseData: IBrowse, queryStringSuffix: string) {
    const {path, lastFetched, offset, limit} = this.state;
    const {history} = this.props;
    const fileInfos = browseData.fileInfos;
    const pathInputHandler = this.createInputChangeHandler('path', value => value).bind(this);
    return (
      <React.Fragment>
        <Form className="mb-3 browse-directory-form" id="browseDirectoryForm" inline={true}
              onSubmit={disableFormSubmit}>
          <FormGroup className="mb-2 mr-sm-2">
            <Button tag={Link} to={`/browse?path=/${queryStringSuffix}`} color="secondary"
                    outline={true} disabled={'/' === lastFetched.path}>Root</Button>
          </FormGroup>
          <FormGroup className="mb-2 mr-sm-2">
            <Label for="browsePath" className="mr-sm-2">Path</Label>
            <Input type="text" id="browsePath" placeholder="Enter a Path" value={path || '/'}
                   onChange={pathInputHandler}
                   onKeyUp={this.createInputEnterHandler(history, () => `/browse?path=${path}${queryStringSuffix}`)}/>
          </FormGroup>
          <FormGroup className="mb-2 mr-sm-2">
            <Button tag={Link} to={`/browse?path=${path}${queryStringSuffix}`} color="secondary"
                    disabled={path === lastFetched.path}>Go</Button>
          </FormGroup>
        </Form>
        <Table hover={true}>
          <thead>
          <tr>
            <th/>
            {/* Icon placeholder */}
            <th>File Name</th>
            <th>Size</th>
            <th>Block Size</th>
            <th>In-Alluxio</th>
            {browseData.showPermissions && (
              <React.Fragment>
                <th>Mode</th>
                <th>Owner</th>
                <th>Group</th>
              </React.Fragment>
            )}
            <th>Persistence State</th>
            <th>Pin</th>
            <th>Creation Time</th>
            <th>Modification Time</th>
            {initData.debug && (
              <React.Fragment>
                <th>[D]DepID</th>
                <th>[D]INumber</th>
                <th>[D]UnderfsPath</th>
                <th>[D]File Locations</th>
              </React.Fragment>
            )}
          </tr>
          </thead>
          <tbody>
          {fileInfos && fileInfos.map((fileInfo: IFileInfo) => (
            <tr key={fileInfo.absolutePath}>
              <td><FontAwesomeIcon icon={fileInfo.isDirectory ? faFolder : faFile}/></td>
              <td>
                {renderFileNameLink.call(this, fileInfo.absolutePath, `/browse?path=${fileInfo.absolutePath}`)}
              </td>
              <td>{fileInfo.size}</td>
              <td>{fileInfo.blockSizeBytes}</td>
              <td>{fileInfo.inAlluxioPercentage}%</td>
              {browseData.showPermissions && (
                <React.Fragment>
                  <td>
                    <pre className="mb-0"><code>{fileInfo.mode}</code></pre>
                  </td>
                  <td>{fileInfo.owner}</td>
                  <td>{fileInfo.group}</td>
                </React.Fragment>
              )}
              <td>{fileInfo.persistenceState}</td>
              <td>{fileInfo.pinned ? 'YES' : 'NO'}</td>
              <td>{fileInfo.creationTime}</td>
              <td>{fileInfo.modificationTime}</td>
              {initData.debug && (
                <React.Fragment>
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
                </React.Fragment>
              )}
            </tr>
          ))}
          </tbody>
        </Table>
        <Paginator baseUrl={'/browse'} path={path} total={browseData.ntotalFile} offset={offset} limit={limit}/>
      </React.Fragment>
    )
  }

  private fetchData(path?: string, offset?: string, limit?: string, end?: string) {
    this.setState({lastFetched: {path, offset, limit, end}});
    this.props.fetchRequest(path, offset, limit, end);
  }

  private createInputChangeHandler(stateKey: string, stateValueCallback: (value: string) => string | undefined) {
    return (event: React.ChangeEvent<HTMLInputElement>) => {
      const value = event.target.value;
      this.setState({...this.state, [stateKey]: stateValueCallback(value)});
    };
  }

  private createInputEnterHandler(history: History<LocationState>, stateValueCallback: (value: string) => string | undefined) {
    return (event: React.KeyboardEvent<HTMLInputElement>) => {
      const value = event.key;
      if (event.key === 'Enter') {
        const newPath = stateValueCallback(value);
        if (newPath) {
          if (history) {
            history.push(newPath);
          }
        }
      }
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

const mapStateToProps = ({browse, init, refresh}: IApplicationState) => ({
  browseData: browse.data,
  browseErrors: browse.errors,
  browseLoading: browse.loading,
  initData: init.data,
  initErrors: init.errors,
  initLoading: init.loading,
  refresh: refresh.data
});

const mapDispatchToProps = (dispatch: Dispatch) => ({
  fetchRequest: (path?: string, offset?: string, limit?: string, end?: string) => dispatch(fetchRequest(path, offset, limit, end))
});

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(Browse);
