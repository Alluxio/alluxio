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
import React from 'react';
import {connect} from 'react-redux';
import {Link} from 'react-router-dom';
import {Alert, Button, Form, FormGroup, Input, Label, Table} from 'reactstrap';
import {Dispatch} from 'redux';

import {FileView, Paginator} from '@alluxio/common-ui/src/components';
import {IFileBlockInfo, IFileInfo} from '@alluxio/common-ui/src/constants';
import {getDebouncedFunction, parseQuerystring} from '@alluxio/common-ui/src/utilities';
import {IApplicationState} from '../../../store';
import {fetchRequest} from '../../../store/browse/actions';
import {IBrowse} from '../../../store/browse/types';

import './Browse.css';

interface IPropsFromState {
  browse: IBrowse;
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

type AllProps = IPropsFromState & IPropsFromDispatch;

class Browse extends React.Component<AllProps, IBrowseState> {
  private readonly textAreaResizeMs = 100;
  private readonly debouncedUpdateTextAreaHeight = getDebouncedFunction(this.updateTextAreaHeight.bind(this), this.textAreaResizeMs, true);

  constructor(props: AllProps) {
    super(props);

    const {path, offset, limit, end} = parseQuerystring(this.props.location.search);
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
    const {errors, browse} = this.props;
    let queryStringSuffix = Object.entries(this.state)
      .filter((obj: any[]) => ['offset', 'limit', 'end'].includes(obj[0]) && obj[1] != undefined)
      .map((obj: any) => `${obj[0]}=${obj[1]}`).join('&');
    queryStringSuffix = queryStringSuffix ? '&' + queryStringSuffix : queryStringSuffix;

    if (errors || browse.accessControlException || browse.fatalError || browse.fileDoesNotExistException ||
      browse.invalidPathError || browse.invalidPathException) {
      return (
        <Alert color="danger">
          {errors && <div>Unable to reach the api endpoint for this page.</div>}
          {browse.accessControlException && <div>{browse.accessControlException}</div>}
          {browse.fatalError && <div>{browse.fatalError}</div>}
          {browse.fileDoesNotExistException && <div>{browse.fileDoesNotExistException}</div>}
          {browse.invalidPathError && <div>{browse.invalidPathError}</div>}
          {browse.invalidPathException && <div>{browse.invalidPathException}</div>}
        </Alert>
      );
    }

    return (
      <div className="browse-page">
        <div className="container-fluid">
          <div className="row">
            <div className="col-12">
              {!browse.currentDirectory.isDirectory && this.renderFileView(browse, queryStringSuffix)}
              {browse.currentDirectory.isDirectory && this.renderDirectoryListing(browse, queryStringSuffix)}
            </div>
          </div>
        </div>
      </div>
    );
  }

  private renderFileView(browse: IBrowse, queryStringSuffix: string) {
    const {textAreaHeight, path, offset, end, lastFetched} = this.state;
    const offsetInputHandler = this.createInputHandler('offset', value => value).bind(this);
    const beginInputHandler = this.createButtonHandler('end', value => undefined).bind(this);
    const endInputHandler = this.createButtonHandler('end', value => '1').bind(this);
    return (
      <React.Fragment>
        <FileView allowDownload={true} beginInputHandler={beginInputHandler} end={end} endInputHandler={endInputHandler}
                  lastFetched={lastFetched} offset={offset} offsetInputHandler={offsetInputHandler} path={path}
                  queryStringPrefix="/browse" queryStringSuffix={queryStringSuffix} textAreaHeight={textAreaHeight}
                  viewData={browse}/>
        <hr/>
        <h6>Detailed blocks information (block capacity is {browse.blockSizeBytes} Bytes):</h6>
        <Table hover={true}>
          <thead>
          <tr>
            <th>ID</th>
            <th>Size (Byte)</th>
            <th>In {browse.highestTierAlias}</th>
            <th>Locations</th>
          </tr>
          </thead>
          <tbody>
          {browse.fileBlocks.map((fileBlock: IFileBlockInfo) => (
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

  private renderDirectoryListing(browse: IBrowse, queryStringSuffix: string) {
    const {path, lastFetched, offset, limit} = this.state;
    const fileInfos = browse.fileInfos;
    const pathInputHandler = this.createInputHandler('path', value => value).bind(this);
    return (
      <React.Fragment>
        <Form className="mb-3 browse-directory-form" id="browseDirectoryForm" inline={true}>
          <FormGroup className="mb-2 mr-sm-2">
            <Button tag={Link} to={`/browse?path=/${queryStringSuffix}`} color="secondary"
                    outline={true} disabled={'/' === lastFetched.path}>Root</Button>
          </FormGroup>
          <FormGroup className="mb-2 mr-sm-2">
            <Label for="browsePath" className="mr-sm-2">Path</Label>
            <Input type="text" id="browsePath" placeholder="Enter a Path" value={path || '/'}
                   onChange={pathInputHandler}/>
          </FormGroup>
          <FormGroup className="mb-2 mr-sm-2">
            <Button tag={Link} to={`/browse?path=${path}${queryStringSuffix}`} color="primary"
                    disabled={path === lastFetched.path}>Go</Button>
          </FormGroup>
        </Form>
        <Table hover={true}>
          <thead>
          <tr>
            <th/>
            <th>File Name</th>
            <th>Block Size</th>
            <th>In-Alluxio</th>
            <th>Mode</th>
            <th>Owner</th>
            <th>Group</th>
            <th>Persistence State</th>
            <th>Pin</th>
            <th>Creation Time</th>
          </tr>
          </thead>
          <tbody>
          {fileInfos && fileInfos.map((fileInfo: IFileInfo) => (
            <tr key={fileInfo.absolutePath}>
              <td><FontAwesomeIcon icon={fileInfo.isDirectory ? faFolder : faFile}/></td>
              <td>
                {this.renderFileNameLink(fileInfo.absolutePath, queryStringSuffix)}
              </td>
              <td>{fileInfo.size}</td>
              <td>{fileInfo.inAlluxio ? 'YES' : 'NO'}</td>
              <td>{fileInfo.mode}</td>
              <td>{fileInfo.owner}</td>
              <td>{fileInfo.group}</td>
              <td>{fileInfo.persistenceState}</td>
              <td>{fileInfo.pinned}</td>
              <td>{fileInfo.creationTime}</td>
            </tr>
          ))}
          </tbody>
        </Table>
        <Paginator baseUrl={'/browse'} path={path} total={browse.ntotalFile} offset={offset} limit={limit}/>
      </React.Fragment>
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
      <Link to={`/browse?path=${filePath}${queryStringSuffix}`}>
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

const mapStateToProps = ({browse, refresh}: IApplicationState) => ({
  browse: browse.browse,
  errors: browse.errors,
  loading: browse.loading,
  refresh: refresh.refresh
});

const mapDispatchToProps = (dispatch: Dispatch) => ({
  fetchRequest: (path?: string, offset?: string, limit?: string, end?: string) => dispatch(fetchRequest(path, offset, limit, end))
});

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(Browse);
