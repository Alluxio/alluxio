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
import {Alert, Table} from 'reactstrap';
import {Dispatch} from 'redux';

import {LoadingMessage, Paginator} from '@alluxio/common-ui/src/components';
import {IFileInfo} from '@alluxio/common-ui/src/constants';
import {parseQuerystring} from '@alluxio/common-ui/src/utilities';
import {IApplicationState} from '../../../store';
import {fetchRequest} from '../../../store/data/actions';
import {IData} from '../../../store/data/types';

interface IPropsFromState {
  data: IData;
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

interface IDataState {
  limit?: string;
  offset?: string;
  lastFetched: {
    limit?: string;
    offset?: string;
  }
}

export type AllProps = IPropsFromState & IPropsFromDispatch;

export class Data extends React.Component<AllProps, IDataState> {
  constructor(props: AllProps) {
    super(props);

    const {offset, limit} = parseQuerystring(this.props.location.search);
    this.state = {offset, limit, lastFetched: {}};
  }

  public componentDidUpdate(prevProps: AllProps) {
    const {refresh, location: {search}} = this.props;
    const {refresh: prevRefresh, location: {search: prevSearch}} = prevProps;
    if (search !== prevSearch) {
      const {offset, limit} = parseQuerystring(search);
      this.setState({offset, limit});
      this.fetchData(offset, limit);
    } else if (refresh !== prevRefresh) {
      const {offset, limit} = this.state;
      this.fetchData(offset, limit);
    }
  }

  public componentWillMount() {
    const {offset, limit} = this.state;
    this.fetchData(offset, limit);
  }

  public render() {
    const {offset, limit} = this.state;
    const {data, errors, loading} = this.props;

    if (errors || data.permissionError || data.fatalError) {
      return (
        <Alert color="danger">
          {errors && <div>Unable to reach the api endpoint for this page.</div>}
          {data.permissionError && <div>{data.permissionError}</div>}
          {data.fatalError && <div>{data.fatalError}</div>}
        </Alert>
      );
    }

    if (loading) {
      return (
        <div className="h-100 w-100 data-page">
          <LoadingMessage/>
        </div>
      );
    }

    return (
      <div className="data-page">
        <div className="container-fluid">
          <div className="row">
            <div className="col-12">
              {this.renderFileListing(data.fileInfos)}
              <Paginator baseUrl={'/data'} total={data.inAlluxioFileNum} offset={offset} limit={limit}/>
            </div>
          </div>
        </div>
      </div>
    );
  }

  private renderFileListing(fileInfos: IFileInfo[]) {
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
        {fileInfos && fileInfos.map((fileInfo: IFileInfo) => (
          <tr key={fileInfo.absolutePath}>
            <td>{fileInfo.absolutePath}</td>
            <td>{fileInfo.size}</td>
            <td>{fileInfo.blockSizeBytes}</td>
            <td>
              <pre className="mb-0"><code>{fileInfo.mode}</code></pre>
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
    )
  }

  private fetchData(offset?: string, limit?: string) {
    this.setState({lastFetched: {offset, limit}});
    this.props.fetchRequest(offset, limit);
  }
}

const mapStateToProps = ({data, refresh}: IApplicationState) => ({
  data: data.data,
  errors: data.errors,
  loading: data.loading,
  refresh: refresh.data
});

const mapDispatchToProps = (dispatch: Dispatch) => ({
  fetchRequest: (offset?: string, limit?: string) => dispatch(fetchRequest(offset, limit))
});

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(Data);
