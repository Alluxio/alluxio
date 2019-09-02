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
import {IFileBlockInfo, IFileInfo} from '@alluxio/common-ui/src/constants';
import {parseQuerystring, renderFileNameLink} from '@alluxio/common-ui/src/utilities';
import {IApplicationState} from '../../../store';
import {fetchRequest} from '../../../store/blockInfo/actions';
import {IBlockInfo, IFileBlocksOnTier} from '../../../store/blockInfo/types';

interface IPropsFromState {
  data: IBlockInfo;
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

interface IBlockInfoState {
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
}

export type AllProps = IPropsFromState & IPropsFromDispatch;

export class BlockInfo extends React.Component<AllProps, IBlockInfoState> {
  constructor(props: AllProps) {
    super(props);

    const {path, offset, limit, end} = parseQuerystring(this.props.location.search);
    this.state = {end, limit, offset, path: path || '/', lastFetched: {}};
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
        <div className="blockInfo-page">
          <LoadingMessage/>
        </div>
      );
    }

    return (
      <div className="blockInfo-page">
        <div className="container-fluid">
          <div className="row">
            <div className="col-12">
              {data.blockSizeBytes && this.renderBlockInfoView(data)}
              {!data.blockSizeBytes && this.renderBlockInfoListing(data, data.orderedTierAliases, queryStringSuffix)}
            </div>
          </div>
        </div>
      </div>
    );
  }

  private renderBlockInfoView(blockInfo: IBlockInfo) {
    const {path} = this.state;
    return (
      <React.Fragment>
        <h5>{path}</h5>
        <hr/>
        <h6>Blocks on this worker (block capacity is {blockInfo.blockSizeBytes} Bytes):</h6>
        <Table hover={true}>
          <thead>
          <tr>
            <th>ID</th>
            <th>Tier</th>
            <th>Size (Byte)</th>
          </tr>
          </thead>
          <tbody>
          {blockInfo.fileBlocksOnTier.map((fileBlocksOnTier: IFileBlocksOnTier) => {
            return Object.keys(fileBlocksOnTier).map((tierAlias: string) => {
              const fileBlocksDatas: IFileBlockInfo[] = fileBlocksOnTier[tierAlias];
              return fileBlocksDatas.map((fileBlocksData: IFileBlockInfo) => (
                <tr key={fileBlocksData.id}>
                  <td>{fileBlocksData.id}</td>
                  <td>{tierAlias}</td>
                  <td>{fileBlocksData.blockLength}</td>
                </tr>
              ));
            });
          })}
          </tbody>
        </Table>
      </React.Fragment>
    );
  }

  private renderBlockInfoListing(blockInfo: IBlockInfo, tierAliases: string[], queryStringSuffix: string) {
    const fileInfos = blockInfo.fileInfos;
    const {path, offset, limit} = this.state;
    return (
      <React.Fragment>
        <Table hover={true}>
          <thead>
          <tr>
            <th>File Path</th>
            {tierAliases.map((tierAlias: string) => (
              <th key={tierAlias}>in-{tierAlias}</th>
            ))}
            <th>Size</th>
            <th>Creation Time</th>
            <th>Modification Time</th>
          </tr>
          </thead>
          <tbody>
          {fileInfos && fileInfos.map((fileInfo: IFileInfo) => (
            <tr key={fileInfo.absolutePath}>
              <td>{renderFileNameLink.call(this, fileInfo.absolutePath, `/blockInfo?path=`)}</td>
              {tierAliases.map((tierAlias: string) => (
                <td key={tierAlias}>{`${fileInfo.inAlluxioPercentage}%`}</td>
              ))}
              <td>{fileInfo.size}</td>
              <td>{fileInfo.creationTime}</td>
              <td>{fileInfo.modificationTime}</td>
            </tr>
          ))}
          </tbody>
        </Table>
        <Paginator baseUrl={'/blockInfo'} path={path} total={blockInfo.ntotalFile} offset={offset} limit={limit}/>
      </React.Fragment>
    )
  }

  private fetchData(path?: string, offset?: string, limit?: string, end?: string) {
    this.setState({lastFetched: {path, offset, limit, end}});
    this.props.fetchRequest(path, offset, limit, end);
  }
}

const mapStateToProps = ({blockInfo, refresh}: IApplicationState) => ({
  data: blockInfo.data,
  errors: blockInfo.errors,
  loading: blockInfo.loading,
  refresh: refresh.data
});

const mapDispatchToProps = (dispatch: Dispatch) => ({
  fetchRequest: (path?: string, offset?: string, limit?: string, end?: string) => dispatch(fetchRequest(path, offset, limit, end))
});

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(BlockInfo);
