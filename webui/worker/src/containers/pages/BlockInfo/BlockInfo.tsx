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
import { IAlertErrors, ICommonState, IFileBlockInfo, IFileInfo, IRequest } from '@alluxio/common-ui/src/constants';
import { createAlertErrors, renderFileNameLink } from '@alluxio/common-ui/src/utilities';
import { IApplicationState } from '../../../store';
import { fetchRequest } from '../../../store/blockInfo/actions';
import { IBlockInfo, IFileBlocksOnTier } from '../../../store/blockInfo/types';
import { routePaths } from '../../../constants';

interface IPropsFromState extends ICommonState {
  data: IBlockInfo;
}

interface IPropsFromDispatch {
  fetchRequest: typeof fetchRequest;
}

interface IBlockInfoProps {
  request: IRequest;
}

export type AllProps = IPropsFromState & IPropsFromDispatch & IBlockInfoProps;

export class BlockInfoPresenter extends React.Component<AllProps> {
  public render(): JSX.Element {
    const { data } = this.props;

    return (
      <div className="col-12">
        {data.blockSizeBytes
          ? this.renderBlockInfoView(data)
          : this.renderBlockInfoListing(data, data.orderedTierAliases)}
      </div>
    );
  }

  private renderBlockInfoView(blockInfo: IBlockInfo): JSX.Element {
    const {
      request: { path },
    } = this.props;
    return (
      <React.Fragment>
        <h5>{path}</h5>
        <hr />
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

  private renderBlockInfoListing(blockInfo: IBlockInfo, tierAliases: string[]): JSX.Element {
    const fileInfos = blockInfo.fileInfos;
    const {
      request: { path, limit, offset },
    } = this.props;
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
            {fileInfos &&
              fileInfos.map((fileInfo: IFileInfo) => (
                <tr key={fileInfo.absolutePath}>
                  <td>{renderFileNameLink(fileInfo.absolutePath, `/blockInfo?path=`)}</td>
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
        <Paginator
          baseUrl={routePaths.blockInfo}
          path={path}
          total={blockInfo.ntotalFile}
          offset={offset}
          limit={limit}
        />
      </React.Fragment>
    );
  }
}

const mapStateToProps = ({ blockInfo, refresh }: IApplicationState): IPropsFromState => {
  const errors: IAlertErrors = createAlertErrors(blockInfo.errors != undefined, [
    blockInfo.data.invalidPathError,
    blockInfo.data.fatalError,
  ]);
  return {
    data: blockInfo.data,
    errors: errors,
    loading: blockInfo.loading,
    refresh: refresh.data,
    class: 'blockInfo-page',
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
)(BlockInfoPresenter) as typeof React.Component;
