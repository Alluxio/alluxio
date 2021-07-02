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

import { AxiosResponse } from 'axios';

import { IFileBlockInfo, IFileInfo } from '@alluxio/common-ui/src/constants';

export interface IFileBlocksOnTier {
  [tierAlias: string]: IFileBlockInfo[];
}

export interface IBlockInfo {
  blockSizeBytes: string;
  fatalError: string;
  fileBlocksOnTier: IFileBlocksOnTier[];
  fileInfos: IFileInfo[];
  invalidPathError: string;
  ntotalFile: number;
  orderedTierAliases: string[];
  path: string;
}

export enum BlockInfoActionTypes {
  FETCH_REQUEST = '@@blockInfo/FETCH_REQUEST',
  FETCH_SUCCESS = '@@blockInfo/FETCH_SUCCESS',
  FETCH_ERROR = '@@blockInfo/FETCH_ERROR',
}

export interface IBlockInfoState {
  readonly data: IBlockInfo;
  readonly errors?: AxiosResponse;
  readonly loading: boolean;
  readonly response?: AxiosResponse;
}
