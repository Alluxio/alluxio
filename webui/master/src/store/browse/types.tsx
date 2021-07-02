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

export interface IBrowse {
  accessControlException: string;
  blockSizeBytes: string;
  currentDirectory: IFileInfo;
  currentPath: string;
  debug: boolean;
  fatalError: string;
  fileBlocks: IFileBlockInfo[];
  fileData: string;
  fileDoesNotExistException: string;
  fileInfos: IFileInfo[];
  highestTierAlias: string;
  invalidPathError: string;
  invalidPathException: string;
  masterNodeAddress: string;
  ntotalFile: number;
  pathInfos: IFileInfo[];
  showPermissions: boolean;
  viewingOffset: number;
}

export enum BrowseActionTypes {
  FETCH_REQUEST = '@@browse/FETCH_REQUEST',
  FETCH_SUCCESS = '@@browse/FETCH_SUCCESS',
  FETCH_ERROR = '@@browse/FETCH_ERROR',
}

export interface IBrowseState {
  readonly data: IBrowse;
  readonly errors?: AxiosResponse;
  readonly loading: boolean;
  readonly response?: AxiosResponse;
}
