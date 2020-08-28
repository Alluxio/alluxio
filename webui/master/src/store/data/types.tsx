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

import { IFileInfo } from '@alluxio/common-ui/src/constants';

export interface IData {
  fatalError: string;
  fileInfos: IFileInfo[];
  inAlluxioFileNum: number;
  masterNodeAddress: string;
  permissionError: string;
  showPermissions: boolean;
}

export enum DataActionTypes {
  FETCH_REQUEST = '@@data/FETCH_REQUEST',
  FETCH_SUCCESS = '@@data/FETCH_SUCCESS',
  FETCH_ERROR = '@@data/FETCH_ERROR',
}

export interface IDataState {
  readonly data: IData;
  readonly loading: boolean;
  readonly errors?: AxiosResponse;
  readonly response?: AxiosResponse;
}
