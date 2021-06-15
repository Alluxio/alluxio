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

export interface IMountPointInfo {
  mountPoint: string;
  ufsUri: string;
  ufsType: string;
  ufsCapacityBytes: string;
  ufsUsedBytes: number;
  readOnly: boolean;
  shared: boolean;
  properties: string;
}

export interface IMountTable {
  debug: boolean;
  mountPointInfos: IMountPointInfo[];
}

export enum MountTableActionTypes {
  FETCH_REQUEST = '@@mounttable/FETCH_REQUEST',
  FETCH_SUCCESS = '@@mounttable/FETCH_SUCCESS',
  FETCH_ERROR = '@@mounttable/FETCH_ERROR',
}

export interface IMountTableState {
  readonly data: IMountTable;
  readonly errors?: AxiosResponse;
  readonly loading: boolean;
  readonly response?: AxiosResponse;
}
