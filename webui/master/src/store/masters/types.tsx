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

import { IMasterInfo } from '../../constants';

export interface IMasters {
  debug: boolean;
  lostMasterInfos: IMasterInfo[];
  standbyMasterInfos: IMasterInfo[];
  primaryMasterInfo: IMasterInfo;
}

export enum MastersActionTypes {
  FETCH_REQUEST = '@@masters/FETCH_REQUEST',
  FETCH_SUCCESS = '@@masters/FETCH_SUCCESS',
  FETCH_ERROR = '@@masters/FETCH_ERROR',
}

export interface IMastersState {
  readonly data: IMasters;
  readonly errors?: AxiosResponse;
  readonly loading: boolean;
  readonly response?: AxiosResponse;
}
