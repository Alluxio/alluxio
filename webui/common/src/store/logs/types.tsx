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

import { IFileInfo } from '../../constants';

export interface ILogs {
  currentPath: string;
  debug: boolean;
  fatalError: string;
  fileData: string | null;
  fileInfos: IFileInfo[] | null;
  invalidPathError: string;
  ntotalFile: number;
  viewingOffset: number;
}

export enum LogsActionTypes {
  FETCH_REQUEST = '@@logs/FETCH_REQUEST',
  FETCH_SUCCESS = '@@logs/FETCH_SUCCESS',
  FETCH_ERROR = '@@logs/FETCH_ERROR',
}

export interface ILogsState {
  readonly data: ILogs;
  readonly errors?: AxiosResponse;
  readonly loading: boolean;
  readonly response?: AxiosResponse;
}
