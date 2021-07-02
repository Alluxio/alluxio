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

export interface IInit {
  debug: boolean;
  newerVersionAvailable: boolean;
  proxyDownloadFileApiUrl: {
    prefix: string;
    suffix: string;
  };
  refreshInterval: number;
  securityAuthorizationPermissionEnabled: boolean;
  webFileInfoEnabled: boolean;
  workerPort: number;
}

export enum InitActionTypes {
  FETCH_REQUEST = '@@init/FETCH_REQUEST',
  FETCH_SUCCESS = '@@init/FETCH_SUCCESS',
  FETCH_ERROR = '@@init/FETCH_ERROR',
}

export interface IInitState {
  readonly data: IInit;
  readonly errors?: AxiosResponse;
  readonly loading: boolean;
  readonly response?: AxiosResponse;
}
