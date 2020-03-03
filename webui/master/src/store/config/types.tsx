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

import { IConfigTriple } from '../../constants';

export interface IConfig {
  configuration: IConfigTriple[];
  whitelist: string[];
}

export enum ConfigActionTypes {
  FETCH_REQUEST = '@@config/FETCH_REQUEST',
  FETCH_SUCCESS = '@@config/FETCH_SUCCESS',
  FETCH_ERROR = '@@config/FETCH_ERROR',
}

export interface IConfigState {
  readonly data: IConfig;
  readonly loading: boolean;
  readonly errors?: AxiosResponse;
  readonly response?: AxiosResponse;
}
