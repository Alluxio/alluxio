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

import { IScopedPropertyInfo, IStorageTierInfo } from '../../constants';

export interface IOverview {
  debug: boolean;
  capacity: string;
  clusterId: string;
  configCheckErrors: IScopedPropertyInfo[];
  configCheckStatus: string;
  configCheckWarns: IScopedPropertyInfo[];
  diskCapacity: string;
  diskFreeCapacity: string;
  diskUsedCapacity: string;
  freeCapacity: string;
  journalCheckpointTimeWarning: string;
  journalDiskWarnings: string[];
  liveWorkerNodes: number;
  masterNodeAddress: string;
  startTime: string;
  storageTierInfos: IStorageTierInfo[];
  uptime: string;
  usedCapacity: string;
  version: string;
}

export enum OverviewActionTypes {
  FETCH_REQUEST = '@@overview/FETCH_REQUEST',
  FETCH_SUCCESS = '@@overview/FETCH_SUCCESS',
  FETCH_ERROR = '@@overview/FETCH_ERROR',
}

export interface IOverviewState {
  readonly data: IOverview;
  readonly errors?: AxiosResponse;
  readonly loading: boolean;
  readonly response?: AxiosResponse;
}
