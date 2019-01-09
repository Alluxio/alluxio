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

import {AxiosResponse} from 'axios';

import {ICounter} from '@alluxio/common-ui/src/constants';

export interface IMetrics {
  cacheHitLocal: string;
  cacheHitRemote: string;
  cacheMiss: string;
  masterCapacityFreePercentage: number;
  masterCapacityUsedPercentage: number;
  masterUnderfsCapacityFreePercentage: number;
  masterUnderfsCapacityUsedPercentage: number;
  totalBytesReadLocal: string;
  totalBytesReadLocalThroughput: string;
  totalBytesReadRemote: string;
  totalBytesReadRemoteThroughput: string;
  totalBytesReadUfs: string;
  totalBytesReadUfsThroughput: string;
  totalBytesWrittenAlluxio: string;
  totalBytesWrittenAlluxioThroughput: string;
  totalBytesWrittenUfs: string;
  totalBytesWrittenUfsThroughput: string;
  ufsOps: {
    [key: string]: {
      [key: string]: number;
    };
  },
  ufsReadSize: {
    [key: string]: string;
  },
  ufsWriteSize: {
    [key: string]: string;
  },
  operationMetrics: {
    [key: string]: ICounter;
  },
  rpcInvocationMetrics: {
    [key:string]: ICounter;
  }
}

export const enum MetricsActionTypes {
  FETCH_REQUEST = '@@metrics/FETCH_REQUEST',
  FETCH_SUCCESS = '@@metrics/FETCH_SUCCESS',
  FETCH_ERROR = '@@metrics/FETCH_ERROR'
}

export interface IMetricsState {
  readonly loading: boolean;
  readonly metrics: IMetrics;
  readonly errors?: AxiosResponse;
  readonly response?: AxiosResponse;
}
