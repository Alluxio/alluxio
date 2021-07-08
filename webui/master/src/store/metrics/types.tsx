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

import { LineSerieData } from '@nivo/line';
import { AxiosResponse } from 'axios';

import { ICounter } from '@alluxio/common-ui/src/constants';
import { IJournalDiskInfo } from '../../constants/types/IJournalDiskInfo';

export interface IMetrics {
  cacheHitLocal: string;
  cacheHitRemote: string;
  cacheMiss: string;
  masterCapacityFreePercentage: number;
  masterCapacityUsedPercentage: number;
  masterUnderfsCapacityFreePercentage: number;
  masterUnderfsCapacityUsedPercentage: number;
  rpcInvocationMetrics: {
    [key: string]: ICounter;
  };
  timeSeriesMetrics: LineSerieData[];
  journalDiskMetrics: IJournalDiskInfo[];
  journalLastCheckpointTime: string;
  journalEntriesSinceCheckpoint: number;
  totalBytesReadLocal: string;
  totalBytesReadLocalThroughput: string;
  totalBytesReadDomainSocket: string;
  totalBytesReadDomainSocketThroughput: string;
  totalBytesReadRemote: string;
  totalBytesReadRemoteThroughput: string;
  totalBytesReadUfs: string;
  totalBytesReadUfsThroughput: string;
  totalBytesWrittenLocal: string;
  totalBytesWrittenLocalThroughput: string;
  totalBytesWrittenRemote: string;
  totalBytesWrittenRemoteThroughput: string;
  totalBytesWrittenDomainSocket: string;
  totalBytesWrittenDomainSocketThroughput: string;
  totalBytesWrittenUfs: string;
  totalBytesWrittenUfsThroughput: string;
  ufsOps: {
    [key: string]: {
      [key: string]: number;
    };
  };
  ufsOpsSaved: {
    [key: string]: {
      [key: string]: number;
    };
  };
  ufsReadSize: {
    [key: string]: string;
  };
  ufsWriteSize: {
    [key: string]: string;
  };
  operationMetrics: {
    [key: string]: ICounter;
  };
}

export enum MetricsActionTypes {
  FETCH_REQUEST = '@@metrics/FETCH_REQUEST',
  FETCH_SUCCESS = '@@metrics/FETCH_SUCCESS',
  FETCH_ERROR = '@@metrics/FETCH_ERROR',
}

export interface IMetricsState {
  readonly data: IMetrics;
  readonly errors?: AxiosResponse;
  readonly loading: boolean;
  readonly response?: AxiosResponse;
}
