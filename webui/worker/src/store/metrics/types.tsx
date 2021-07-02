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

import { ICounter } from '@alluxio/common-ui/src/constants';

export interface IMetrics {
  workerCapacityFreePercentage: number;
  workerCapacityUsedPercentage: number;
  operationMetrics: {
    [key: string]: ICounter;
  };
  timeSeriesMetrics: {
    [key: string]: {
      [key: string]: number;
    };
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
