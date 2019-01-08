import {AxiosResponse} from 'axios';

import {ICounter} from '@alluxio/common-ui/src/constants';

export interface IMetrics {
  workerCapacityFreePercentage: number;
  workerCapacityUsedPercentage: number;
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
