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

import {Reducer} from 'redux';

import {IMetricsState, MetricsActionTypes} from './types';

const initialState: IMetricsState = {
  errors: undefined,
  loading: false,
  metrics: {
    'cacheHitLocal': '',
    'cacheHitRemote': '',
    'cacheMiss': '0.00',
    'masterCapacityFreePercentage': 0,
    'masterCapacityUsedPercentage': 0,
    'masterUnderfsCapacityFreePercentage': 0,
    'masterUnderfsCapacityUsedPercentage': 0,
    'operationMetrics': {},
    'rpcInvocationMetrics': {},
    'totalBytesReadLocal': '',
    'totalBytesReadLocalThroughput': '',
    'totalBytesReadRemote': '',
    'totalBytesReadRemoteThroughput': '',
    'totalBytesReadUfs': '',
    'totalBytesReadUfsThroughput': '',
    'totalBytesWrittenAlluxio': '',
    'totalBytesWrittenAlluxioThroughput': '',
    'totalBytesWrittenUfs': '',
    'totalBytesWrittenUfsThroughput': '',
    'ufsOps': {},
    'ufsReadSize': {},
    'ufsWriteSize': {}
  }
};

export const metricsReducer: Reducer<IMetricsState> = (state = initialState, action) => {
  switch (action.type) {
    case MetricsActionTypes.FETCH_REQUEST:
      return {...state, loading: true};
    case MetricsActionTypes.FETCH_SUCCESS:
      return {...state, loading: false, metrics: action.payload.data, response: action.payload};
    case MetricsActionTypes.FETCH_ERROR:
      return {...state, loading: false, errors: action.payload};
    default:
      return state;
  }
};
