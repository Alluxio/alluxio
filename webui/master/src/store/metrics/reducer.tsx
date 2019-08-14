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

import {transformToNivoFormat} from '@alluxio/common-ui/src/utilities';
import {IMetricsState, MetricsActionTypes} from './types';
import {LineSerieData} from '@nivo/line';

export const initialMetricsState: IMetricsState = {
  data: {
    'cacheHitLocal': '',
    'cacheHitRemote': '',
    'cacheMiss': '0.00',
    'masterCapacityFreePercentage': 0,
    'masterCapacityUsedPercentage': 0,
    'masterUnderfsCapacityFreePercentage': 0,
    'masterUnderfsCapacityUsedPercentage': 0,
    'operationMetrics': {},
    'rpcInvocationMetrics': {},
    'timeSeriesMetrics': [],
    'totalBytesReadLocal': '',
    'totalBytesReadLocalThroughput': '',
    'totalBytesReadDomainSocket': '',
    'totalBytesReadDomainSocketThroughput': '',
    'totalBytesReadRemote': '',
    'totalBytesReadRemoteThroughput': '',
    'totalBytesReadUfs': '',
    'totalBytesReadUfsThroughput': '',
    'totalBytesWrittenAlluxio': '',
    'totalBytesWrittenAlluxioThroughput': '',
    'totalBytesWrittenDomainSocket': '',
    'totalBytesWrittenDomainSocketThroughput': '',
    'totalBytesWrittenUfs': '',
    'totalBytesWrittenUfsThroughput': '',
    'ufsOps': {},
    'ufsReadSize': {},
    'ufsWriteSize': {}
  },
  errors: undefined,
  loading: false
};

export const metricsReducer: Reducer<IMetricsState> = (state = initialMetricsState, action) => {
  switch (action.type) {
    case MetricsActionTypes.FETCH_REQUEST:
      return {...state, loading: true};
    case MetricsActionTypes.FETCH_SUCCESS:
      const timeSeriesMetrics: LineSerieData[] = [];
      action.payload.data.timeSeriesMetrics.map((item: any) => {
        // only push the latest 20 points of data
        timeSeriesMetrics.push({
          id: item.name,
          xAxisLabel: 'Time Stamp',
          yAxisLabel: 'Percent (%)',
          data: transformToNivoFormat(item.dataPoints.splice(0,24), 'timeStamp', 'value')
        });
      });
      action.payload.data.timeSeriesMetrics = timeSeriesMetrics;
      return {...state, loading: false, data: action.payload.data, response: action.payload, errors: undefined};
    case MetricsActionTypes.FETCH_ERROR:
      return {...state, loading: false, errors: action.payload};
    default:
      return state;
  }
};
