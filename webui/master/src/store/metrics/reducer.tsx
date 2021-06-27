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

import { Reducer } from 'redux';

import { transformToNivoFormat } from '@alluxio/common-ui/src/utilities';
import { IMetricsState, MetricsActionTypes } from './types';
import { LineSerieData } from '@nivo/line';

export const initialMetricsState: IMetricsState = {
  data: {
    cacheHitLocal: '',
    cacheHitRemote: '',
    cacheMiss: '0.00',
    masterCapacityFreePercentage: 0,
    masterCapacityUsedPercentage: 0,
    masterUnderfsCapacityFreePercentage: 0,
    masterUnderfsCapacityUsedPercentage: 0,
    operationMetrics: {},
    rpcInvocationMetrics: {},
    timeSeriesMetrics: [],
    journalDiskMetrics: [],
    journalLastCheckpointTime: '',
    journalEntriesSinceCheckpoint: 0,
    totalBytesReadLocal: '',
    totalBytesReadLocalThroughput: '',
    totalBytesReadDomainSocket: '',
    totalBytesReadDomainSocketThroughput: '',
    totalBytesReadRemote: '',
    totalBytesReadRemoteThroughput: '',
    totalBytesReadUfs: '',
    totalBytesReadUfsThroughput: '',
    totalBytesWrittenLocal: '',
    totalBytesWrittenLocalThroughput: '',
    totalBytesWrittenRemote: '',
    totalBytesWrittenRemoteThroughput: '',
    totalBytesWrittenDomainSocket: '',
    totalBytesWrittenDomainSocketThroughput: '',
    totalBytesWrittenUfs: '',
    totalBytesWrittenUfsThroughput: '',
    ufsOps: {},
    ufsOpsSaved: {},
    ufsReadSize: {},
    ufsWriteSize: {},
  },
  errors: undefined,
  loading: false,
};

export const metricsReducer: Reducer<IMetricsState> = (state = initialMetricsState, action) => {
  const timeSeriesMetrics: LineSerieData[] = [];
  switch (action.type) {
    case MetricsActionTypes.FETCH_REQUEST:
      return { ...state, loading: true };
    case MetricsActionTypes.FETCH_SUCCESS:
      action.payload.data.timeSeriesMetrics.map((item: { name: string; dataPoints: [] }) => {
        timeSeriesMetrics.push({
          id: item.name,
          xAxisLabel: 'Time Stamp',
          yAxisLabel: item.name.includes('%') ? 'Percent (%)' : 'Raw Throughput (Bytes/Minute)',
          data: transformToNivoFormat(item.dataPoints, 'timeStamp', 'value'),
        });
      });
      action.payload.data.timeSeriesMetrics = timeSeriesMetrics;
      return { ...state, loading: false, data: action.payload.data, response: action.payload, errors: undefined };
    case MetricsActionTypes.FETCH_ERROR:
      return { ...state, loading: false, errors: action.payload };
    default:
      return state;
  }
};
