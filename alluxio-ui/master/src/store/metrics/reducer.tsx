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
    'masterUnderfsCapacityFreePercentage': 53,
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
      return {...state, loading: false, metrics: action.payload};
    case MetricsActionTypes.FETCH_ERROR:
      return {...state, loading: false, errors: action.payload};
    default:
      return state;
  }
};
