import {Reducer} from 'redux';

import {IMetricsState, MetricsActionTypes} from './types';

const initialState: IMetricsState = {
  errors: undefined,
  loading: false,
  metrics: {
    'operationMetrics': {},
    'rpcInvocationMetrics': {},
    'workerCapacityFreePercentage': 0,
    'workerCapacityUsedPercentage': 0
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
