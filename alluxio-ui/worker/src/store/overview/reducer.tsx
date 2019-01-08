import {Reducer} from 'redux';

import {IOverviewState, OverviewActionTypes} from './types';

const initialState: IOverviewState = {
  errors: undefined,
  loading: false,
  overview: {
    'capacityBytes': '',
    'storageDirs': [],
    'usageOnTiers': [],
    'usedBytes': '',
    'version': '',
    'workerInfo': {
      'startTime': '',
      'uptime': '',
      'workerAddress': ''
    }
  }
};

export const overviewReducer: Reducer<IOverviewState> = (state = initialState, action) => {
  switch (action.type) {
    case OverviewActionTypes.FETCH_REQUEST:
      return {...state, loading: true};
    case OverviewActionTypes.FETCH_SUCCESS:
      return {...state, loading: false, overview: action.payload.data, response: action.payload};
    case OverviewActionTypes.FETCH_ERROR:
      return {...state, loading: false, errors: action.payload};
    default:
      return state;
  }
};
