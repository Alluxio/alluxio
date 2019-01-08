import {Reducer} from 'redux';

import {IOverviewState, OverviewActionTypes} from './types';

const initialState: IOverviewState = {
  errors: undefined,
  loading: false,
  overview: {
    'capacity': {
      'total': 0,
      'used': 100
    },
    'configCheckErrors': [],
    'configCheckStatus': '',
    'configCheckWarns': [],
    'consistencyCheckStatus': '',
    'debug': false,
    'diskCapacity': '',
    'diskFreeCapacity': '',
    'diskUsedCapacity': '',
    'freeCapacity': '',
    'inconsistentPathItems': [],
    'inconsistentPaths': 0,
    'liveWorkerNodes': 0,
    'masterNodeAddress': '',
    'startTime': '',
    'storageTierInfos': [{
      'capacity': '',
      'freeCapacity': '',
      'freeSpacePercent': 0,
      'storageTierAlias': '',
      'usedCapacity': '',
      'usedSpacePercent': 100
    }],
    'uptime': '',
    'usedCapacity': '',
    'version': ''
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
