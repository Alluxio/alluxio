import {Reducer} from 'redux';

import {IWorkersState, WorkersActionTypes} from './types';

const initialState: IWorkersState = {
  errors: undefined,
  loading: false,
  workers: {
    'debug': false,
    'failedNodeInfos': [],
    'normalNodeInfos': []
  }
};

export const workersReducer: Reducer<IWorkersState> = (state = initialState, action) => {
  switch (action.type) {
    case WorkersActionTypes.FETCH_REQUEST:
      return {...state, loading: true};
    case WorkersActionTypes.FETCH_SUCCESS:
      return {...state, loading: false, workers: action.payload};
    case WorkersActionTypes.FETCH_ERROR:
      return {...state, loading: false, errors: action.payload};
    default:
      return state;
  }
};
