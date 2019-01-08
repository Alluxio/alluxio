import {Reducer} from 'redux';

import {ILogsState, LogsActionTypes} from './types';

const initialState: ILogsState = {
  errors: undefined,
  loading: false,
  logs: {
    'currentPath': '',
    'debug': false,
    'fatalError': '',
    'fileData': '',
    'fileInfos': [],
    'invalidPathError': '',
    'ntotalFile': 0,
    'viewingOffset': 0
  }
};

export const logsReducer: Reducer<ILogsState> = (state = initialState, action) => {
  switch (action.type) {
    case LogsActionTypes.FETCH_REQUEST:
      return {...state, loading: true};
    case LogsActionTypes.FETCH_SUCCESS:
      return {...state, loading: false, logs: action.payload.data, response: action.payload};
    case LogsActionTypes.FETCH_ERROR:
      return {...state, loading: false, errors: action.payload};
    default:
      return state;
  }
};
