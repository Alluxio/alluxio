import {Reducer} from 'redux';

import {DataActionTypes, IDataState} from './types';

const initialState: IDataState = {
  data: {
    'fatalError': '',
    'fileInfos': [{
      'absolutePath': '',
      'blockSizeBytes': '',
      'blocksOnTier': {},
      'creationTime': '',
      'fileLocations': [],
      'group': '',
      'id': 0,
      'inAlluxio': false,
      'inAlluxioPercentage': 0,
      'isDirectory': false,
      'mode': '',
      'modificationTime': '',
      'name': 'LICENSE',
      'owner': 'William',
      'persistenceState': '',
      'pinned': false,
      'size': ''
    }],
    'inAlluxioFileNum': 0,
    'masterNodeAddress': '',
    'permissionError': '',
    'showPermissions': true
  },
  errors: undefined,
  loading: false
};

export const dataReducer: Reducer<IDataState> = (state = initialState, action) => {
  switch (action.type) {
    case DataActionTypes.FETCH_REQUEST:
      return {...state, loading: true};
    case DataActionTypes.FETCH_SUCCESS:
      return {...state, loading: false, data: action.payload.data, response: action.payload};
    case DataActionTypes.FETCH_ERROR:
      return {...state, loading: false, errors: action.payload};
    default:
      return state;
  }
};
