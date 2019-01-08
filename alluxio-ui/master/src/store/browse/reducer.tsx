import {Reducer} from 'redux';

import {BrowseActionTypes, IBrowseState} from './types';

const initialState: IBrowseState = {
  browse: {
    'accessControlException': '',
    'blockSizeBytes': '',
    'currentDirectory': {
      'absolutePath': '',
      'blockSizeBytes': '',
      'blocksOnTier': {},
      'creationTime': '',
      'fileLocations': [],
      'group': '',
      'id': 0,
      'inAlluxio': false,
      'inAlluxioPercentage': 0,
      'isDirectory': true,
      'mode': '',
      'modificationTime': '',
      'name': '',
      'owner': '',
      'persistenceState': '',
      'pinned': false,
      'size': ''
    },
    'currentPath': '',
    'debug': false,
    'fatalError': '',
    'fileBlocks': [],
    'fileData': '',
    'fileDoesNotExistException': '',
    'fileInfos': [],
    'highestTierAlias': '',
    'invalidPathError': '',
    'invalidPathException': '',
    'masterNodeAddress': '',
    'ntotalFile': 0,
    'pathInfos': [],
    'showPermissions': true,
    'viewingOffset': 0
  },
  errors: undefined,
  loading: false,
};

export const browseReducer: Reducer<IBrowseState> = (state = initialState, action) => {
  switch (action.type) {
    case BrowseActionTypes.FETCH_REQUEST:
      return {...state, loading: true};
    case BrowseActionTypes.FETCH_SUCCESS:
      return {...state, loading: false, browse: action.payload.data, response: action.payload};
    case BrowseActionTypes.FETCH_ERROR:
      return {...state, loading: false, errors: action.payload};
    default:
      return state;
  }
};
