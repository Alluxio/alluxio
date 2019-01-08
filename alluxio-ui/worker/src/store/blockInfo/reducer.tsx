import {Reducer} from 'redux';

import {BlockInfoActionTypes, IBlockInfoState} from './types';

const initialState: IBlockInfoState = {
  blockInfo: {
    'blockSizeBytes': '',
    'fatalError': '',
    'fileBlocksOnTier': [],
    'fileInfos': [],
    'invalidPathError': '',
    'ntotalFile': 0,
    'orderedTierAliases': [],
    'path': ''
  },
  errors: undefined,
  loading: false
};

export const blockInfoReducer: Reducer<IBlockInfoState> = (state = initialState, action) => {
  switch (action.type) {
    case BlockInfoActionTypes.FETCH_REQUEST:
      return {...state, loading: true};
    case BlockInfoActionTypes.FETCH_SUCCESS:
      return {...state, loading: false, blockInfo: action.payload.data, response: action.payload};
    case BlockInfoActionTypes.FETCH_ERROR:
      return {...state, loading: false, errors: action.payload};
    default:
      return state;
  }
};
