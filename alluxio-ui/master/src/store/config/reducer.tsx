import {Reducer} from 'redux';

import {ConfigActionTypes, IConfigState} from './types';

const initialState: IConfigState = {
  config: {
  },
  errors: undefined,
  loading: false
};

export const configReducer: Reducer<IConfigState> = (state = initialState, action) => {
  switch (action.type) {
    case ConfigActionTypes.FETCH_REQUEST:
      return {...state, loading: true};
    case ConfigActionTypes.FETCH_SUCCESS:
      return {...state, loading: false, config: action.payload};
    case ConfigActionTypes.FETCH_ERROR:
      return {...state, loading: false, errors: action.payload};
    default:
      return state;
  }
};
