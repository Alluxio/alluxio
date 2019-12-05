/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

import { Reducer } from 'redux';

import { ConfigActionTypes, IConfigState } from './types';

export const initialConfigState: IConfigState = {
  data: {
    configuration: [],
    whitelist: [],
  },
  errors: undefined,
  loading: false,
};

export const configReducer: Reducer<IConfigState> = (state = initialConfigState, action) => {
  switch (action.type) {
    case ConfigActionTypes.FETCH_REQUEST:
      return { ...state, loading: true };
    case ConfigActionTypes.FETCH_SUCCESS:
      return { ...state, loading: false, data: action.payload.data, response: action.payload, errors: undefined };
    case ConfigActionTypes.FETCH_ERROR:
      return { ...state, loading: false, errors: action.payload };
    default:
      return state;
  }
};
