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

import { DataActionTypes, IDataState } from './types';

export const initialDataState: IDataState = {
  data: {
    fatalError: '',
    fileInfos: [],
    inAlluxioFileNum: 0,
    masterNodeAddress: '',
    permissionError: '',
    showPermissions: true,
  },
  errors: undefined,
  loading: false,
};

export const dataReducer: Reducer<IDataState> = (state = initialDataState, action) => {
  switch (action.type) {
    case DataActionTypes.FETCH_REQUEST:
      return { ...state, loading: true };
    case DataActionTypes.FETCH_SUCCESS:
      return { ...state, loading: false, data: action.payload.data, response: action.payload, errors: undefined };
    case DataActionTypes.FETCH_ERROR:
      return { ...state, loading: false, errors: action.payload };
    default:
      return state;
  }
};
