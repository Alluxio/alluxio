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

import { IInitState, InitActionTypes } from './types';

export const initialInitState: IInitState = {
  data: {
    debug: false,
    newerVersionAvailable: false,
    proxyDownloadFileApiUrl: {
      prefix: 'http://192.168.1.6:39999/api/v1/paths/',
      suffix: '/download-file/',
    },
    refreshInterval: 15000,
    securityAuthorizationPermissionEnabled: false,
    webFileInfoEnabled: false,
    workerPort: 30000,
  },
  errors: undefined,
  loading: false,
};

export const initReducer: Reducer<IInitState> = (state = initialInitState, action) => {
  switch (action.type) {
    case InitActionTypes.FETCH_REQUEST:
      return { ...state, loading: true };
    case InitActionTypes.FETCH_SUCCESS:
      return { ...state, loading: false, data: action.payload.data, response: action.payload, errors: undefined };
    case InitActionTypes.FETCH_ERROR:
      return { ...state, loading: false, errors: action.payload };
    default:
      return state;
  }
};
