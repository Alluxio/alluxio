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

import { IMastersState, MastersActionTypes } from './types';

export const initialMastersState: IMastersState = {
  data: {
    debug: false,
    failedMasterInfos: [],
    normalMasterInfos: [],
    leaderMasterInfo: {
      address: {
        rpcPort: 19998,
        host: '127.0.0.1',
      },
      id: '1',
      lastUpdatedTime: '12-08-2022 23:38:04:639',
      startTime: '12-08-2022 23:37:50:222',
      primacyChangeTime: '12-08-2022 23:37:53:984',
      version: '2.10.0-SNAPSHOT',
      revision: 'a55b99ad3a72321c251f9934662dcc1979c9cb7',
    },
  },
  errors: undefined,
  loading: false,
};

export const mastersReducer: Reducer<IMastersState> = (state = initialMastersState, action) => {
  switch (action.type) {
    case MastersActionTypes.FETCH_REQUEST:
      return { ...state, loading: true };
    case MastersActionTypes.FETCH_SUCCESS:
      return { ...state, loading: false, data: action.payload.data, response: action.payload, errors: undefined };
    case MastersActionTypes.FETCH_ERROR:
      return { ...state, loading: false, errors: action.payload };
    default:
      return state;
  }
};
