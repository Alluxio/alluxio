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

import { IOverviewState, OverviewActionTypes } from './types';

export const initialOverviewState: IOverviewState = {
  data: {
    capacity: '',
    configCheckErrors: [],
    configCheckStatus: '',
    configCheckWarns: [],
    debug: false,
    diskCapacity: '',
    diskFreeCapacity: '',
    diskUsedCapacity: '',
    freeCapacity: '',
    liveWorkerNodes: 0,
    masterNodeAddress: '',
    startTime: '',
    storageTierInfos: [
      {
        capacity: '',
        freeCapacity: '',
        freeSpacePercent: 0,
        storageTierAlias: '',
        usedCapacity: '',
        usedSpacePercent: 100
      }
    ],
    uptime: '',
    usedCapacity: '',
    version: ''
  },
  errors: undefined,
  loading: false
};

export const overviewReducer: Reducer<IOverviewState> = (
  state = initialOverviewState,
  action
) => {
  switch (action.type) {
    case OverviewActionTypes.FETCH_REQUEST:
      return { ...state, loading: true };
    case OverviewActionTypes.FETCH_SUCCESS:
      return {
        ...state,
        data: action.payload.data,
        errors: undefined,
        loading: false,
        response: action.payload
      };
    case OverviewActionTypes.FETCH_ERROR:
      return { ...state, loading: false, errors: action.payload };
    default:
      return state;
  }
};
