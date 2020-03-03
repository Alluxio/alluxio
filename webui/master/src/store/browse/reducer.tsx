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

import { BrowseActionTypes, IBrowseState } from './types';

export const initialBrowseState: IBrowseState = {
  data: {
    accessControlException: '',
    blockSizeBytes: '',
    currentDirectory: {
      absolutePath: '',
      blockSizeBytes: '',
      blocksOnTier: {},
      creationTime: '',
      fileLocations: [],
      group: '',
      id: 0,
      inAlluxio: false,
      inAlluxioPercentage: 0,
      isDirectory: true,
      mode: '',
      modificationTime: '',
      name: '',
      owner: '',
      persistenceState: '',
      pinned: false,
      size: '',
    },
    currentPath: '',
    debug: false,
    fatalError: '',
    fileBlocks: [],
    fileData: '',
    fileDoesNotExistException: '',
    fileInfos: [],
    highestTierAlias: '',
    invalidPathError: '',
    invalidPathException: '',
    masterNodeAddress: '',
    ntotalFile: 0,
    pathInfos: [],
    showPermissions: true,
    viewingOffset: 0,
  },
  errors: undefined,
  loading: false,
};

export const browseReducer: Reducer<IBrowseState> = (state = initialBrowseState, action) => {
  switch (action.type) {
    case BrowseActionTypes.FETCH_REQUEST:
      return { ...state, loading: true };
    case BrowseActionTypes.FETCH_SUCCESS:
      return { ...state, loading: false, data: action.payload.data, response: action.payload, errors: undefined };
    case BrowseActionTypes.FETCH_ERROR:
      return { ...state, loading: false, errors: action.payload };
    default:
      return state;
  }
};
