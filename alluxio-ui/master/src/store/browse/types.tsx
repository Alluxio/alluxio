import {AxiosResponse} from 'axios';

import {IFileBlockInfo, IFileInfo} from '@alluxio/common-ui/src/constants';

export interface IBrowse {
  'accessControlException': string;
  'blockSizeBytes': string;
  'currentDirectory': IFileInfo;
  'currentPath': string;
  'debug': boolean;
  'fatalError': string;
  'fileBlocks': IFileBlockInfo[],
  'fileData': string,
  'fileDoesNotExistException': string;
  'fileInfos': IFileInfo[];
  'highestTierAlias': string,
  'invalidPathError': string;
  'invalidPathException': string;
  'masterNodeAddress': string;
  'ntotalFile': number;
  'pathInfos': IFileInfo[];
  'showPermissions': boolean;
  'viewingOffset': number;
}

export const enum BrowseActionTypes {
  FETCH_REQUEST = '@@browse/FETCH_REQUEST',
  FETCH_SUCCESS = '@@browse/FETCH_SUCCESS',
  FETCH_ERROR = '@@browse/FETCH_ERROR'
}

export interface IBrowseState {
  readonly browse: IBrowse;
  readonly errors?: AxiosResponse;
  readonly loading: boolean;
  readonly response?: AxiosResponse;
}
