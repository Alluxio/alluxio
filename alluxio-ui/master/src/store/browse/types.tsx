import {IFileBlockInfo, IFileInfo} from '../../constants';

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
  readonly errors?: string;
  readonly loading: boolean;
}
