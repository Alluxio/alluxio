import {IUiFileBlockInfo, IUiFileInfo} from '../../constants';

export interface IBrowse {
  'accessControlException': string;
  'blockSizeBytes': string;
  'currentDirectory': IUiFileInfo;
  'currentPath': string;
  'debug': boolean;
  'fatalError': string;
  'fileBlocks': IUiFileBlockInfo[],
  'fileData': string,
  'fileDoesNotExistException': string;
  'fileInfos': IUiFileInfo[];
  'highestTierAlias': string,
  'invalidPathError': string;
  'invalidPathException': string;
  'masterNodeAddress': string;
  'ntotalFile': number;
  'pathInfos': IUiFileInfo[];
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
