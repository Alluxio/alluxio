// tslint:disable:no-empty-interface // TODO: remove this line

import {IFileInfo} from '../../constants';

export interface ILogs {
  'currentPath': string;
  'debug': boolean;
  'fatalError': string;
  'fileData': null;
  'fileInfos': IFileInfo[];
  'invalidPathError': string;
  'ntotalFile': number;
  'viewingOffset': number;
}

export const enum LogsActionTypes {
  FETCH_REQUEST = '@@logs/FETCH_REQUEST',
  FETCH_SUCCESS = '@@logs/FETCH_SUCCESS',
  FETCH_ERROR = '@@logs/FETCH_ERROR'
}

export interface ILogsState {
  readonly loading: boolean;
  readonly logs: ILogs;
  readonly errors?: string;
}
