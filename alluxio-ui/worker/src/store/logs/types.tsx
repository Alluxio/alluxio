import {AxiosResponse} from 'axios';

import {IFileInfo} from '@alluxio/common-ui/src/constants';

export interface ILogs {
  'currentPath': string;
  'debug': boolean;
  'fatalError': string;
  'fileData': string;
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
  readonly errors?: AxiosResponse;
  readonly response?: AxiosResponse;
}
