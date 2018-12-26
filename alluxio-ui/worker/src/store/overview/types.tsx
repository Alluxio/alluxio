import {IStorageTierInfo} from '../../constants';

export interface IOverview {
  'capacityBytes': string;
  'usedBytes': string;
  'workerInfo': {
    'workerAddress': string;
    'startTime': string;
    'uptime': string;
  };
  'usageOnTiers':IStorageTierInfo[];
  'storageDirs': IStorageTierInfo[];
  'version': string;
}

export const enum OverviewActionTypes {
  FETCH_REQUEST = '@@overview/FETCH_REQUEST',
  FETCH_SUCCESS = '@@overview/FETCH_SUCCESS',
  FETCH_ERROR = '@@overview/FETCH_ERROR'
}

export interface IOverviewState {
  readonly loading: boolean;
  readonly overview: IOverview;
  readonly errors?: string;
}
