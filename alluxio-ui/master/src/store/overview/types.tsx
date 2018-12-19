export interface IOverviewStorageTierInfo {
  'capacity': string;
  'freeCapacity': string;
  'freeSpacePercent': number;
  'storageTierAlias': string;
  'usedCapacity': string;
  'usedSpacePercent': number;
}

export type IOverviewScopedPropertyIssue = {
  [scopeKey in 'MASTER' | 'WORKER' | 'CLIENT' | 'SERVER' | 'ALL' | 'NONE']: {// This is derived from 'alluxio/core/common/src/main/java/alluxio/wire/Scope.java'
    [propertyKey: string]: string; // we allow any string as a property key here
  };
}

export interface IOverview {
  'debug': boolean;
  'capacity': {
    'total': number;
    'used': number;
  };
  'configCheckErrors': IOverviewScopedPropertyIssue[];
  'configCheckStatus': string;
  'configCheckWarns': IOverviewScopedPropertyIssue[];
  'consistencyCheckStatus': string;
  'diskCapacity': string;
  'diskFreeCapacity': string;
  'diskUsedCapacity': string;
  'freeCapacity': string;
  'inconsistentPathItems': string[];
  'inconsistentPaths': number;
  'liveWorkerNodes': number;
  'masterNodeAddress': string;
  'startTime': string;
  'storageTierInfos': IOverviewStorageTierInfo[];
  'uptime': string;
  'usedCapacity': string;
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
