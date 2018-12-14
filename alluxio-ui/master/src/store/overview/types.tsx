export interface IOverview {
  'debug': boolean;
  'capacity': {
    'total': number;
    'used': number;
  };
  'configCheckErrorNum': number;
  'configCheckErrors': any | null;
  'configCheckStatus': string[];
  'configCheckWarnNum': number;
  'configCheckWarns': any | null;
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
  'storageTierInfos': any | null;
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
  readonly  overview: IOverview;
  readonly errors?: string;
}
