import {INodeInfo} from '../../constants';

export interface IWorkers {
  'debug': boolean;
  'normalNodeInfos': INodeInfo[],
  'failedNodeInfos': INodeInfo[]
}

export const enum WorkersActionTypes {
  FETCH_REQUEST = '@@workers/FETCH_REQUEST',
  FETCH_SUCCESS = '@@workers/FETCH_SUCCESS',
  FETCH_ERROR = '@@workers/FETCH_ERROR'
}

export interface IWorkersState {
  readonly loading: boolean;
  readonly workers: IWorkers;
  readonly errors?: string;
}
