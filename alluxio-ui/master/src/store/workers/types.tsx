// tslint:disable:no-empty-interface // TODO: remove this line

export interface IWorkers {
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
