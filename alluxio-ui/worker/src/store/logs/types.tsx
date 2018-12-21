// tslint:disable:no-empty-interface // TODO: remove this line

export interface ILogs {
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
