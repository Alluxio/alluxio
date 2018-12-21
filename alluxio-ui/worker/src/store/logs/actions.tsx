import {action} from 'typesafe-actions';

import {ILogs, LogsActionTypes} from './types';

export const fetchRequest = (path?: string, offset?: string, limit?: string, end?: string) => action(LogsActionTypes.FETCH_REQUEST,
  {queryString: {end, limit, offset, path}}
);
export const fetchSuccess = (logs: ILogs) => action(LogsActionTypes.FETCH_SUCCESS, logs);
export const fetchError = (message: string) => action(LogsActionTypes.FETCH_ERROR, message);
