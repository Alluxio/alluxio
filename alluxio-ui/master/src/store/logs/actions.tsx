import {action} from 'typesafe-actions';

import {ILogs, LogsActionTypes} from './types';

export const fetchRequest = (path: string, offset: number, limit: number, end: boolean) => action(LogsActionTypes.FETCH_REQUEST, {
  end: end ? '1' : undefined,
  limit: '' + limit,
  offset: '' + offset,
  path
});
export const fetchSuccess = (logs: ILogs) => action(LogsActionTypes.FETCH_SUCCESS, logs);
export const fetchError = (message: string) => action(LogsActionTypes.FETCH_ERROR, message);
