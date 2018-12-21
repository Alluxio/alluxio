import {action} from 'typesafe-actions';

import {ILogs, LogsActionTypes} from './types';

export const fetchRequest = (path?: string, offset?: number, limit?: number, end?: boolean) => action(LogsActionTypes.FETCH_REQUEST, {
  queryString: {
    end: end ? '1' : undefined,
    limit: limit ? '' + limit : undefined,
    offset: offset ? '' + offset : undefined,
    path
  }
});
export const fetchSuccess = (logs: ILogs) => action(LogsActionTypes.FETCH_SUCCESS, logs);
export const fetchError = (message: string) => action(LogsActionTypes.FETCH_ERROR, message);
