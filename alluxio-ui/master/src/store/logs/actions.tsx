import {action} from 'typesafe-actions';

import {ILogs, LogsActionTypes} from './types';

export const fetchRequest = () => action(LogsActionTypes.FETCH_REQUEST);
export const fetchSuccess = (logs: ILogs) => action(LogsActionTypes.FETCH_SUCCESS, logs);
export const fetchError = (message: string) => action(LogsActionTypes.FETCH_ERROR, message);
