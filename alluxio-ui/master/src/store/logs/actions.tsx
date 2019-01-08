import {AxiosResponse} from 'axios';
import {action} from 'typesafe-actions';

import {LogsActionTypes} from './types';

export const fetchRequest = (path?: string, offset?: string, limit?: string, end?: string) => action(LogsActionTypes.FETCH_REQUEST,
  {queryString: {end, limit, offset, path}}
);
export const fetchSuccess = (response: AxiosResponse) => action(LogsActionTypes.FETCH_SUCCESS, response);
export const fetchError = (message: string) => action(LogsActionTypes.FETCH_ERROR, message);
