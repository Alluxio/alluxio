import {AxiosResponse} from 'axios';
import {action} from 'typesafe-actions';

import {BrowseActionTypes} from './types';

export const fetchRequest = (path?: string, offset?: string, limit?: string, end?: string) => action(BrowseActionTypes.FETCH_REQUEST,
  {queryString: {end, limit, offset, path}}
);
export const fetchSuccess = (response: AxiosResponse) => action(BrowseActionTypes.FETCH_SUCCESS, response);
export const fetchError = (message: string) => action(BrowseActionTypes.FETCH_ERROR, message);
