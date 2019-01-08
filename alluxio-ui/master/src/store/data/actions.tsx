import {AxiosResponse} from 'axios';
import {action} from 'typesafe-actions';

import {DataActionTypes} from './types';

export const fetchRequest = (offset?: string, limit?: string) => action(DataActionTypes.FETCH_REQUEST,
  {queryString: {limit, offset}}
);
export const fetchSuccess = (response: AxiosResponse) => action(DataActionTypes.FETCH_SUCCESS, response);
export const fetchError = (message: string) => action(DataActionTypes.FETCH_ERROR, message);
