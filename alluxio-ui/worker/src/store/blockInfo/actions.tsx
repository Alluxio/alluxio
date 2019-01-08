import {AxiosResponse} from 'axios';
import {action} from 'typesafe-actions';

import {BlockInfoActionTypes} from './types';

export const fetchRequest = (path?: string, offset?: string, limit?: string, end?: string) => action(BlockInfoActionTypes.FETCH_REQUEST,
  {queryString: {end, limit, offset, path}}
);
export const fetchSuccess = (response: AxiosResponse) => action(BlockInfoActionTypes.FETCH_SUCCESS, response);
export const fetchError = (message: string) => action(BlockInfoActionTypes.FETCH_ERROR, message);
