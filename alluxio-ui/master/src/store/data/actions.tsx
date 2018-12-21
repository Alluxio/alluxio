import {action} from 'typesafe-actions';

import {DataActionTypes, IData} from './types';

export const fetchRequest = (offset?: string, limit?: string) => action(DataActionTypes.FETCH_REQUEST,
  {queryString: {limit, offset}}
);
export const fetchSuccess = (data: IData) => action(DataActionTypes.FETCH_SUCCESS, data);
export const fetchError = (message: string) => action(DataActionTypes.FETCH_ERROR, message);
