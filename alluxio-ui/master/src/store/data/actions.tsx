import {action} from 'typesafe-actions';

import {DataActionTypes, IData} from './types';

export const fetchRequest = (offset: number, limit: number) => action(DataActionTypes.FETCH_REQUEST, {
  limit: '' + limit,
  offset: '' + offset
});
export const fetchSuccess = (data: IData) => action(DataActionTypes.FETCH_SUCCESS, data);
export const fetchError = (message: string) => action(DataActionTypes.FETCH_ERROR, message);
