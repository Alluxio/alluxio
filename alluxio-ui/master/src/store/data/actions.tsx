import {action} from 'typesafe-actions';

import {DataActionTypes, IData} from './types';

export const fetchRequest = (offset?: number, limit?: number) => action(DataActionTypes.FETCH_REQUEST, {
  queryString: {
    limit: limit ? '' + limit : undefined,
    offset: offset ? '' + offset : undefined
  }
});
export const fetchSuccess = (data: IData) => action(DataActionTypes.FETCH_SUCCESS, data);
export const fetchError = (message: string) => action(DataActionTypes.FETCH_ERROR, message);
