import {action} from 'typesafe-actions';

import {BrowseActionTypes, IBrowse} from './types';

export const fetchRequest = (path: string, offset: number, limit: number, end: boolean) => action(BrowseActionTypes.FETCH_REQUEST, {
  end: end ? '1' : undefined,
  limit: '' + limit,
  offset: '' + offset,
  path
});
export const fetchSuccess = (browse: IBrowse) => action(BrowseActionTypes.FETCH_SUCCESS, browse);
export const fetchError = (message: string) => action(BrowseActionTypes.FETCH_ERROR, message);
