import {action} from 'typesafe-actions';

import {BrowseActionTypes, IBrowse} from './types';

export const fetchRequest = (path?: string, offset?: string, limit?: string, end?: string) => action(BrowseActionTypes.FETCH_REQUEST,
  {queryString: {end, limit, offset, path}}
);
export const fetchSuccess = (browse: IBrowse) => action(BrowseActionTypes.FETCH_SUCCESS, browse);
export const fetchError = (message: string) => action(BrowseActionTypes.FETCH_ERROR, message);
