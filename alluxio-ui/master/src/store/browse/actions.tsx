import {action} from 'typesafe-actions';

import {BrowseActionTypes, IBrowse} from './types';

export const fetchRequest = () => action(BrowseActionTypes.FETCH_REQUEST);
export const fetchSuccess = (browse: IBrowse) => action(BrowseActionTypes.FETCH_SUCCESS, browse);
export const fetchError = (message: string) => action(BrowseActionTypes.FETCH_ERROR, message);
