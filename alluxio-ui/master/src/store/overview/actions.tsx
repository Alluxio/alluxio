import {action} from 'typesafe-actions';

import {IOverview, OverviewActionTypes} from './types';

export const fetchRequest = () => action(OverviewActionTypes.FETCH_REQUEST);
export const fetchSuccess = (overview: IOverview) => action(OverviewActionTypes.FETCH_SUCCESS, overview);
export const fetchError = (message: string) => action(OverviewActionTypes.FETCH_ERROR, message);
