import {AxiosResponse} from 'axios';
import {action} from 'typesafe-actions';

import {OverviewActionTypes} from './types';

export const fetchRequest = () => action(OverviewActionTypes.FETCH_REQUEST);
export const fetchSuccess = (response: AxiosResponse) => action(OverviewActionTypes.FETCH_SUCCESS, response);
export const fetchError = (message: string) => action(OverviewActionTypes.FETCH_ERROR, message);
