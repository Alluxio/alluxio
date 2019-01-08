import {AxiosResponse} from 'axios';
import {action} from 'typesafe-actions';

import {MetricsActionTypes} from './types';

export const fetchRequest = () => action(MetricsActionTypes.FETCH_REQUEST);
export const fetchSuccess = (response: AxiosResponse) => action(MetricsActionTypes.FETCH_SUCCESS, response);
export const fetchError = (message: string) => action(MetricsActionTypes.FETCH_ERROR, message);
