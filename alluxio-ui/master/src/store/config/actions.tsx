import {AxiosResponse} from 'axios';
import {action} from 'typesafe-actions';

import {ConfigActionTypes} from './types';

export const fetchRequest = () => action(ConfigActionTypes.FETCH_REQUEST);
export const fetchSuccess = (response: AxiosResponse) => action(ConfigActionTypes.FETCH_SUCCESS, response);
export const fetchError = (message: string) => action(ConfigActionTypes.FETCH_ERROR, message);
