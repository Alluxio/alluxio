import {AxiosResponse} from 'axios';
import {action} from 'typesafe-actions';

import {WorkersActionTypes} from './types';

export const fetchRequest = () => action(WorkersActionTypes.FETCH_REQUEST);
export const fetchSuccess = (response: AxiosResponse) => action(WorkersActionTypes.FETCH_SUCCESS, response);
export const fetchError = (message: string) => action(WorkersActionTypes.FETCH_ERROR, message);
