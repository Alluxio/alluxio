import {action} from 'typesafe-actions';

import {ConfigActionTypes, IConfig} from './types';

export const fetchRequest = () => action(ConfigActionTypes.FETCH_REQUEST);
export const fetchSuccess = (config: IConfig) => action(ConfigActionTypes.FETCH_SUCCESS, config);
export const fetchError = (message: string) => action(ConfigActionTypes.FETCH_ERROR, message);
