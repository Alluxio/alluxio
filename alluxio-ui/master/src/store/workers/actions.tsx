import {action} from 'typesafe-actions';

import {IWorkers, WorkersActionTypes} from './types';

export const fetchRequest = () => action(WorkersActionTypes.FETCH_REQUEST);
export const fetchSuccess = (workers: IWorkers) => action(WorkersActionTypes.FETCH_SUCCESS, workers);
export const fetchError = (message: string) => action(WorkersActionTypes.FETCH_ERROR, message);
