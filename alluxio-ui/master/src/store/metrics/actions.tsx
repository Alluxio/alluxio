import {action} from 'typesafe-actions';

import {IMetrics, MetricsActionTypes} from './types';

export const fetchRequest = () => action(MetricsActionTypes.FETCH_REQUEST);
export const fetchSuccess = (metrics: IMetrics) => action(MetricsActionTypes.FETCH_SUCCESS, metrics);
export const fetchError = (message: string) => action(MetricsActionTypes.FETCH_ERROR, message);
