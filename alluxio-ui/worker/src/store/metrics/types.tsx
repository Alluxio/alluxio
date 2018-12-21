// tslint:disable:no-empty-interface // TODO: remove this line

export interface IMetrics {
}

export const enum MetricsActionTypes {
  FETCH_REQUEST = '@@metrics/FETCH_REQUEST',
  FETCH_SUCCESS = '@@metrics/FETCH_SUCCESS',
  FETCH_ERROR = '@@metrics/FETCH_ERROR'
}

export interface IMetricsState {
  readonly loading: boolean;
  readonly metrics: IMetrics;
  readonly errors?: string;
}
