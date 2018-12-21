// tslint:disable:no-empty-interface // TODO: remove this line

export interface IOverview {
}

export const enum OverviewActionTypes {
  FETCH_REQUEST = '@@overview/FETCH_REQUEST',
  FETCH_SUCCESS = '@@overview/FETCH_SUCCESS',
  FETCH_ERROR = '@@overview/FETCH_ERROR'
}

export interface IOverviewState {
  readonly loading: boolean;
  readonly overview: IOverview;
  readonly errors?: string;
}
