// tslint:disable:no-empty-interface // TODO: remove this line

export interface IConfig {
}

export const enum ConfigActionTypes {
  FETCH_REQUEST = '@@config/FETCH_REQUEST',
  FETCH_SUCCESS = '@@config/FETCH_SUCCESS',
  FETCH_ERROR = '@@config/FETCH_ERROR'
}

export interface IConfigState {
  readonly loading: boolean;
  readonly config: IConfig;
  readonly errors?: string;
}
