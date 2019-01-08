import {AxiosResponse} from 'axios';

import {IConfigTriple} from '../../constants';

export interface IConfig {
  configuration: IConfigTriple[];
  whitelist: string[];
}

export const enum ConfigActionTypes {
  FETCH_REQUEST = '@@config/FETCH_REQUEST',
  FETCH_SUCCESS = '@@config/FETCH_SUCCESS',
  FETCH_ERROR = '@@config/FETCH_ERROR'
}

export interface IConfigState {
  readonly loading: boolean;
  readonly config: IConfig;
  readonly errors?: AxiosResponse;
  readonly response?: AxiosResponse;
}
