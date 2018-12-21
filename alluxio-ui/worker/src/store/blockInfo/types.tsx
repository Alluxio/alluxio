// tslint:disable:no-empty-interface // TODO: remove this line

export interface IBlockInfo {
}

export const enum BlockInfoActionTypes {
  FETCH_REQUEST = '@@blockInfo/FETCH_REQUEST',
  FETCH_SUCCESS = '@@blockInfo/FETCH_SUCCESS',
  FETCH_ERROR = '@@blockInfo/FETCH_ERROR'
}

export interface IBlockInfoState {
  readonly blockInfo: IBlockInfo;
  readonly errors?: string;
  readonly loading: boolean;
}
