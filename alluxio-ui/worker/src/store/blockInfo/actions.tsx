import {action} from 'typesafe-actions';

import {BlockInfoActionTypes, IBlockInfo} from './types';

export const fetchRequest = (path?: string, offset?: string, limit?: string, end?: string) => action(BlockInfoActionTypes.FETCH_REQUEST,
  {queryString: {end, limit, offset, path}}
);
export const fetchSuccess = (blockInfo: IBlockInfo) => action(BlockInfoActionTypes.FETCH_SUCCESS, blockInfo);
export const fetchError = (message: string) => action(BlockInfoActionTypes.FETCH_ERROR, message);
