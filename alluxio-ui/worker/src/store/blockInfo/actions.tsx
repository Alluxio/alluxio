import {action} from 'typesafe-actions';

import {BlockInfoActionTypes, IBlockInfo} from './types';

export const fetchRequest = () => action(BlockInfoActionTypes.FETCH_REQUEST);
export const fetchSuccess = (blockInfo: IBlockInfo) => action(BlockInfoActionTypes.FETCH_SUCCESS, blockInfo);
export const fetchError = (message: string) => action(BlockInfoActionTypes.FETCH_ERROR, message);
