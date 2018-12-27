import {IFileBlockInfo, IFileInfo} from '@alluxio/common-ui/src/constants';

export interface IBlockInfo {
  'blockSizeBytes': string;
  'fatalError': string;
  'fileBlocksOnTier': Array<{
    [tierAlias: string]: IFileBlockInfo[];
  }>;
  'fileInfos': IFileInfo[];
  'invalidPathError': string;
  'ntotalFile': number;
  'orderedTierAliases': string[];
  'path': string;
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
