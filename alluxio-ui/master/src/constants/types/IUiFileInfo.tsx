import {IUiFileBlockInfo} from '..';

export interface IUiFileInfo {
  'absolutePath': string;
  'blockSizeBytes': string;
  'blocksOnTier': {
    [tierAlias: string]: IUiFileBlockInfo[];
  };
  'creationTime': string;
  'fileLocations': string[];
  'group': string;
  'id': number;
  'inAlluxio': boolean;
  'inAlluxioPercentage': number;
  'isDirectory': boolean;
  'mode': string;
  'modificationTime': string;
  'name': string;
  'owner': string;
  'persistenceState': string;
  'pinned': boolean;
  'size': string;
}
