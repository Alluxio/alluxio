import {IFileBlockInfo} from '..';

export interface IFileInfo {
  'absolutePath': string;
  'blockSizeBytes': string;
  'blocksOnTier': {
    [tierAlias: string]: IFileBlockInfo[];
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
