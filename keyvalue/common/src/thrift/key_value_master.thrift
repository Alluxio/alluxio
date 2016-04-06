namespace java alluxio.thrift

include "common.thrift"
include "exception.thrift"

/**
 * Information about a key-value partition.
 */
struct PartitionInfo {
  1: binary keyStart
  2: binary keyLimit
  3: i64 blockId
}

/**
 * This interface contains key-value master service endpoints for Alluxio clients.
 */
service KeyValueMasterClientService extends common.AlluxioService {

  /**
   * Marks a partition complete and adds it to the store.
   */
  void completePartition( /** the path of the store */  1: string path,
      /** information about the partition to mark complete */ 2: PartitionInfo info)
    throws (1: exception.AlluxioTException e)

  /**
   * Marks a store complete with its filesystem path.
   */
  void completeStore( /** the path of the store */ 1: string path)
    throws (1: exception.AlluxioTException e)

  /**
   * Creates a new key-value store on master.
   */
  void createStore( /** the path of the store */  1: string path)
    throws (1: exception.AlluxioTException e)

  /**
   * Gets the partition information for the key-value store at the given filesystem path.
   */
  list<PartitionInfo> getPartitionInfo( /** the path of the store */ 1: string path)
    throws (1: exception.AlluxioTException e)

  /**
   * Deletes a completed key-value store.
   */
  void deleteStore( /** the path of the store */ 1: string path)
    throws (1: exception.AlluxioTException e, 2: exception.ThriftIOException ioe)

  /**
   * Renames a completed key-value store.
   */
  void renameStore( /** the old path of the store */ 1: string oldPath,
      /**the new path of the store*/ 2:string newPath)
    throws (1: exception.AlluxioTException e, 2: exception.ThriftIOException ioe)

  /**
   * Merges one completed key-value store to another completed key-value store.
   */
  void mergeStore( /** the path of the store to be merged */ 1: string fromPath,
      /** the path of the store to be merged to */ 2: string toPath)
    throws (1: exception.AlluxioTException e, 2: exception.ThriftIOException ioe)
}
