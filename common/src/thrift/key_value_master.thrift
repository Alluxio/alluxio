namespace java tachyon.thrift

include "common.thrift"
include "exception.thrift"

struct PartitionInfo {
  1: binary keyStart
  2: binary keyLimit
  3: i64 blockId
}

/**
 * This interface contains key-value master service endpoints for Tachyon clients.
 */
service KeyValueMasterClientService extends common.TachyonService {

  /**
   * Creates a new key-value store on master.
   */
  void createStore( /** the path of the store */  1: string path)
    throws (1: exception.TachyonTException e)

  /**
   * Marks a partition complete and add it to the store.
   */
  void completePartition( /** the path of the store */  1: string path,
      /** the path of the store */ 2: PartitionInfo info)
    throws (1: exception.TachyonTException e)

  /**
   * Marks a store complete with its filesystem path.
   */
  void completeStore( /** the path of the store */ 1: string path)
    throws (1: exception.TachyonTException e)

  /**
   * Gets a list of partition information given a filesystem path.
   */
  list<PartitionInfo> getPartitionInfo( /** the path of the store */ 1: string path)
    throws (1: exception.TachyonTException e)
}
