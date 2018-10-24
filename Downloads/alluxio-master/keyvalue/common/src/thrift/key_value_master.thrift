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
  4: i32 keyCount
}

struct CompletePartitionTOptions {}
struct CompletePartitionTResponse {}

struct CompleteStoreTOptions {}
struct CompleteStoreTResponse {}

struct CreateStoreTOptions {}
struct CreateStoreTResponse {}

struct DeleteStoreTOptions {}
struct DeleteStoreTResponse {}

struct GetPartitionInfoTOptions {}
struct GetPartitionInfoTResponse {
  1: list<PartitionInfo> partitionInfo
}

struct MergeStoreTOptions {}
struct MergeStoreTResponse {}

struct RenameStoreTOptions {}
struct RenameStoreTResponse {}

/**
 * This interface contains key-value master service endpoints for Alluxio clients.
 */
service KeyValueMasterClientService extends common.AlluxioService {

  /**
   * Marks a partition complete and adds it to the store.
   */
  CompletePartitionTResponse completePartition(
    /** the path of the store */  1: string path,
    /** information about the partition to mark complete */ 2: PartitionInfo info,
    /** the method options */ 3: CompletePartitionTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Marks a store complete with its filesystem path.
   */
  CompleteStoreTResponse completeStore(
    /** the path of the store */ 1: string path,
    /** the method options */ 2: CompleteStoreTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Creates a new key-value store on master.
   */
  CreateStoreTResponse createStore(
    /** the path of the store */  1: string path,
    /** the method options */ 2: CreateStoreTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Deletes a completed key-value store.
   */
  DeleteStoreTResponse deleteStore(
    /** the path of the store */ 1: string path,
    /** the method options */ 2: DeleteStoreTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Gets the partition information for the key-value store at the given filesystem path.
   */
  GetPartitionInfoTResponse getPartitionInfo(
    /** the path of the store */ 1: string path,
    /** the method options */ 2: GetPartitionInfoTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Merges one completed key-value store to another completed key-value store.
   */
  MergeStoreTResponse mergeStore(
    /** the path of the store to be merged */ 1: string fromPath,
    /** the path of the store to be merged to */ 2: string toPath,
    /** the method options */ 3: MergeStoreTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Renames a completed key-value store.
   */
  RenameStoreTResponse renameStore(
    /** the old path of the store */ 1: string oldPath,
    /** the new path of the store*/ 2:string newPath,
    /** the method options */ 3: RenameStoreTOptions options,
    )
    throws (1: exception.AlluxioTException e)
}
