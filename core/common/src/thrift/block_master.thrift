namespace java alluxio.thrift

include "common.thrift"
include "exception.thrift"

struct WorkerInfo {
  1: i64 id
  2: common.WorkerNetAddress address
  3: i32 lastContactSec
  4: string state
  5: i64 capacityBytes
  6: i64 usedBytes
  7: i64 startTimeMs
}

/**
 * This interface contains block master service endpoints for Alluxio clients.
 */
service BlockMasterClientService extends common.AlluxioService {

  /**
   * Returns the block information for the given block id.
   */
  common.BlockInfo getBlockInfo( /** the id of the block */  1: i64 blockId)
    throws (1: exception.AlluxioTException e)

  /**
   * Returns the capacity (in bytes).
   */
  i64 getCapacityBytes()

  /**
   * Returns the used storage (in bytes).
   */
  i64 getUsedBytes()

  /**
   * Returns a list of workers information.
   */
  list<WorkerInfo> getWorkerInfoList()
}

/**
 * This interface contains block master service endpoints for Alluxio workers.
 */
service BlockMasterWorkerService extends common.AlluxioService {

  /**
   * Marks the given block as committed.
   */
  void commitBlock( /** the id of the worker */  1: i64 workerId,
      /** the space used in bytes on the target tier */ 2: i64 usedBytesOnTier,
      /** the alias of the target tier */ 3: string tierAlias,
      /** the id of the block being committed */ 4: i64 blockId,
      /** the length of the block being committed */ 5: i64 length)

  /**
   * Returns a worker id for the given network address.
   */
  i64 getWorkerId( /** the worker network address */ 1: common.WorkerNetAddress workerNetAddress)

  /**
   * Periodic worker heartbeat returns an optional command for the worker to execute.
   */
  common.Command heartbeat( /** the id of the worker */ 1: i64 workerId,
      /** the map of space used in bytes on all tiers */ 2: map<string, i64> usedBytesOnTiers,
      /** the list of removed block ids */ 3: list<i64> removedBlockIds,
      /** the map of added blocks on all tiers */ 4: map<string, list<i64>> addedBlocksOnTiers)

  /**
   * Registers a worker.
   */
  void registerWorker( /** the id of the worker */  1: i64 workerId,
      /** the list of storage tiers */  2: list<string> storageTiers,
      /** the map of total bytes on each tier */  3: map<string, i64> totalBytesOnTiers,
      /** the map of used bytes on each tier */  4: map<string, i64> usedBytesOnTiers,
      /** the map of list of blocks on each tier */  5: map<string, list<i64>> currentBlocksOnTiers)
    throws (1: exception.AlluxioTException e)
}
