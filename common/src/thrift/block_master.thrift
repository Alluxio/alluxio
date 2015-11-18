namespace java tachyon.thrift

include "common.thrift"
include "exception.thrift"

struct WorkerInfo {
  1: i64 id
  2: common.NetAddress address
  3: i32 lastContactSec
  4: string state
  5: i64 capacityBytes
  6: i64 usedBytes
  7: i64 startTimeMs
}

service BlockMasterService extends common.TachyonService {

  /**
   * Returns the block information for the given block id.
   * @param blockId
   * @return the block information for the given block id
   * @throws TachyonTException
   */
  common.BlockInfo getBlockInfo(1: i64 blockId) throws (1: exception.TachyonTException e)

  /**
   * Returns the capacity (in bytes).
   * @return the block capacity in number of bytes
   */
  i64 getCapacityBytes()

  /**
   * Returns the used storage (in bytes).
   * @return the block usage in number of bytes
   */
  i64 getUsedBytes()

  /**
   * Returns a list of workers information.
   * @return a list of workers information
   */
  list<WorkerInfo> getWorkerInfoList()

  /**
   * Marks the given block as committed.
   * @param workerId
   * @param usedBytesOnTier
   * @param tierAlias
   * @param blockId
   * @param length
   */
  void workerCommitBlock(1: i64 workerId, 2: i64 usedBytesOnTier, 3: string tierAlias, 4: i64 blockId,
      5: i64 length)

  /**
   * Returns a worker id for the given network address.
   * @param workerNetAddress
   * @return a worker id for the given network address
   */
  i64 workerGetWorkerId(1: common.NetAddress workerNetAddress)

  /**
   * Periodic worker heartbeat.
   * @param workerId
   * @param usedBytesOnTiers
   * @param removedBlockIds
   * @param addedBlocksOnTiers
   * @return  an optional command for the worker to execute
   */
  common.Command workerHeartbeat(1: i64 workerId, 2: map<string, i64> usedBytesOnTiers,
      3: list<i
   * @param totalBytesOnTiers
   * @param usedBytesOnTiers
   * @param currentBlocksOnTiers64> removedBlockIds, 4: map<string, list<i64>> addedBlocksOnTiers)

  /**
   * Registers a worker.
   * @param workerId
   * @param storageTiers
   * @param totalBytesOnTiers
   * @param usedBytesOnTiers
   * @param currentBlocksOnTiers
   * @throws TachyonTException
   */
  void workerRegister(1: i64 workerId, 2: list<string> storageid of the workertesOnTiers,
      4: map<string, i64> usedBytesOnTiers, 5: map<string, list<i64>> currentBlocksOnTiers)
    throws (1: exception.TachyonTException e)
}
