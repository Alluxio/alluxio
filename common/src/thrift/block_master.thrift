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

service BlockMasterService {

  // Tachyon Client API

  /**
   * Returns the block information for the given block id.
   */
  common.BlockInfo getBlockInfo(1: i64 blockId) throws (1: exception.TachyonTException e)

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

  // Tachyon Worker API

  /**
   * Marks the given block as committed.
   */
  void workerCommitBlock(1: i64 workerId, 2: i64 usedBytesOnTier, 3: i32 tier, 4: i64 blockId,
      5: i64 length)
    throws (1: exception.TachyonTException e)

  /**
   * Returns a worker id for the given network address.
   */
  i64 workerGetWorkerId(1: common.NetAddress workerNetAddress)

  /**
   * Periodic worker heartbeat.
   */
  common.Command workerHeartbeat(1: i64 workerId, 2: list<i64> usedBytesOnTiers,
      3: list<i64> removedBlockIds, 4: map<i64, list<i64>> addedBlocksOnTiers)
    throws (1: exception.TachyonTException e)

  /**
   * Registers a worker.
   */
  void workerRegister(1: i64 workerId, 2: list<i64> totalBytesOnTiers,
      3: list<i64> usedBytesOnTiers, 4: map<i64, list<i64>> currentBlocksOnTiers)
    throws (1: exception.TachyonTException e)
}
