namespace java alluxio.thrift

include "common.thrift"
include "exception.thrift"

enum BlockMasterInfoField {
  CAPACITY_BYTES
  CAPACITY_BYTES_ON_TIERS
  FREE_BYTES
  LIVE_WORKER_NUM
  LOST_WORKER_NUM
  USED_BYTES
  USED_BYTES_ON_TIERS
}

struct BlockMasterInfo {
  1: i64 capacityBytes
  2: map<string, i64> capacityBytesOnTiers
  3: i64 freeBytes
  4: i32 liveWorkerNum
  5: i32 lostWorkerNum
  6: i64 usedBytes
  7: map<string, i64> usedBytesOnTiers
}

struct GetBlockInfoTOptions {}
struct GetBlockInfoTResponse {
  1: common.BlockInfo blockInfo
}

struct GetBlockMasterInfoTOptions {
 1: set<BlockMasterInfoField> filter
}

struct GetBlockMasterInfoTResponse {
 1: BlockMasterInfo blockMasterInfo
}

struct GetCapacityBytesTOptions {}
struct GetCapacityBytesTResponse {
  1: i64 bytes
}

struct GetUsedBytesTOptions {}
struct GetUsedBytesTResponse {
  1: i64 bytes
}

enum WorkerRange {
  ALL
  LIVE
  LOST
  SPECIFIED
}

enum WorkerInfoField {
  ADDRESS
  CAPACITY_BYTES
  CAPACITY_BYTES_ON_TIERS
  ID
  LAST_CONTACT_SEC
  START_TIME_MS
  STATE
  USED_BYTES
  USED_BYTES_ON_TIERS
}

struct WorkerInfo {
  1: i64 id
  2: common.WorkerNetAddress address
  3: i32 lastContactSec
  4: string state
  5: i64 capacityBytes
  6: i64 usedBytes
  7: i64 startTimeMs
  8: map<string, i64> capacityBytesOnTiers;
  9: map<string, i64> usedBytesOnTiers;
}

struct GetWorkerReportTOptions {
  /** addresses are only valid when workerRange is SPECIFIED */
  1: optional set<string> addresses
  2: optional set<WorkerInfoField>  fieldRange
  3: optional WorkerRange workerRange
}

struct GetWorkerInfoListTOptions {}

struct GetWorkerInfoListTResponse {
  1: list<WorkerInfo> workerInfoList
}

/**
 * This interface contains block master service endpoints for Alluxio clients.
 */
service BlockMasterClientService extends common.AlluxioService {

  /**
   * Returns the block information for the given block id.
   */
  GetBlockInfoTResponse getBlockInfo(
    /** the id of the block */  1: i64 blockId,
    /** the method options */ 2: GetBlockInfoTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
    * Returns block master information.
    */
  GetBlockMasterInfoTResponse getBlockMasterInfo(
    /** the method options */ 1: GetBlockMasterInfoTOptions options,
  ) throws (1: exception.AlluxioTException e)

  /**
   * Returns the capacity (in bytes).
   */
  GetCapacityBytesTResponse getCapacityBytes(
    /** the method options */ 1: GetCapacityBytesTOptions options,
  ) throws (1: exception.AlluxioTException e)

  /**
   * Returns the used storage (in bytes).
   */
  GetUsedBytesTResponse getUsedBytes(
    /** the method options */ 1: GetUsedBytesTOptions options,
  ) throws (1: exception.AlluxioTException e)

  /**
   * Returns a list of workers information.
   */
  GetWorkerInfoListTResponse getWorkerInfoList(
    /** the method options */ 1: GetWorkerInfoListTOptions options,
  ) throws (1: exception.AlluxioTException e)

  /**
   * Returns a list of workers information for report CLI.
   */
  GetWorkerInfoListTResponse getWorkerReport(
    /** the method options */ 1: GetWorkerReportTOptions options,
  ) throws (1: exception.AlluxioTException e)
}

struct BlockHeartbeatTOptions {
  1: list<common.Metric> metrics
}
struct BlockHeartbeatTResponse {
  1: common.Command command
}

struct CommitBlockTOptions {}
struct CommitBlockTResponse {}

struct GetWorkerIdTOptions {}
struct GetWorkerIdTResponse {
  1: i64 workerId
}

struct RegisterWorkerTOptions {
  1: optional list<common.ConfigProperty> configList
}

struct RegisterWorkerTResponse {}

/**
 * This interface contains block master service endpoints for Alluxio workers.
 */
service BlockMasterWorkerService extends common.AlluxioService {

  /**
   * Periodic block worker heartbeat returns an optional command for the block worker to execute.
   */
  BlockHeartbeatTResponse blockHeartbeat(
    /** the id of the worker */ 1: i64 workerId,
    /** the map of space used in bytes on all tiers */ 2: map<string, i64> usedBytesOnTiers,
    /** the list of removed block ids */ 3: list<i64> removedBlockIds,
    /** the map of added blocks on all tiers */ 4: map<string, list<i64>> addedBlocksOnTiers,
    /** the method options */ 5: BlockHeartbeatTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Marks the given block as committed.
   */
  CommitBlockTResponse commitBlock(
    /** the id of the worker */  1: i64 workerId,
    /** the space used in bytes on the target tier */ 2: i64 usedBytesOnTier,
    /** the alias of the target tier */ 3: string tierAlias,
    /** the id of the block being committed */ 4: i64 blockId,
    /** the length of the block being committed */ 5: i64 length,
    /** the method options */ 6: CommitBlockTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Returns a worker id for the given network address.
   */
  GetWorkerIdTResponse getWorkerId(
    /** the worker network address */ 1: common.WorkerNetAddress workerNetAddress,
    /** the method options */ 2: GetWorkerIdTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Registers a worker.
   */
  RegisterWorkerTResponse registerWorker(
    /** the id of the worker */  1: i64 workerId,
    /** the list of storage tiers */  2: list<string> storageTiers,
    /** the map of total bytes on each tier */  3: map<string, i64> totalBytesOnTiers,
    /** the map of used bytes on each tier */  4: map<string, i64> usedBytesOnTiers,
    /** the map of list of blocks on each tier */  5: map<string, list<i64>> currentBlocksOnTiers,
    /** the method options */ 6: RegisterWorkerTOptions options,
    )
    throws (1: exception.AlluxioTException e)
}
