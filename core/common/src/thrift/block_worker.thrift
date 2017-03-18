namespace java alluxio.thrift

include "common.thrift"
include "exception.thrift"

struct LockBlockResult {
  1: i64 lockId
  2: string blockPath
  3: LockBlockStatus lockBlockStatus
}

enum LockBlockStatus {
   /** The Alluxio block is acquired. */
   ALLUXIO_BLOCK_LOCKED = 1,
   /** The block is not in Alluxio but a UFS access token is acquired for this block. */
   UFS_TOKEN_ACQUIRED = 2,
   /** The block is not in Alluxio and a UFS access token is not acquired. */
   UFS_TOKEN_NOT_ACQUIRED = 3,
}

struct LockBlockTOptions {
  1: string ufsPath
  2: i64 offset
  3: i64 blockSize
  4: i32 maxUfsReadConcurrency
}

service BlockWorkerClientService extends common.AlluxioService {

  /**
   * Accesses a block given the block id.
   */
  void accessBlock(
    /** the id of the block being accessed */ 1: i64 blockId,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Used to cache a block into Alluxio space, worker will move the temporary block file from session
   * folder to data folder, and update the space usage information related. then update the block
   * information to master.
   */
  void cacheBlock(
    /** the id of the current session */ 1: i64 sessionId,
    /** the id of the block being accessed */ 2: i64 blockId,
    )
    throws (1: exception.AlluxioTException e, 2: exception.ThriftIOException ioe)

  /**
   * Used to cancel a block which is being written. worker will delete the temporary block file and
   * the location and space information related, then reclaim space allocated to the block.
   */
  void cancelBlock(
    /** the id of the current session */ 1: i64 sessionId,
    /** the id of the block being accessed */ 2: i64 blockId,
    )
    throws (1: exception.AlluxioTException e, 2: exception.ThriftIOException ioe)

  /**
   * Locks the file in Alluxio's space while the session is reading it. If lock succeeds, the path of
   * the block's file along with the internal lock id of locked block will be returned. If the block's file
   * is not found, FileDoesNotExistException will be thrown.
   */
  LockBlockResult lockBlock(
    /** the id of the block being accessed */ 1: i64 blockId,
    /** the id of the current session */ 2: i64 sessionId,
    /** the lock block options */ 3: LockBlockTOptions options,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Used to promote block on under storage layer to top storage layer when there are more than one
   * storage layers in Alluxio's space. return true if the block is successfully promoted, false
   * otherwise.
   */
  bool promoteBlock(
    /** the id of the block being accessed */ 1: i64 blockId,
    )
    throws (1: exception.AlluxioTException e, 2: exception.ThriftIOException ioe)

  /**
   * Used to remove a block from an Alluxio worker.
   **/
  void removeBlock(
    /** the id of the block being removed */ 1: i64 blockId,
    )
    throws (1: exception.AlluxioTException e, 2: exception.ThriftIOException ioe)

  /**
   * Used to allocate location and space for a new coming block, worker will choose the appropriate
   * storage directory which fits the initial block size by some allocation strategy, and the
   * temporary file path of the block file will be returned. if there is no enough space on Alluxio
   * storage OutOfSpaceException will be thrown, if the file is already being written by the session,
   * FileAlreadyExistsException will be thrown.
   */
  string requestBlockLocation(
    /** the id of the current session */ 1: i64 sessionId,
    /** the id of the block being accessed */ 2: i64 blockId,
    /** initial number of bytes requested */ 3: i64 initialBytes,
    /** the target tier to write to */ 4: i32 writeTier,
    )
    throws (1: exception.AlluxioTException e, 2: exception.ThriftIOException ioe)

  /**
   * Used to request space for some block file. return true if the worker successfully allocates
   * space for the block on block’s location, false if there is no enough space, if there is no
   * information of the block on worker, FileDoesNotExistException will be thrown.
   */
  bool requestSpace(
    /** the id of the current session */ 1: i64 sessionId,
    /** the id of the block being accessed */ 2: i64 blockId,
    /** the number of bytes requested */ 3: i64 requestBytes,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Local session send heartbeat to local worker to keep its temporary folder.
   */
  void sessionHeartbeat(
    /** the id of the current session */ 1: i64 sessionId,
    /** deprecated since 1.3.0 and will be removed in 2.0 */ 2: list<i64> metrics,
    )
    throws (1: exception.AlluxioTException e)

  /**
   * Used to unlock a block after the block is accessed, if the block is to be removed, delete the
   * block file. return true if successfully unlock the block, return false if the block is not
   * found or failed to delete the block.
   */
  bool unlockBlock(
    /** the id of the block being accessed */ 1: i64 blockId,
    /** the id of the current session */ 2: i64 sessionId,
    )
    throws (1: exception.AlluxioTException e, 2: exception.ThriftIOException ioe)
}
