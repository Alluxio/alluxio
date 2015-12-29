namespace java tachyon.thrift

include "common.thrift"
include "exception.thrift"

struct LockBlockResult {
  1: i64 lockId
  2: string blockPath
}

service BlockWorkerClientService extends common.TachyonService {

  /**
   * Accesses a block given the block id.
   */
  void accessBlock( /** the id of the block being accessed */ 1: i64 blockId)

  /**
   * Asynchronously checkpoints a file: returns whether the checkpoint operation succeeded.
   */
  bool asyncCheckpoint( /** the id of the file being accessed */ 1: i64 fileId)
    throws (1: exception.TachyonTException e)

  /**
   * Used to cache a block into Tachyon space, worker will move the temporary block file from session
   * folder to data folder, and update the space usage information related. then update the block
   * information to master.
   */
  void cacheBlock( /** the id of the current session */ 1: i64 sessionId,
      /** the id of the block being accessed */ 2: i64 blockId)
    throws (1: exception.TachyonTException e, 2: exception.ThriftIOException ioe)

  /**
   * Used to cancel a block which is being written. worker will delete the temporary block file and
   * the location and space information related, then reclaim space allocated to the block.
   */
  void cancelBlock( /** the id of the current session */ 1: i64 sessionId,
      /** the id of the block being accessed */ 2: i64 blockId)
    throws (1: exception.TachyonTException e, 2: exception.ThriftIOException ioe)

  /**
   * Locks the file in Tachyon's space while the session is reading it. If lock succeeds, the path of
   * the block's file along with the internal lock id of locked block will be returned. If the block's file
   * is not found, FileDoesNotExistException will be thrown.
   */
  LockBlockResult lockBlock( /** the id of the block being accessed */ 1: i64 blockId,
      /** the id of the current session */ 2: i64 sessionId)
    throws (1: exception.TachyonTException e)

  /**
   * Used to promote block on under storage layer to top storage layer when there are more than one
   * storage layers in Tachyon's space. return true if the block is successfully promoted, false
   * otherwise.
   */
  bool promoteBlock( /** the id of the block being accessed */ 1: i64 blockId)
    throws (1: exception.TachyonTException e, 2: exception.ThriftIOException ioe)

  /**
   * Used to allocate location and space for a new coming block, worker will choose the appropriate
   * storage directory which fits the initial block size by some allocation strategy, and the
   * temporary file path of the block file will be returned. if there is no enough space on Tachyon
   * storage OutOfSpaceException will be thrown, if the file is already being written by the session,
   * FileAlreadyExistsException will be thrown.
   */
  string requestBlockLocation( /** the id of the current session */ 1: i64 sessionId,
      /** the id of the block being accessed */ 2: i64 blockId,
      /** initial number of bytes requested */ 3: i64 initialBytes)
    throws (1: exception.TachyonTException e, 2: exception.ThriftIOException ioe)

  /**
   * Used to request space for some block file. return true if the worker successfully allocates
   * space for the block on block’s location, false if there is no enough space, if there is no
   * information of the block on worker, FileDoesNotExistException will be thrown.
   */
  bool requestSpace( /** the id of the current session */ 1: i64 sessionId,
      /** the id of the block being accessed */ 2: i64 blockId,
      /** the number of bytes requested */ 3: i64 requestBytes)
    throws (1: exception.TachyonTException e)

  /**
   * Local session send heartbeat to local worker to keep its temporary folder. It also sends client
   * metrics to the worker.
   */
  void sessionHeartbeat( /** the id of the current session */ 1: i64 sessionId,
      /** the client metrics */ 2: list<i64> metrics)

  /**
   * Used to unlock a block after the block is accessed, if the block is to be removed, delete the
   * block file. return true if successfully unlock the block, return false if the block is not
   * found or failed to delete the block.
   */
  bool unlockBlock( /** the id of the block being accessed */ 1: i64 blockId,
      /** the id of the current session */ 2: i64 sessionId)
}
