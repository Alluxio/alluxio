namespace java tachyon.thrift

include "common.thrift"
include "exception.thrift"

service WorkerService {
  void accessBlock(1: i64 blockId)

  bool asyncCheckpoint(1: i64 fileId)
    throws (1: exception.TachyonTException e)

  /**
   * Used to cache a block into Tachyon space, worker will move the temporary block file from session
   * folder to data folder, and update the space usage information related. then update the block
   * information to master.
   */
  void cacheBlock(1: i64 sessionId, 2: i64 blockId)
    throws (1: exception.TachyonTException e, 2: exception.ThriftIOException ioe)

  /**
   * Used to cancel a block which is being written. worker will delete the temporary block file and
   * the location and space information related, then reclaim space allocated to the block.
   */
  void cancelBlock(1: i64 sessionId, 2: i64 blockId)
    throws (1: exception.TachyonTException e, 2: exception.ThriftIOException ioe)

  /**
   * Lock the file in Tachyon's space while the session is reading it, and the path of the block file
   * locked will be returned, if the block file is not found, FileDoesNotExistException will be
   * thrown.
   */
  string lockBlock(1: i64 blockId, 2: i64 sessionId)
    throws (1: exception.TachyonTException e)

  void persistFile(1: i64 fileId, 2: i64 nonce, 3: string path)
    throws (1: exception.TachyonTException e)

  /**
   * Used to promote block on under storage layer to top storage layer when there are more than one
   * storage layers in Tachyon's space. return true if the block is successfully promoted, false
   * otherwise.
   */
  bool promoteBlock(1: i64 blockId)
    throws (1: exception.TachyonTException e, 2: exception.ThriftIOException ioe)

  /**
   * Used to allocate location and space for a new coming block, worker will choose the appropriate
   * storage directory which fits the initial block size by some allocation strategy, and the
   * temporary file path of the block file will be returned. if there is no enough space on Tachyon
   * storage OutOfSpaceException will be thrown, if the file is already being written by the session,
   * FileAlreadyExistsException will be thrown.
   */
  string requestBlockLocation(1: i64 sessionId, 2: i64 blockId, 3: i64 initialBytes)
    throws (1: exception.TachyonTException e, 2: exception.ThriftIOException ioe)

  /**
   * Used to request space for some block file. return true if the worker successfully allocates
   * space for the block on block’s location, false if there is no enough space, if there is no
   * information of the block on worker, FileDoesNotExistException will be thrown.
   */
  bool requestSpace(1: i64 sessionId, 2: i64 blockId, 3: i64 requestBytes)
    throws (1: exception.TachyonTException e)

  /**
   * Local session send heartbeat to local worker to keep its temporary folder. It also sends client
   * metrics to the worker.
   */
  void sessionHeartbeat(1: i64 sessionId, 2: list<i64> metrics)

  /**
   * Used to unlock a block after the block is accessed, if the block is to be removed, delete the
   * block file. return true if successfully unlock the block, return false if the block is not
   * found or failed to delete the block.
   */
  bool unlockBlock(1: i64 blockId, 2: i64 sessionId)
}
