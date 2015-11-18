namespace java tachyon.thrift

include "common.thrift"
include "exception.thrift"

service WorkerService extends common.TachyonService {

  /**
   * @param blockId
   */
  void accessBlock(1: i64 blockId)

  /**
   * @param fileId
   * @return whether the checkpoint operation succeeded
   * @throws TachyonTException
   */
  bool asyncCheckpoint(1: i64 fileId)
    throws (1: exception.TachyonTException e)

  /**
   * Used to cache a block into Tachyon space, worker will move the temporary block file from session
   * folder to data folder, and update the space usage information related. then update the block
   * information to master.
   * @param sessionId
   * @param blockId
   * @throws TachyonTException
   * @throws ThriftIOException
   */
  void cacheBlock(1: i64 sessionId, 2: i64 blockId)
    throws (1: exception.TachyonTException e, 2: exception.ThriftIOException ioe)

  /**
   * Used to cancel a block which is being written. worker will delete the temporary block file and
   * the location and space information related, then reclaim space allocated to the block.
   * @param sessionId
   * @param blockId
   * @throws TachyonTException
   * @throws ThriftIOException
   */
  void cancelBlock(1: i64 sessionId, 2: i64 blockId)
    throws (1: exception.TachyonTException e, 2: exception.ThriftIOException ioe)

  /**
   * Lock the file in Tachyon's space while the session is reading it, and the path of the block file
   * locked will be returned, if the block file is not found, FileDoesNotExistException will be
   * thrown.
   * @param blockId
   * @param sessionId
   * @return the path of the block file locked
   * @throws TachyonTException
   */
  string lockBlock(1: i64 blockId, 2: i64 sessionId)
    throws (1: exception.TachyonTException e)

  /**
   * Used to promote block on under storage layer to top storage layer when there are more than one
   * storage layers in Tachyon's space. return true if the block is successfully promoted, false
   * otherwise.
   * @param blockId
   * @return whether the promotBlock operation succeeded
   * @throws TachyonTException
   * @throws ThriftIOException
   */
  bool promoteBlock(1: i64 blockId)
    throws (1: exception.TachyonTException e, 2: exception.ThriftIOException ioe)

  /**
   * Used to allocate location and space for a new coming block, worker will choose the appropriate
   * storage directory which fits the initial block size by some allocation strategy, and the
   * temporary file path of the block file will be returned. if there is no enough space on Tachyon
   * storage OutOfSpaceException will be thrown, if the file is already being written by the session,
   * FileAlreadyExistsException will be thrown.
   * @param sessionId
   * @param blockId
   * @param initialBytes
   * @return the temporary file path of the block file
   * @throws TachyonTException
   * @throws ThriftIOException
   */
  string requestBlockLocation(1: i64 sessionId, 2: i64 blockId, 3: i64 initialBytes)
    throws (1: exception.TachyonTException e, 2: exception.ThriftIOException ioe)

  /**
   * Used to request space for some block file. return true if the worker successfully allocates
   * space for the block on block’s location, false if there is no enough space, if there is no
   * information of the block on worker, FileDoesNotExistException will be thrown.
   * @param sessionId
   * @param blockId
   * @param requestBytes
   * @return whether the requestSpace operation succeeded
   * @throws TachyonTException
   */
  bool requestSpace(1: i64 sessionId, 2: i64 blockId, 3: i64 requestBytes)
    throws (1: exception.TachyonTException e)

  /**
   * Local session send heartbeat to local worker to keep its temporary folder. It also sends client
   * metrics to the worker.
   * @param sessionId
   * @param metrics
   */
  void sessionHeartbeat(1: i64 sessionId, 2: list<i64> metrics)

  /**
   * Used to unlock a block after the block is accessed, if the block is to be removed, delete the
   * block file. return true if successfully unlock the block, return false if the block is not
   * found or failed to delete the block.
   * @param blockId
   * @param sessionId
   * @return whether the unlockBlock operation succeeded
   */
  bool unlockBlock(1: i64 blockId, 2: i64 sessionId)
}
