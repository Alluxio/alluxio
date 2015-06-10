/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.worker.block;

import java.io.IOException;
import java.util.List;

import org.apache.thrift.TException;

import tachyon.thrift.BlockInfoException;
import tachyon.thrift.FailedToCheckpointException;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.OutOfSpaceException;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.TachyonException;
import tachyon.thrift.WorkerService;

/**
 * Handles all thrift RPC calls to the worker. This class is a thrift server implementation and is
 * thread safe.
 */
public class BlockServiceHandler implements WorkerService.Iface {

  private final BlockDataManager mWorker;

  public BlockServiceHandler(BlockDataManager worker) {
    mWorker = worker;
  }

  /**
   * Used when a client wishes to abort a temporary block it is managing.
   *
   * @param userId The user id of the client
   * @param blockId The id of the block to be aborted
   * @return true if successful, false otherwise
   * @throws TException if the block does not exist or is committed
   */
  public boolean abortBlock(long userId, long blockId) throws TException {
    try {
      return mWorker.abortBlock(userId, blockId);
    } catch (IOException ioe) {
      throw new TException(ioe);
    }
  }

  /**
   * Used to create a new block on this worker. This is only used for local clients.
   *
   * @param userId The id of the client
   * @param blockId The id of the block
   * @param location The tier to place the block in, 0 for any tier
   * @param initialBytes The amount of space to request for the block initially
   * @return Path to the local file, or null if it failed
   * @throws OutOfSpaceException if there is not enough space in location to create the block
   * @throws FileAlreadyExistException if the block already exists
   */
  public String createBlock(long userId, long blockId, int location, long initialBytes)
      throws TException {
    try {
      return mWorker.createBlock(userId, blockId, location, initialBytes);
    } catch (IOException ioe) {
      throw new TException(ioe);
    }
  }

  /**
   * Used to close a block. Calling this method will move the block from the user temporary folder
   * to the worker's data folder.
   *
   * @param userId The id of the client
   * @param blockId The block id to complete
   * @throws TException if the block fails to be completed
   */
  public void completeBlock(long userId, long blockId) throws TException {
    try {
      mWorker.commitBlock(userId, blockId);
    } catch (IOException ioe) {
      throw new TException(ioe);
    }
  }

  /**
   * Used to remove a block from the Tachyon storage managed by this worker.
   *
   * @param blockId The id of the block
   * @return true if the block is freed successfully, false otherwise
   * @throws TException if the block does not exist
   */
  public boolean freeBlock(long blockId) throws TException {
    try {
      return mWorker.freeBlock(-1L, blockId);
    } catch (IOException ioe) {
      throw new TException(ioe);
    }
  }

  /**
   * Used to get a completed block for reading. This method should only be used if the block is in
   * Tachyon managed space on this worker.
   *
   * @param userId The id of the client
   * @param blockId The id of the block to read
   * @param lockId The lock id of the lock acquired on the block
   * @return the path of the block on local disk
   * @throws TException if the block does not exist
   */
  public String getBlock(long userId, long blockId, int lockId) throws TException {
    return mWorker.readBlock(userId, blockId, lockId);
  }

  // TODO: Rename this method when complete, currently is V2 to avoid checkstyle errors
  /**
   * Obtains a lock on the block.
   *
   * @param userId The id of the client
   * @param blockId The id of the block to lock
   * @param type The type of lock to acquire, 0 for READ, 1 for WRITE
   * @return the lockId of the lock obtained
   */
  public long lockBlockV2(long userId, long blockId, int type) {
    return mWorker.lockBlock(userId, blockId, type);
  }

  // TODO: Rename this method when complete, currently is V2 to avoid checkstyle errors
  /**
   * Relinquishes the lock on the block.
   *
   * @param lockId The id of the lock to relinquish
   * @return true if successful, false otherwise
   */
  public boolean unlockBlockV2(long lockId) {
    return mWorker.unlockBlock(lockId);
  }

  // ================================ WORKER V1 INTERFACE =======================================
  public void accessBlock(long blockId) throws org.apache.thrift.TException {
    mWorker.accessBlock(-1, blockId);
  }

  public void addCheckpoint(long userId, int fileId) throws FileDoesNotExistException,
      SuspectedFileSizeException, FailedToCheckpointException, BlockInfoException,
      org.apache.thrift.TException {

  }

  public boolean asyncCheckpoint(int fileId) throws TachyonException, org.apache.thrift.TException {
    return false;
  }

  /**
   * Used to cache a block into Tachyon space, worker will move the temporary block file from user
   * folder to data folder, and update the space usage information related. then update the block
   * information to master.
   *
   * @param userId
   * @param blockId
   */
  public void cacheBlock(long userId, long blockId) throws TException {
    try {
      mWorker.commitBlock(userId, blockId);
    } catch (IOException ioe) {
      throw new TException(ioe);
    }
  }

  /**
   * Used to cancel a block which is being written. worker will delete the temporary block file and
   * the location and space information related, then reclaim space allocated to the block.
   *
   * @param userId
   * @param blockId
   */
  public void cancelBlock(long userId, long blockId) throws TException {
    try {
      mWorker.abortBlock(userId, blockId);
    } catch (IOException ioe) {
      throw new TException(ioe);
    }
  }

  /**
   * Used to get user's temporary folder on under file system, and the path of the user's temporary
   * folder will be returned.
   *
   * @param userId
   */
  public String getUserUfsTempFolder(long userId) throws TException {
    return mWorker.getUserUfsTmpFolder(userId);
  }

  /**
   * Lock the file in Tachyon's space while the user is reading it, and the path of the block file
   * locked will be returned, if the block file is not found, FileDoesNotExistException will be
   * thrown.
   *
   * @param blockId
   * @param userId
   */
  public String lockBlock(long blockId, long userId) throws TException {
    long lockId = mWorker.lockBlock(userId, blockId, 1);
    return mWorker.readBlock(userId, blockId, lockId);
  }

  /**
   * Used to promote block on under storage layer to top storage layer when there are more than one
   * storage layers in Tachyon's space. return true if the block is successfully promoted, false
   * otherwise.
   *
   * @param blockId
   */
  public boolean promoteBlock(long blockId) throws TException {
    try {
      return mWorker.moveBlock(-1, blockId, 1);
    } catch (IOException ioe) {
      throw new TException(ioe);
    }
  }

  /**
   * Used to allocate location and space for a new coming block, worker will choose the appropriate
   * storage directory which fits the initial block size by some allocation strategy, and the
   * temporary file path of the block file will be returned. if there is no enough space on Tachyon
   * storage OutOfSpaceException will be thrown, if the file is already being written by the user,
   * FileAlreadyExistException will be thrown.
   *
   * @param userId
   * @param blockId
   * @param initialBytes
   */
  public String requestBlockLocation(long userId, long blockId, long initialBytes)
      throws TException {
    try {
      return mWorker.createBlock(userId, blockId, 0, initialBytes);
    } catch (IOException ioe) {
      throw new TException(ioe);
    }
  }

  /**
   * Used to request space for some block file. return true if the worker successfully allocates
   * space for the block on block’s location, false if there is no enough space, if there is no
   * information of the block on worker, FileDoesNotExistException will be thrown.
   *
   * @param userId
   * @param blockId
   * @param requestBytes
   */
  public boolean requestSpace(long userId, long blockId, long requestBytes)
      throws TException {
    try {
      return mWorker.requestSpace(userId, blockId, requestBytes);
    } catch (IOException ioe) {
      throw new TException(ioe);
    }
  }

  /**
   * Used to unlock a block after the block is accessed, if the block is to be removed, delete the
   * block file. return true if successfully unlock the block, return false if the block is not
   * found or failed to delete the block.
   *
   * @param blockId
   * @param userId
   */
  public boolean unlockBlock(long blockId, long userId) {
    return mWorker.unlockBlock(blockId);
  }

  /**
   * Local user send heartbeat to local worker to keep its temporary folder.
   *
   * @param userId
   */
  public void userHeartbeat(long userId, List<Long> metrics) {
    mWorker.userHeartbeat(userId, metrics);
  }
}
