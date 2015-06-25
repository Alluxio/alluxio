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

import tachyon.Users;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.OutOfSpaceException;
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

  public void accessBlock(long blockId) throws TException {
    try {
      mWorker.accessBlock(-1, blockId);
    } catch (IOException ioe) {
      throw new TException(ioe);
    }
  }

  public void addCheckpoint(long userId, int fileId) throws TException {
    try {
      mWorker.addCheckpoint(userId, fileId);
    } catch (IOException ioe) {
      throw new TException(ioe);
    }
  }

  // TODO: Make this supported again
  public boolean asyncCheckpoint(int fileId) throws TException {
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
  // TODO: Reconsider this exception handling
  public void cacheBlock(long userId, long blockId) throws TException {
    try {
      if (!mWorker.commitBlock(userId, blockId)) {
        throw new TException("Failed to commit block: " + blockId);
      }
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
    try {
      long lockId = mWorker.lockBlock(userId, blockId);
      return mWorker.readBlock(userId, blockId, lockId);
    } catch (IOException ioe) {
      throw new FileDoesNotExistException("Block " + blockId + " does not exist on this worker.");
    }
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
      mWorker.promoteBlock(Users.MIGRATE_DATA_USER_ID, blockId);
      return true;
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
      // NOTE: right now, we ask allocator to allocate new blocks in MEM tier by setting location
      // to be 1.
      // TODO: Maybe add a constant for anyTier?
      return mWorker.createBlock(userId, blockId, 1, initialBytes);
    } catch (IOException ioe) {
      throw new OutOfSpaceException("Failed to allocate " + initialBytes + " for user " + userId);
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
  public boolean requestSpace(long userId, long blockId, long requestBytes) throws TException {
    try {
      mWorker.requestSpace(userId, blockId, requestBytes);
      return true;
    } catch (IOException ioe) {
      return false;
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
  public boolean unlockBlock(long blockId, long userId) throws TException {
    try {
      mWorker.unlockBlock(userId, blockId);
      return true;
    } catch (IOException ioe) {
      throw new TException(ioe);
    }
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
