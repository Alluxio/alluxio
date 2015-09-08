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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.StorageLevelAlias;
import tachyon.Users;
import tachyon.exception.AlreadyExistsException;
import tachyon.exception.InvalidStateException;
import tachyon.exception.NotFoundException;
import tachyon.exception.OutOfSpaceException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.WorkerService;

/**
 * Handles all thrift RPC calls to the worker. This class is a thrift server implementation and is
 * thread safe.
 */
// TODO: better exception handling than wrapping into TException
public final class BlockServiceHandler implements WorkerService.Iface {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Block data manager that carries out most of the operations **/
  private final BlockDataManager mWorker;

  public BlockServiceHandler(BlockDataManager worker) {
    mWorker = worker;
  }

  /**
   * This should be called whenever a client does a direct read in order to update the worker's
   * components that may care about the access times of the blocks (for example, Evictor, UI).
   *
   * @param blockId the id of the block to access
   * @throws TException if the underlying worker RPC fails
   */
  @Override
  public void accessBlock(long blockId) throws TException {
    try {
      mWorker.accessBlock(Users.ACCESS_BLOCK_USER_ID, blockId);
    } catch (NotFoundException nfe) {
      throw new TException(nfe);
    }
  }

  /**
   * This should be called in order to commit a file that was written directly to the under storage
   * system via a THROUGH type write. This will update the master with the appropriate metadata
   * for the new block.
   *
   * @param userId the id of the client requesting the checkpoint
   * @param fileId the id of the file that was written to the under storage system
   * @throws TException if the underlying worker RPC fails
   */
  @Override
  public void addCheckpoint(long userId, long fileId) throws TException {
    try {
      mWorker.addCheckpoint(userId, fileId);
    } catch (IOException ioe) {
      throw new TException(ioe);
    }
  }

  // TODO: Make this supported again
  @Override
  public boolean asyncCheckpoint(long fileId) throws TException {
    return false;
  }

  /**
   * Used to cache a block into Tachyon space, worker will move the temporary block file from user
   * folder to data folder, and update the space usage information related. then update the block
   * information to master.
   *
   * @param userId the id of the client requesting the commit
   * @param blockId the id of the block to commit
   * @throws TException if the underlying worker RPC fails
   */
  // TODO: Reconsider this exception handling
  @Override
  public void cacheBlock(long userId, long blockId) throws TException {
    try {
      mWorker.commitBlock(userId, blockId);
    } catch (AlreadyExistsException aee) {
      throw new TException(aee);
    } catch (NotFoundException nfe) {
      throw new TException(nfe);
    } catch (InvalidStateException fpe) {
      throw new TException(fpe);
    } catch (IOException ioe) {
      throw new TException(ioe);
    } catch (OutOfSpaceException ooe) {
      throw new TException(ooe);
    }
  }

  /**
   * Used to cancel a block which is being written. worker will delete the temporary block file and
   * the location and space information related, then reclaim space allocated to the block.
   *
   * @param userId the id of the client requesting the abort
   * @param blockId the id of the block to be aborted
   * @throws TException if the underlying worker RPC fails
   */
  @Override
  public void cancelBlock(long userId, long blockId) throws TException {
    try {
      mWorker.abortBlock(userId, blockId);
    } catch (AlreadyExistsException aee) {
      throw new TException(aee);
    } catch (NotFoundException nfe) {
      throw new TException(nfe);
    } catch (InvalidStateException fpe) {
      throw new TException(fpe);
    } catch (IOException ioe) {
      throw new TException(ioe);
    }
  }

  /**
   * Used to get user's temporary folder on under file system, and the path of the user's temporary
   * folder will be returned.
   *
   * @param userId the id of the user requesting the ufs location
   */
  @Override
  public String getUserUfsTempFolder(long userId) {
    return mWorker.getUserUfsTmpFolder(userId);
  }

  /**
   * Lock the file in Tachyon's space while the user is reading it, and the path of the block file
   * locked will be returned, if the block file is not found, FileDoesNotExistException will be
   * thrown.
   *
   * @param blockId the id of the block to be locked
   * @param userId the id of the
   * @throws FileDoesNotExistException if the underlying file is not found
   * @throws TException if the underlying worker RPC fails
   */
  @Override
  public String lockBlock(long blockId, long userId) throws FileDoesNotExistException, TException {
    try {
      long lockId = mWorker.lockBlock(userId, blockId);
      return mWorker.readBlock(userId, blockId, lockId);
    } catch (NotFoundException nfe) {
      // TODO: reconsider this, maybe it is because lockId can not be found
      throw new FileDoesNotExistException(nfe.getMessage());
    } catch (InvalidStateException fpe) {
      throw new TException(fpe);
    }
  }

  /**
   * Used to promote block on under storage layer to top storage layer when there are more than one
   * storage layers in Tachyon's space. return true if the block is successfully promoted, false
   * otherwise.
   *
   * @param blockId the id of the block to move to the top layer
   * @throws TException if the underlying worker RPC fails
   */
  // TODO: This may be better as void
  @Override
  public boolean promoteBlock(long blockId) throws TException {
    try {
      // TODO: Make the top level configurable
      mWorker.moveBlock(Users.MIGRATE_DATA_USER_ID, blockId, StorageLevelAlias.MEM.getValue());
      return true;
    } catch (NotFoundException nfe) {
      throw new TException(nfe);
    } catch (AlreadyExistsException aee) {
      throw new TException(aee);
    } catch (InvalidStateException fpe) {
      throw new TException(fpe);
    } catch (IllegalArgumentException iae) {
      throw new TException(iae);
    } catch (tachyon.exception.OutOfSpaceException ooe) {
      throw new TException(ooe);
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
   * @param userId the id of the client requesting the create
   * @param blockId the id of the new block to create
   * @param initialBytes the initial number of bytes to allocate for this block
   * @throws tachyon.thrift.OutOfSpaceException if there is no space available
   * @throws TException if the underlying worker RPC fails
   */
  @Override
  public String requestBlockLocation(long userId, long blockId, long initialBytes)
      throws tachyon.thrift.OutOfSpaceException, TException {
    try {
      // NOTE: right now, we ask allocator to allocate new blocks in MEM tier
      return mWorker.createBlock(userId, blockId, StorageLevelAlias.MEM.getValue(), initialBytes);
    } catch (AlreadyExistsException aee) {
      throw new TException(aee);
    } catch (IllegalArgumentException iae) {
      throw new TException(iae);
    } catch (OutOfSpaceException ooe) {
      throw new tachyon.thrift.OutOfSpaceException(ooe.getMessage());
    } catch (NotFoundException nfe) {
      throw new TException(nfe);
    } catch (IOException ioe) {
      throw new TException(ioe);
    } catch (InvalidStateException ise) {
      throw new TException(ise);
    }
  }

  /**
   * Used to request space for some block file. return true if the worker successfully allocates
   * space for the block on block’s location, false if there is no enough space.
   *
   * @param userId the id of the client requesting space
   * @param blockId the id of the block to add the space to, this must be a temporary block
   * @param requestBytes the amount of bytes to add to the block
   */
  @Override
  public boolean requestSpace(long userId, long blockId, long requestBytes) {
    try {
      mWorker.requestSpace(userId, blockId, requestBytes);
      return true;
    } catch (Exception e) {
      LOG.error("Failed to request " + requestBytes + " bytes for block: " + blockId, e);
    }
    return false;
  }

  /**
   * Used to unlock a block after the block is accessed, if the block is to be removed, delete the
   * block file. return true if successfully unlock the block, return false if the block is not
   * found or failed to delete the block.
   *
   * @param blockId the id of the block to unlock
   * @param userId the id of the client requesting the unlock
   * @throws TException if the block does not exist
   */
  @Override
  public boolean unlockBlock(long blockId, long userId) throws TException {
    try {
      mWorker.unlockBlock(userId, blockId);
      return true;
    } catch (NotFoundException nfe) {
      throw new TException(nfe);
    }
  }

  /**
   * Local user send heartbeat to local worker to keep its temporary folder.
   *
   * @param userId the id of the client heartbeating
   * @param metrics a list of the client metrics that were collected between this heartbeat and
   *                the last. Each value in the list represents a specific metric based on the
   *                index.
   */
  @Override
  public void userHeartbeat(long userId, List<Long> metrics) {
    mWorker.userHeartbeat(userId, metrics);
  }
}
