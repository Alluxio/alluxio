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
import tachyon.Sessions;
import tachyon.StorageLevelAlias;
import tachyon.thrift.BlockAlreadyExistsException;
import tachyon.thrift.BlockDoesNotExistException;
import tachyon.thrift.FailedToCheckpointException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidWorkerStateException;
import tachyon.thrift.ThriftIOException;
import tachyon.thrift.WorkerOutOfSpaceException;
import tachyon.thrift.WorkerService;

/**
 * Handles all thrift RPC calls to the worker. This class is a thrift server implementation and is
 * thread safe.
 */
// TODO(cc): Better exception handling than wrapping into TException.
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
   * @throws BlockDoesNotExistException if the blockId is not found
   */
  @Override
  public void accessBlock(long blockId) throws BlockDoesNotExistException {
    mWorker.accessBlock(Sessions.ACCESS_BLOCK_SESSION_ID, blockId);
  }

  /**
   * This should be called in order to commit a file that was written directly to the under storage
   * system via a THROUGH type write. This will update the master with the appropriate metadata for
   * the new block.
   *
   * @param sessionId the id of the client requesting the checkpoint
   * @param fileId the id of the file that was written to the under storage system
   * @throws FailedToCheckpointException if the checkpointing failed
   * @throws FileDoesNotExistException if the file does not exist in Tachyon
   * @throws ThriftIOException if the update to the master fails
   */
  @Override
  public void addCheckpoint(long sessionId, long fileId) throws FileDoesNotExistException,
      ThriftIOException, FailedToCheckpointException {
    try {
      mWorker.addCheckpoint(sessionId, fileId);
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }

  // TODO(calvin): Make this supported again.
  @Override
  public boolean asyncCheckpoint(long fileId) {
    return false;
  }

  /**
   * Used to cache a block into Tachyon space, worker will move the temporary block file from
   * session folder to data folder, and update the space usage information related. then update the
   * block information to master.
   *
   * @param sessionId the id of the client requesting the commit
   * @param blockId the id of the block to commit
   * @throws BlockAlreadyExistsException if blockId already exists in committed blocks
   * @throws BlockDoesNotExistException if the temporary block cannot be found
   * @throws InvalidWorkerStateException if blockId does not belong to sessionId
   * @throws ThriftIOException if the block cannot be moved from temporary path to committed path
   * @throws WorkerOutOfSpaceException if there is no more space left to hold the block
   */
  // TODO(calvin): Reconsider this exception handling.
  @Override
  public void cacheBlock(long sessionId, long blockId) throws WorkerOutOfSpaceException,
      BlockDoesNotExistException, InvalidWorkerStateException, BlockAlreadyExistsException,
      ThriftIOException {
    try {
      mWorker.commitBlock(sessionId, blockId);
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }

  /**
   * Used to cancel a block which is being written. worker will delete the temporary block file and
   * the location and space information related, then reclaim space allocated to the block.
   *
   * @param sessionId the id of the client requesting the abort
   * @param blockId the id of the block to be aborted
   * @throws BlockAlreadyExistsException if blockId already exists in committed blocks
   * @throws BlockDoesNotExistException if the temporary block cannot be found
   * @throws InvalidWorkerStateException if blockId does not belong to sessionId
   * @throws ThriftIOException if temporary block cannot be deleted
   */
  @Override
  public void cancelBlock(long sessionId, long blockId) throws InvalidWorkerStateException,
      BlockAlreadyExistsException, BlockDoesNotExistException, ThriftIOException {
    try {
      mWorker.abortBlock(sessionId, blockId);
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }

  /**
   * Used to get session's temporary folder on under file system, and the path of the session's
   * temporary folder will be returned.
   *
   * @param sessionId the id of the session requesting the ufs location
   */
  @Override
  public String getSessionUfsTempFolder(long sessionId) {
    return mWorker.getSessionUfsTmpFolder(sessionId);
  }

  /**
   * Lock the file in Tachyon's space while the session is reading it, and the path of the block
   * file locked will be returned, if the block file is not found, FileDoesNotExistException will be
   * thrown.
   *
   * @param blockId the id of the block to be locked
   * @param sessionId the id of the session
   * @throws FileDoesNotExistException if blockId cannot be found, for example, evicted already.
   * @throws InvalidWorkerStateException if sessionId or blockId is not the same as that in the
   *         LockRecord of lockId
   */
  @Override
  public String lockBlock(long blockId, long sessionId) throws FileDoesNotExistException,
      InvalidWorkerStateException {
    try {
      long lockId = mWorker.lockBlock(sessionId, blockId);
      return mWorker.readBlock(sessionId, blockId, lockId);
    } catch (BlockDoesNotExistException nfe) {
      // TODO(cc): reconsider this, maybe it is because lockId can not be found
      throw new FileDoesNotExistException(nfe.getMessage());
    }
  }

  /**
   * Used to promote block on under storage layer to top storage layer when there are more than one
   * storage layers in Tachyon's space. return true if the block is successfully promoted, false
   * otherwise.
   *
   * @param blockId the id of the block to move to the top layer
   * @throws IllegalArgumentException if tierAlias is out of range of tiered storage
   * @throws BlockDoesNotExistException if blockId cannot be found
   * @throws BlockAlreadyExistsException if blockId already exists in committed blocks of the
   *         newLocation
   * @throws InvalidWorkerStateException if blockId has not been committed
   * @throws WorkerOutOfSpaceException if newLocation does not have enough extra space to hold the
   *         block
   * @throws ThriftIOException if block cannot be moved from current location to newLocation
   */
  // TODO(calvin): This may be better as void.
  @Override
  public boolean promoteBlock(long blockId) throws WorkerOutOfSpaceException,
      BlockDoesNotExistException, InvalidWorkerStateException, BlockAlreadyExistsException,
      ThriftIOException {
    try {
      // TODO(calvin): Make the top level configurable
      mWorker
          .moveBlock(Sessions.MIGRATE_DATA_SESSION_ID, blockId, StorageLevelAlias.MEM.getValue());
      return true;
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }

  /**
   * Used to allocate location and space for a new coming block, worker will choose the appropriate
   * storage directory which fits the initial block size by some allocation strategy, and the
   * temporary file path of the block file will be returned. if there is no enough space on Tachyon
   * storage WorkerOutOfSpaceException will be thrown, if the file is already being written by the
   * session, FileAlreadyExistException will be thrown.
   *
   * @param sessionId the id of the client requesting the create
   * @param blockId the id of the new block to create
   * @param initialBytes the initial number of bytes to allocate for this block
   * @throws IllegalArgumentException if location does not belong to tiered storage
   * @throws BlockAlreadyExistsException if blockId already exists, either temporary or committed,
   *         or block in eviction plan already exists
   * @throws WorkerOutOfSpaceException if this Store has no more space than the initialBlockSize
   * @throws BlockDoesNotExistException if blocks in eviction plan can not be found
   * @throws ThriftIOException if blocks in eviction plan fail to be moved or deleted
   * @throws InvalidWorkerStateException if blocks to be moved/deleted in eviction plan is
   *         uncommitted
   */
  @Override
  public String requestBlockLocation(long sessionId, long blockId, long initialBytes)
      throws InvalidWorkerStateException, WorkerOutOfSpaceException, BlockDoesNotExistException,
      BlockAlreadyExistsException, ThriftIOException {
    try {
      // NOTE: right now, we ask allocator to allocate new blocks in MEM tier
      return mWorker
          .createBlock(sessionId, blockId, StorageLevelAlias.MEM.getValue(), initialBytes);
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }

  /**
   * Used to request space for some block file. return true if the worker successfully allocates
   * space for the block on block’s location, false if there is no enough space.
   *
   * @param sessionId the id of the client requesting space
   * @param blockId the id of the block to add the space to, this must be a temporary block
   * @param requestBytes the amount of bytes to add to the block
   */
  @Override
  public boolean requestSpace(long sessionId, long blockId, long requestBytes) {
    try {
      mWorker.requestSpace(sessionId, blockId, requestBytes);
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
   * @param sessionId the id of the client requesting the unlock
   * @throws BlockDoesNotExistException if blockId can not be found, for example, evicted already.
   */
  @Override
  public boolean unlockBlock(long blockId, long sessionId) throws BlockDoesNotExistException {
    mWorker.unlockBlock(sessionId, blockId);
    return true;
  }

  /**
   * Local session send heartbeat to local worker to keep its temporary folder.
   *
   * @param sessionId the id of the client heartbeating
   * @param metrics a list of the client metrics that were collected between this heartbeat and
   *                the last. Each value in the list represents a specific metric based on the
   *                index.
   */
  @Override
  public void sessionHeartbeat(long sessionId, List<Long> metrics) {
    mWorker.sessionHeartbeat(sessionId, metrics);
  }
}
