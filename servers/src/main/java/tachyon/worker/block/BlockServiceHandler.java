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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.Sessions;
import tachyon.StorageLevelAlias;
import tachyon.exception.TachyonException;
import tachyon.thrift.TachyonTException;
import tachyon.thrift.ThriftIOException;
import tachyon.thrift.WorkerService;

/**
 * Handles all thrift RPC calls to the worker. This class is a thrift server implementation and is
 * thread safe.
 */
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
   * @throws TachyonTException if a tachyon error occurs
   */
  @Override
  public void accessBlock(long blockId) throws TachyonTException {
    try {
      mWorker.accessBlock(Sessions.ACCESS_BLOCK_SESSION_ID, blockId);
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  /**
   * This should be called in order to persist a file that was written directly to the under storage
   * system via a THROUGH type write. This will update the master with the appropriate metadata for
   * the new block.
   *
   * @param fileId the file id
   * @param nonce nonce a nonce used for temporary file creation
   * @param path the UFS path of the file
   */
  @Override
  public void persistFile(long fileId, long nonce, String path)
      throws TachyonTException, ThriftIOException {
    try {
      mWorker.persistFile(fileId, nonce, path);
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  // TODO(calvin): Make this supported again.
  @Override
  public boolean asyncCheckpoint(long fileId) throws TachyonTException {
    return false;
  }

  /**
   * Used to cache a block into Tachyon space, worker will move the temporary block file from
   * session folder to data folder, and update the space usage information related. then update the
   * block information to master.
   *
   * @param sessionId the id of the client requesting the commit
   * @param blockId the id of the block to commit
   * @throws TachyonTException if a tachyon error occurs
   * @throws ThriftIOException if an I/O error occurs
   */
  // TODO(calvin): Reconsider this exception handling.
  @Override
  public void cacheBlock(long sessionId, long blockId) throws TachyonTException, ThriftIOException {
    try {
      mWorker.commitBlock(sessionId, blockId);
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  /**
   * Used to cancel a block which is being written. worker will delete the temporary block file and
   * the location and space information related, then reclaim space allocated to the block.
   *
   * @param sessionId the id of the client requesting the abort
   * @param blockId the id of the block to be aborted
   * @throws TachyonTException if a tachyon error occurs
   * @throws ThriftIOException if an I/O error occurs
   */
  @Override
  public void cancelBlock(long sessionId, long blockId)
      throws TachyonTException, ThriftIOException {
    try {
      mWorker.abortBlock(sessionId, blockId);
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }

  /**
   * Lock the file in Tachyon's space while the session is reading it, and the path of the block
   * file locked will be returned.
   *
   * @param blockId the id of the block to be locked
   * @param sessionId the id of the session
   * @throws TachyonTException if a tachyon error occurs
   */
  @Override
  public String lockBlock(long blockId, long sessionId) throws TachyonTException {
    try {
      long lockId = mWorker.lockBlock(sessionId, blockId);
      return mWorker.readBlock(sessionId, blockId, lockId);
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  /**
   * Used to promote block on under storage layer to top storage layer when there are more than one
   * storage layers in Tachyon's space. return true if the block is successfully promoted, false
   * otherwise.
   *
   * @param blockId the id of the block to move to the top layer
   * @throws TachyonTException if a tachyon error occurs
   * @throws ThriftIOException if an I/O error occurs
   */
  // TODO(calvin): This may be better as void.
  @Override
  public boolean promoteBlock(long blockId) throws TachyonTException, ThriftIOException {
    try {
      // TODO(calvin): Make the top level configurable.
      mWorker.moveBlock(Sessions.MIGRATE_DATA_SESSION_ID, blockId,
          StorageLevelAlias.MEM.getValue());
      return true;
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }

  /**
   * Used to allocate location and space for a new coming block, worker will choose the appropriate
   * storage directory which fits the initial block size by some allocation strategy, and the
   * temporary file path of the block file will be returned. if there is no enough space on Tachyon
   * storage WorkerOutOfSpaceException will be thrown, if the file is already being written by the
   * session, FileAlreadyExistsException will be thrown.
   *
   * @param sessionId the id of the client requesting the create
   * @param blockId the id of the new block to create
   * @param initialBytes the initial number of bytes to allocate for this block
   * @throws TachyonTException if a tachyon error occurs
   * @throws ThriftIOException if an I/O error occurs
   */
  @Override
  public String requestBlockLocation(long sessionId, long blockId, long initialBytes)
      throws TachyonTException, ThriftIOException {
    try {
      // NOTE: right now, we ask allocator to allocate new blocks in MEM tier
      return mWorker.createBlock(sessionId, blockId, StorageLevelAlias.MEM.getValue(),
          initialBytes);
    } catch (TachyonException e) {
      throw e.toTachyonTException();
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
   * @throws TachyonTException if a tachyon error occurs
   */
  @Override
  public boolean unlockBlock(long blockId, long sessionId) throws TachyonTException {
    try {
      mWorker.unlockBlock(sessionId, blockId);
      return true;
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  /**
   * Local session send heartbeat to local worker to keep its temporary folder.
   *
   * @param sessionId the id of the client heartbeating
   * @param metrics a list of the client metrics that were collected between this heartbeat and the
   *        last. Each value in the list represents a specific metric based on the index.
   */
  @Override
  public void sessionHeartbeat(long sessionId, List<Long> metrics) {
    mWorker.sessionHeartbeat(sessionId, metrics);
  }
}
