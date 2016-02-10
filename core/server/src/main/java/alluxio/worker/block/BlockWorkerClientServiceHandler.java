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

package alluxio.worker.block;

import alluxio.Constants;
import alluxio.Sessions;
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.exception.AlluxioException;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.BlockWorkerClientService;
import alluxio.thrift.LockBlockResult;
import alluxio.thrift.ThriftIOException;
import alluxio.worker.WorkerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class is a Thrift handler for block worker RPCs invoked by an Alluxio client.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1624)
public final class BlockWorkerClientServiceHandler implements BlockWorkerClientService.Iface {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Block Worker handle that carries out most of the operations. */
  private final BlockWorker mWorker;
  /** Association between storage tier aliases and ordinals ond this worker. */
  private final StorageTierAssoc mStorageTierAssoc;

  /**
   * Creates a new instance of {@link BlockWorkerClientServiceHandler}.
   *
   * @param worker block worker handler
   */
  public BlockWorkerClientServiceHandler(BlockWorker worker) {
    mWorker = worker;
    mStorageTierAssoc = new WorkerStorageTierAssoc(WorkerContext.getConf());
  }

  @Override
  public long getServiceVersion() {
    return Constants.BLOCK_WORKER_CLIENT_SERVICE_VERSION;
  }

  /**
   * This should be called whenever a client does a direct read in order to update the worker's
   * components that may care about the access times of the blocks (for example, Evictor, UI).
   *
   * @param blockId the id of the block to access
   * @throws AlluxioTException if an Alluxio error occurs
   */
  @Override
  public void accessBlock(long blockId) throws AlluxioTException {
    try {
      mWorker.accessBlock(Sessions.ACCESS_BLOCK_SESSION_ID, blockId);
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    }
  }

  // TODO(calvin): Make this supported again.
  @Override
  public boolean asyncCheckpoint(long fileId) throws AlluxioTException {
    return false;
  }

  /**
   * Used to cache a block into Alluxio space, worker will move the temporary block file from
   * session folder to data folder, and update the space usage information related. then update the
   * block information to master.
   *
   * @param sessionId the id of the client requesting the commit
   * @param blockId the id of the block to commit
   * @throws AlluxioTException if an Alluxio error occurs
   * @throws ThriftIOException if an I/O error occurs
   */
  @Override
  public void cacheBlock(long sessionId, long blockId) throws AlluxioTException, ThriftIOException {
    try {
      mWorker.commitBlock(sessionId, blockId);
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    }
  }

  /**
   * Used to cancel a block which is being written. worker will delete the temporary block file and
   * the location and space information related, then reclaim space allocated to the block.
   *
   * @param sessionId the id of the client requesting the abort
   * @param blockId the id of the block to be aborted
   * @throws AlluxioTException if an Alluxio error occurs
   * @throws ThriftIOException if an I/O error occurs
   */
  @Override
  public void cancelBlock(long sessionId, long blockId)
      throws AlluxioTException, ThriftIOException {
    try {
      mWorker.abortBlock(sessionId, blockId);
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }

  /**
   * Locks the file in Alluxio's space while the session is reading it.
   *
   * @param blockId the id of the block to be locked
   * @param sessionId the id of the session
   * @return the path of the block file locked
   * @throws AlluxioTException if an Alluxio error occurs
   */
  @Override
  public LockBlockResult lockBlock(long blockId, long sessionId) throws AlluxioTException {
    try {
      long lockId = mWorker.lockBlock(sessionId, blockId);
      return new LockBlockResult(lockId, mWorker.readBlock(sessionId, blockId, lockId));
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    }
  }

  /**
   * Used to promote block on under storage layer to top storage layer when there are more than one
   * storage layers in Alluxio's space.
   * otherwise.
   *
   * @param blockId the id of the block to move to the top layer
   * @return true if the block is successfully promoted, otherwise false
   * @throws AlluxioTException if an Alluxio error occurs
   * @throws ThriftIOException if an I/O error occurs
   */
  // TODO(calvin): This may be better as void.
  @Override
  public boolean promoteBlock(long blockId) throws AlluxioTException, ThriftIOException {
    try {
      // TODO(calvin): Make the top level configurable.
      mWorker.moveBlock(Sessions.MIGRATE_DATA_SESSION_ID, blockId, mStorageTierAssoc.getAlias(0));
      return true;
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }

  /**
   * Used to allocate location and space for a new coming block, worker will choose the appropriate
   * storage directory which fits the initial block size by some allocation strategy. If there is
   * not enough space on Alluxio storage {@link alluxio.exception.WorkerOutOfSpaceException} will be
   * thrown, if the file is already being written by the session,
   * {@link alluxio.exception.FileAlreadyExistsException} will be thrown.
   *
   * @param sessionId the id of the client requesting the create
   * @param blockId the id of the new block to create
   * @param initialBytes the initial number of bytes to allocate for this block
   * @return the temporary file path of the block file
   * @throws AlluxioTException if an Alluxio error occurs
   * @throws ThriftIOException if an I/O error occurs
   */
  @Override
  public String requestBlockLocation(long sessionId, long blockId, long initialBytes)
      throws AlluxioTException, ThriftIOException {
    try {
      // NOTE: right now, we ask allocator to allocate new blocks in top tier
      return mWorker.createBlock(sessionId, blockId, mStorageTierAssoc.getAlias(0), initialBytes);
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }

  /**
   * Used to request space for some block file.
   *
   * @param sessionId the id of the client requesting space
   * @param blockId the id of the block to add the space to, this must be a temporary block
   * @param requestBytes the amount of bytes to add to the block
   * @return true if the worker successfully allocates space for the block on block’s location,
   *         false if there is no enough space
   */
  @Override
  public boolean requestSpace(long sessionId, long blockId, long requestBytes) {
    try {
      mWorker.requestSpace(sessionId, blockId, requestBytes);
      return true;
    } catch (Exception e) {
      LOG.error("Failed to request {} bytes for block: {}", requestBytes, blockId, e);
    }
    return false;
  }

  /**
   * Used to unlock a block after the block is accessed, if the block is to be removed, delete the
   * block file.
   *
   * @param blockId the id of the block to unlock
   * @param sessionId the id of the client requesting the unlock
   * @return true if successfully unlock the block, return false if the block is not
   * found or failed to delete the block
   * @throws AlluxioTException if an Alluxio error occurs
   */
  @Override
  public boolean unlockBlock(long blockId, long sessionId) throws AlluxioTException {
    try {
      mWorker.unlockBlock(sessionId, blockId);
      return true;
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
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
