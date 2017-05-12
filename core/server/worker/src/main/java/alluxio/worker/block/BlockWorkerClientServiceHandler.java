/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block;

import alluxio.Constants;
import alluxio.RpcUtils;
import alluxio.RpcUtils.RpcCallable;
import alluxio.RpcUtils.RpcCallableThrowsIOException;
import alluxio.Sessions;
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.UnexpectedAlluxioException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.BlockWorkerClientService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class is a Thrift handler for block worker RPCs invoked by an Alluxio client. These RPCs are
 * no longer supported as of 1.5.0. All methods will throw {@link UnsupportedOperationException}.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1624)
public final class BlockWorkerClientServiceHandler implements BlockWorkerClientService.Iface {
  private static final Logger LOG = LoggerFactory.getLogger(BlockWorkerClientServiceHandler.class);
  private static final String UNSUPPORTED_MESSAGE = "Unsupported as of v1.5.0";

  /**
   * Creates a new instance of {@link BlockWorkerClientServiceHandler}.
   *
   * @param worker block worker handler
   */
  public BlockWorkerClientServiceHandler(BlockWorker worker) {}

  @Override
  public long getServiceVersion() {
    return Constants.BLOCK_WORKER_CLIENT_SERVICE_VERSION;
  }

  /**
   * This should be called whenever a client does a direct read in order to update the worker's
   * components that may care about the access times of the blocks (for example, Evictor, UI).
   *
   * @param blockId the id of the block to access
   * @throws UnsupportedOperationException always
   */
  @Override
  public void accessBlock(final long blockId) throws AlluxioTException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  /**
   * Used to cache a block into Alluxio space, worker will move the temporary block file from
   * session folder to data folder, and update the space usage information related. then update the
   * block information to master.
   *
   * @param sessionId the id of the client requesting the commit
   * @param blockId the id of the block to commit
   * @throws UnsupportedOperationException always
   */
  @Override
  public void cacheBlock(final long sessionId, final long blockId) throws AlluxioTException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  /**
   * Used to cancel a block which is being written. worker will delete the temporary block file and
   * the location and space information related, then reclaim space allocated to the block.
   *
   * @param sessionId the id of the client requesting the abort
   * @param blockId the id of the block to be aborted
   * @throws UnsupportedOperationException always
   */
  @Override
  public void cancelBlock(final long sessionId, final long blockId) throws AlluxioTException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  /**
   * Used to promote block on under storage layer to top storage layer when there are more than one
   * storage layers in Alluxio's space.
   * otherwise.
   *
   * @param blockId the id of the block to move to the top layer
   * @return true if the block is successfully promoted, otherwise false
   * @throws UnsupportedOperationException always
   */
  // TODO(calvin): This may be better as void.
  @Override
  public boolean promoteBlock(final long blockId) throws AlluxioTException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  /**
   * Used to remove a block in Alluxio storage. Worker will delete the block file and
   * reclaim space allocated to the block.
   *
   * @param blockId the id of the block to be removed
   * @throws UnsupportedOperationException always
   */
  @Override
  public void removeBlock(final long blockId) throws AlluxioTException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
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
   * @param writeTier policy used to choose tier for this block
   * @return the temporary file path of the block file
   * @throws UnsupportedOperationException always
   */
  @Override
  public String requestBlockLocation(final long sessionId, final long blockId,
      final long initialBytes, final int writeTier) throws AlluxioTException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  /**
   * Requests space for a block.
   *
   * @param sessionId the id of the client requesting space
   * @param blockId the id of the block to add the space to, this must be a temporary block
   * @param requestBytes the amount of bytes to add to the block
   * @return true if the worker successfully allocates space for the block on block’s location,
   *         false if there is not enough space
   * @throws UnsupportedOperationException always
   */
  @Override
  public boolean requestSpace(final long sessionId, final long blockId, final long requestBytes)
      throws AlluxioTException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }

  /**
   * Local session send heartbeat to local worker to keep its temporary folder.
   *
   * @param sessionId the id of the client heartbeating
   * @param metrics deprecated
   * @throws UnsupportedOperationException always
   */
  @Override
  public void sessionHeartbeat(final long sessionId, final List<Long> metrics)
      throws AlluxioTException {
    throw new UnsupportedOperationException(UNSUPPORTED_MESSAGE);
  }
}
