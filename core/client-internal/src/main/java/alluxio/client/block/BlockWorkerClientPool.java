/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.block;

import alluxio.Constants;
import alluxio.client.ClientContext;
import alluxio.client.ClientUtils;
import alluxio.resource.ResourcePool;
import alluxio.wire.WorkerNetAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Class for managing local block worker clients. After obtaining a client with
 * {@link ResourcePool#acquire()}, {@link ResourcePool#release(Object)} must be called when the
 * thread is done using the client.
 */
@ThreadSafe
final class BlockWorkerClientPool extends ResourcePool<BlockWorkerClient> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  /**
   * The capacity for this pool must be large, since each block written will hold a client until
   * the block is committed at the end of the file completion.
   */
  private final WorkerNetAddress mWorkerNetAddress;

  /**
   * Creates a new block worker client pool.
   *
   * @param workerAddress the worker address
   */
  public BlockWorkerClientPool(WorkerNetAddress workerAddress) {
    super(ClientContext.getConf().getInt(Constants.USER_BLOCK_WORKER_CLIENT_THREADS));
    mWorkerNetAddress = workerAddress;
  }

  @Override
  public void close() {
    // TODO(calvin): Consider collecting all the clients and shutting them down.
  }

  @Override
  public void release(BlockWorkerClient blockWorkerClient) {
    try {
      // Heartbeat to send the client metrics.
      blockWorkerClient.sessionHeartbeat();
    } catch (Exception e) {
      LOG.warn("Failed sending client metrics before releasing the worker client", e);
    }
    blockWorkerClient.createNewSession(ClientUtils.getRandomNonNegativeLong());
    super.release(blockWorkerClient);
  }

  @Override
  protected BlockWorkerClient createNewResource() {
    long clientId = ClientUtils.getRandomNonNegativeLong();
    return new BlockWorkerClient(mWorkerNetAddress, ClientContext.getExecutorService(),
        ClientContext.getConf(), clientId, true, ClientContext.getClientMetrics());
  }
}
