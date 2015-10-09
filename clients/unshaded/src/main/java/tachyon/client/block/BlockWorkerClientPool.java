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

package tachyon.client.block;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.client.ClientContext;
import tachyon.resource.ResourcePool;
import tachyon.thrift.NetAddress;
import tachyon.worker.WorkerClient;

/**
 * Class for managing local block worker clients. After obtaining a client with {@link
 * ResourcePool#acquire}, {@link ResourcePool#release} must be called when the thread is done
 * using the client.
 */
public final class BlockWorkerClientPool extends ResourcePool<WorkerClient> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  /**
   * The capacity for this pool must be large, since each block written will hold a client until
   * the block is committed at the end of the file completion.
   */
  private final NetAddress mWorkerNetAddress;

  /**
   * Creates a new block worker client pool.
   *
   * @param workerAddress the worker address
   */
  public BlockWorkerClientPool(NetAddress workerAddress) {
    super(ClientContext.getConf().getInt(Constants.USER_BLOCK_WORKER_CLIENT_THREADS));
    mWorkerNetAddress = workerAddress;
  }

  @Override
  public void close() {
    // TODO(calvin): Consider collecting all the clients and shutting them down.
  }

  @Override
  public void release(WorkerClient workerClient) {
    try {
      // Heartbeat to send the client metrics.
      workerClient.sessionHeartbeat();
    } catch (IOException ioe) {
      LOG.warn("Failed sending client metrics before releasing the worker client", ioe);
    }
    workerClient.createNewSession(ClientContext.getRandomNonNegativeLong());
    super.release(workerClient);
  }

  @Override
  protected WorkerClient createNewResource() {
    long clientId = ClientContext.getRandomNonNegativeLong();
    return new WorkerClient(mWorkerNetAddress, ClientContext.getExecutorService(),
        ClientContext.getConf(), clientId, true, ClientContext.getClientMetrics());
  }
}
