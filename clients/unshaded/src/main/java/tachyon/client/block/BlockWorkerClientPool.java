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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import tachyon.client.ClientContext;
import tachyon.client.ResourcePool;
import tachyon.thrift.NetAddress;
import tachyon.util.ThreadFactoryUtils;
import tachyon.worker.ClientMetrics;
import tachyon.worker.WorkerClient;

/**
 * Class for managing local block worker clients. After obtaining a client with {@link
 * ResourcePool#acquire}, {@link ResourcePool#release} must be called when the thread is done
 * using the client.
 */
public class BlockWorkerClientPool extends ResourcePool<WorkerClient> {
  /**
   * The capacity for this pool must be large, since each block written will hold a client until
   * the block is committed at the end of the file completion.
   */
  private static final int CAPACITY = 10000;
  private final ExecutorService mExecutorService;
  private final NetAddress mWorkerNetAddress;

  /**
   * Creates a new block worker client pool.
   *
   * @param workerAddress the worker address
   */
  public BlockWorkerClientPool(NetAddress workerAddress) {
    // TODO(calvin): Get the capacity from configuration.
    super(CAPACITY);
    mExecutorService = Executors.newFixedThreadPool(CAPACITY, ThreadFactoryUtils.build(
        "block-worker-heartbeat-%d", true));
    mWorkerNetAddress = workerAddress;
  }

  @Override
  public void close() {
    // TODO(calvin): Consider collecting all the clients and shutting them down.
    mExecutorService.shutdown();
  }

  @Override
  public void release(WorkerClient workerClient) {
    workerClient.createNewSession(ClientContext.getRandomNonNegativeLong());
    super.release(workerClient);
  }

  @Override
  protected WorkerClient createNewResource() {
    long clientId = ClientContext.getRandomNonNegativeLong();
    return new WorkerClient(mWorkerNetAddress, mExecutorService, ClientContext.getConf(),
        clientId, true, new ClientMetrics());
  }
}
