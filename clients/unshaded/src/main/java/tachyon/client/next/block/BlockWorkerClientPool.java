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

package tachyon.client.next.block;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import tachyon.Constants;
import tachyon.client.UserMasterClient;
import tachyon.client.next.ClientContext;
import tachyon.client.next.ResourcePool;
import tachyon.conf.TachyonConf;
import tachyon.thrift.NetAddress;
import tachyon.util.ThreadFactoryUtils;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.worker.ClientMetrics;
import tachyon.worker.next.WorkerClient;

/**
 * Class for managing local block worker clients. After obtaining a client with {@link
 * ResourcePool#acquire}, {@link ResourcePool#release} must be called when the thread is done
 * using the client.
 */
public class BlockWorkerClientPool extends ResourcePool<WorkerClient> {
  private final ExecutorService mExecutorService;
  private final NetAddress mWorkerNetAddress;
  private final TachyonConf mTachyonConf;

  public BlockWorkerClientPool(NetAddress workerNetAddress, TachyonConf conf) {
    // TODO: Get capacity from conf
    super(10000);
    mExecutorService = Executors.newFixedThreadPool(10, ThreadFactoryUtils.build(
        "block-worker-heartbeat-%d", true));
    mWorkerNetAddress = workerNetAddress;
    mTachyonConf = conf;
  }

  @Override
  public void close() {
    // TODO: Consider collecting all the clients and shutting them down
    mExecutorService.shutdown();
  }

  @Override
  public WorkerClient createNewResource() {
    UserMasterClient userMasterClient = ClientContext.acquireUserMasterClient();
    try {
      long clientId = userMasterClient.getUserId();
      return new WorkerClient(mWorkerNetAddress, mExecutorService, ClientContext.getConf(),
          clientId, true, new ClientMetrics());
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to create new BlockWorker Client.", ioe);
    } finally {
      ClientContext.releaseUserMasterClient(userMasterClient);
    }
  }

  @Override
  public void release(WorkerClient workerClient) {
    UserMasterClient userMasterClient = ClientContext.acquireUserMasterClient();
    try {
      workerClient.createNewSession(userMasterClient.getUserId());
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to create new BlockWorker Client.", ioe);
    } finally {
      ClientContext.releaseUserMasterClient(userMasterClient);
    }
    super.release(workerClient);
  }
}
