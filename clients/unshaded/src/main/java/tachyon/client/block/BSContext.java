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

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.base.Preconditions;

import tachyon.client.BlockMasterClient;
import tachyon.client.ClientContext;
import tachyon.thrift.NetAddress;
import tachyon.thrift.WorkerInfo;
import tachyon.util.ThreadFactoryUtils;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.worker.ClientMetrics;
import tachyon.worker.WorkerClient;

/**
 * A shared context in each client JVM for common Block Store client functionality such as a pool of
 * master clients and a pool of local worker clients. Any remote clients will be created and
 * destroyed on a per use basis.
 */
public enum BSContext {
  INSTANCE;

  private BlockMasterClientPool mBlockMasterClientPool;
  private BlockWorkerClientPool mLocalBlockWorkerClientPool;
  private final ExecutorService mRemoteBlockWorkerExecutor;

  /**
   * Creates a new block stream context.
   */
  BSContext() {
    mBlockMasterClientPool =
        new BlockMasterClientPool(ClientContext.getMasterAddress());
    // TODO: Get the capacity from configuration
    final int CAPACITY = 10;
    mRemoteBlockWorkerExecutor =
        Executors.newFixedThreadPool(CAPACITY,
            ThreadFactoryUtils.build("remote-block-worker-heartbeat-%d", true));

    NetAddress localWorkerAddress =
        getWorkerAddress(NetworkAddressUtils.getLocalHostName(ClientContext.getConf()));

    // If the local worker is not available, do not initialize the local worker client pool
    if (null == localWorkerAddress) {
      mLocalBlockWorkerClientPool = null;
    } else {
      mLocalBlockWorkerClientPool =
          new BlockWorkerClientPool(localWorkerAddress);
    }
  }

  /**
   * Re-initializes the Block Store context. This method should only be used in ClientContext.
   *
   * TODO: Prevent classes other than ClientContext from accessing this method.
   */
  public void resetContext() {
    mBlockMasterClientPool.close();
    if (mLocalBlockWorkerClientPool != null) {
      mLocalBlockWorkerClientPool.close();
    }
    mBlockMasterClientPool =
        new BlockMasterClientPool(ClientContext.getMasterAddress());
    NetAddress localWorkerAddress =
        getWorkerAddress(NetworkAddressUtils.getLocalHostName(ClientContext.getConf()));

    // If the local worker is not available, do not initialize the local worker client pool
    if (null == localWorkerAddress) {
      mLocalBlockWorkerClientPool = null;
    } else {
      mLocalBlockWorkerClientPool =
          new BlockWorkerClientPool(localWorkerAddress);
    }
  }

  private NetAddress getWorkerAddress(String hostname) {
    BlockMasterClient masterClient = acquireMasterClient();
    try {
      List<WorkerInfo> workers = masterClient.getWorkerInfoList();
      if (hostname.isEmpty()) {
        // TODO: Do this in a more defined way
        return workers.get(0).getAddress();
      }
      for (WorkerInfo worker : workers) {
        if (worker.getAddress().getMHost().equals(hostname)) {
          return worker.getAddress();
        }
      }
      return null;
    } catch (Exception e) {
      return null;
    } finally {
      releaseMasterClient(masterClient);
    }
  }

  /**
   * Acquires a block master client from the block master client pool.
   *
   * @return the acquired block master client
   */
  public BlockMasterClient acquireMasterClient() {
    return mBlockMasterClientPool.acquire();
  }

  /**
   * Releases a block master client into the block master client pool.
   *
   * @param masterClient a block master client to release
   */
  public void releaseMasterClient(BlockMasterClient masterClient) {
    mBlockMasterClientPool.release(masterClient);
  }

  /**
   * Obtains a worker client to a worker in the system. A local client is preferred to be returned
   * but not guaranteed. The caller should use {@link WorkerClient#isLocal} to verify if the client
   * is local before assuming so.
   *
   * @return a WorkerClient to a worker in the Tachyon system
   */
  public WorkerClient acquireWorkerClient() {
    if (mLocalBlockWorkerClientPool != null) {
      return mLocalBlockWorkerClientPool.acquire();
    } else {
      return acquireRemoteWorkerClient("");
    }
  }

  /**
   * Obtains a worker client to the worker with the given hostname in the system.
   *
   * @param hostname the hostname of the worker to get a client to, empty String indicates all
   *        workers are eligible
   * @return a WorkerClient connected to the worker with the given hostname
   */
  public WorkerClient acquireWorkerClient(String hostname) {
    if (hostname.equals(NetworkAddressUtils.getLocalHostName(ClientContext.getConf()))) {
      if (mLocalBlockWorkerClientPool != null) {
        return mLocalBlockWorkerClientPool.acquire();
      }
      // TODO: Recover from initial worker failure
      throw new RuntimeException("No Tachyon worker available for host: " + hostname);
    }
    return acquireRemoteWorkerClient(hostname);
  }

  private WorkerClient acquireRemoteWorkerClient(String hostname) {
    Preconditions.checkArgument(
        !hostname.equals(NetworkAddressUtils.getLocalHostName(ClientContext.getConf())),
        "Acquire Remote Worker Client cannot not be called with local hostname");
    NetAddress workerAddress = getWorkerAddress(hostname);

    // If we couldn't find a worker, crash
    if (null == workerAddress) {
      // TODO: Better exception usage
      throw new RuntimeException("No Tachyon worker available for host: " + hostname);
    }
    long clientId = ClientContext.getRandomNonNegativeLong();
    return new WorkerClient(workerAddress, mRemoteBlockWorkerExecutor, ClientContext.getConf(),
        clientId, false, new ClientMetrics());
  }

  /**
   * Releases the WorkerClient back to the client pool, or destroys it if it was a remote client.
   *
   * @param workerClient the worker client to release, the client should not be accessed after this
   *        method is called
   */
  public void releaseWorkerClient(WorkerClient workerClient) {
    // If the client is local and the pool exists, release the client to the pool, otherwise just
    // close the client
    if (mLocalBlockWorkerClientPool != null && workerClient.isLocal()) {
      mLocalBlockWorkerClientPool.release(workerClient);
    } else {
      workerClient.close();
    }
  }

  /**
   * Determines if a local worker was available during the initialization of the client.
   *
   * @return true if there was a local worker, false otherwise
   */
  // TODO: Handle the case when the local worker starts up after the client or shuts down before
  // TODO: the client does
  public boolean hasLocalWorker() {
    return mLocalBlockWorkerClientPool != null;
  }
}
