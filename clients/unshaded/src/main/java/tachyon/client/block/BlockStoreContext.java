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
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

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
 * destroyed on a per use basis. This class is thread safe.
 */
public enum BlockStoreContext {
  INSTANCE;

  private BlockMasterClientPool mBlockMasterClientPool;
  private BlockWorkerClientPool mLocalBlockWorkerClientPool;

  private boolean mLocalBlockWorkerClientPoolInitialized;

  /**
   * Creates a new block store context.
   */
  BlockStoreContext() {
    reset();
  }

  /**
   * Initializes {#mLocalBlockWorkerClientPool}. This method is supposed be called in a lazy manner.
   */
  private synchronized void initializeLocalBlockWorkerClientPool() {
    NetAddress localWorkerAddress =
        getWorkerAddress(NetworkAddressUtils.getLocalHostName(ClientContext.getConf()));

    // If the local worker is not available, do not initialize the local worker client pool.
    if (localWorkerAddress == null) {
      mLocalBlockWorkerClientPool = null;
    } else {
      mLocalBlockWorkerClientPool = new BlockWorkerClientPool(localWorkerAddress);
    }
    mLocalBlockWorkerClientPoolInitialized = true;
  }

  /**
   * Gets the worker address based on its hostname by querying the master.
   *
   * @param hostname hostname of the worker to query, empty string denotes any worker
   * @return NetAddress of hostname, or null if no worker found
   */
  private synchronized NetAddress getWorkerAddress(String hostname) {
    BlockMasterClient masterClient = acquireMasterClient();
    try {
      List<WorkerInfo> workers = masterClient.getWorkerInfoList();
      if (hostname.isEmpty() && !workers.isEmpty()) {
        // TODO(calvin): Do this in a more defined way.
        return workers.get(0).getAddress();
      }
      for (WorkerInfo worker : workers) {
        if (worker.getAddress().getHost().equals(hostname)) {
          return worker.getAddress();
        }
      }
    } catch (IOException ioe) {
      Throwables.propagate(ioe);
    } finally {
      releaseMasterClient(masterClient);
    }
    return null;
  }

  /**
   * Acquires a block master client from the block master client pool.
   *
   * @return the acquired block master client
   */
  public synchronized BlockMasterClient acquireMasterClient() {
    return mBlockMasterClientPool.acquire();
  }

  /**
   * Releases a block master client into the block master client pool.
   *
   * @param masterClient a block master client to release
   */
  public synchronized void releaseMasterClient(BlockMasterClient masterClient) {
    mBlockMasterClientPool.release(masterClient);
  }

  /**
   * Obtains a worker client to a worker in the system. A local client is preferred to be returned
   * but not guaranteed. The caller should use {@link WorkerClient#isLocal} to verify if the client
   * is local before assuming so.
   *
   * @return a WorkerClient to a worker in the Tachyon system
   */
  public synchronized WorkerClient acquireWorkerClient() {
    WorkerClient client = acquireLocalWorkerClient();
    if (client == null) {
      // Get a worker client for any worker in the system.
      return acquireRemoteWorkerClient("");
    }
    return client;
  }

  /**
   * Obtains a worker client to the worker with the given hostname in the system.
   *
   * @param hostname the hostname of the worker to get a client to, empty String indicates all
   *        workers are eligible
   * @return a WorkerClient connected to the worker with the given hostname
   */
  public synchronized WorkerClient acquireWorkerClient(String hostname) {
    WorkerClient client;
    if (hostname.equals(NetworkAddressUtils.getLocalHostName(ClientContext.getConf()))) {
      client = acquireLocalWorkerClient();
      if (client == null) {
        // TODO(calvin): Recover from initial worker failure.
        throw new RuntimeException("No Tachyon worker available for host: " + hostname);
      }
    } else {
      client = acquireRemoteWorkerClient(hostname);
    }
    return client;
  }

  /**
   * Obtains a worker client on the local worker in the system.
   *
   * @return a WorkerClient to a worker in the Tachyon system or null if failed
   */
  public synchronized WorkerClient acquireLocalWorkerClient() {
    if (!mLocalBlockWorkerClientPoolInitialized) {
      initializeLocalBlockWorkerClientPool();
    }

    if (mLocalBlockWorkerClientPool == null) {
      return null;
    }
    return mLocalBlockWorkerClientPool.acquire();
  }

  /**
   * Obtains a non local worker client based on the hostname. Illegal argument exception is thrown
   * if the hostname is the local hostname. Runtime exception is thrown if the client cannot be
   * created with a connection to the hostname.
   *
   * @param hostname the worker hostname to connect to, empty string for any worker
   * @return a worker client with a connection to the specified hostname
   */
  private synchronized WorkerClient acquireRemoteWorkerClient(String hostname) {
    Preconditions.checkArgument(
        !hostname.equals(NetworkAddressUtils.getLocalHostName(ClientContext.getConf())),
        "Acquire Remote Worker Client cannot not be called with local hostname");
    NetAddress workerAddress = getWorkerAddress(hostname);

    // If we couldn't find a worker, crash.
    if (workerAddress == null) {
      // TODO(calvin): Better exception usage.
      throw new RuntimeException("No Tachyon worker available for host: " + hostname);
    }
    long clientId = ClientContext.getRandomNonNegativeLong();
    return new WorkerClient(workerAddress, ClientContext.getExecutorService(),
        ClientContext.getConf(), clientId, false, new ClientMetrics());
  }

  /**
   * Releases the WorkerClient back to the client pool, or destroys it if it was a remote client.
   *
   * @param workerClient the worker client to release, the client should not be accessed after this
   *        method is called
   */
  public synchronized void releaseWorkerClient(WorkerClient workerClient) {
    // If the client is local and the pool exists, release the client to the pool, otherwise just
    // close the client.
    if (workerClient.isLocal()) {
      // Return local worker client to its resource pool.
      Preconditions.checkState(mLocalBlockWorkerClientPool != null);
      mLocalBlockWorkerClientPool.release(workerClient);
    } else {
      // Destroy remote worker client.
      workerClient.close();
    }
  }

  /**
   * Determines if a local worker was available during the initialization of the client.
   *
   * @return true if there was a local worker, false otherwise
   */
  // TODO(calvin): Handle the case when the local worker starts up after the client or shuts down
  // before the client does.
  public synchronized boolean hasLocalWorker() {
    if (!mLocalBlockWorkerClientPoolInitialized) {
      initializeLocalBlockWorkerClientPool();
    }
    return mLocalBlockWorkerClientPool != null;
  }

  /**
   * Re-initializes the Block Store context. This method should only be used in
   * {@link ClientContext}.
   */
  public synchronized void reset() {
    if (mBlockMasterClientPool != null) {
      mBlockMasterClientPool.close();
    }
    if (mLocalBlockWorkerClientPool != null) {
      mLocalBlockWorkerClientPool.close();
    }
    mBlockMasterClientPool = new BlockMasterClientPool(ClientContext.getMasterAddress());
    // mLocalBlockWorkerClientPool is initialized in a lazy manner
    mLocalBlockWorkerClientPoolInitialized = false;
  }
}
