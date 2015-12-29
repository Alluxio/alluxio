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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import tachyon.client.ClientContext;
import tachyon.client.Utils;
import tachyon.client.worker.BlockWorkerClient;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.PreconditionMessage;
import tachyon.thrift.NetAddress;
import tachyon.thrift.WorkerInfo;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.worker.ClientMetrics;

/**
 * A shared context in each client JVM for common block master client functionality such as a pool
 * of master clients and a pool of local worker clients. Any remote clients will be created and
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
   * Initializes {@link #mLocalBlockWorkerClientPool}. This method is supposed be called in a lazy
   * manner.
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
   * @return {@link NetAddress} of hostname, or null if no worker found
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
    } catch (Exception e) {
      Throwables.propagate(e);
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
  public BlockMasterClient acquireMasterClient() {
    return mBlockMasterClientPool.acquire();
  }

  /**
   * Releases a block master client into the block master client pool.
   *
   * NOTE: the client pool is already thread-safe. Synchronizing on {@link BlockStoreContext} will
   * lead to deadlock: thread A acquired a client and awaits for {@link BlockStoreContext} to
   * release the client, while thread B holds the lock of {@link BlockStoreContext} but waits for
   * available clients.
   *
   * @param masterClient a block master client to release
   */
  public void releaseMasterClient(BlockMasterClient masterClient) {
    mBlockMasterClientPool.release(masterClient);
  }

  /**
   * Obtains a worker client to a worker in the system. A local client is preferred to be returned
   * but not guaranteed. The caller should use {@link BlockWorkerClient#isLocal()} to verify if the
   * client is local before assuming so.
   *
   * @return a {@link BlockWorkerClient} to a worker in the Tachyon system
   */
  public synchronized BlockWorkerClient acquireWorkerClient() {
    BlockWorkerClient client = acquireLocalWorkerClient();
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
   * @return a {@link BlockWorkerClient} connected to the worker with the given hostname
   * @throws IOException if no Tachyon worker is available for the given hostname
   */
  public synchronized BlockWorkerClient acquireWorkerClient(String hostname) throws IOException {
    BlockWorkerClient client;
    if (hostname.equals(NetworkAddressUtils.getLocalHostName(ClientContext.getConf()))) {
      client = acquireLocalWorkerClient();
      if (client == null) {
        throw new IOException(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage(hostname));
      }
    } else {
      client = acquireRemoteWorkerClient(hostname);
    }
    return client;
  }

  /**
   * Obtains a worker client on the local worker in the system.
   *
   * @return a {@link BlockWorkerClient} to a worker in the Tachyon system or null if failed
   */
  public synchronized BlockWorkerClient acquireLocalWorkerClient() {
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
  private synchronized BlockWorkerClient acquireRemoteWorkerClient(String hostname) {
    Preconditions.checkArgument(
        !hostname.equals(NetworkAddressUtils.getLocalHostName(ClientContext.getConf())),
        PreconditionMessage.REMOTE_CLIENT_BUT_LOCAL_HOSTNAME);
    NetAddress workerAddress = getWorkerAddress(hostname);

    // If we couldn't find a worker, crash.
    if (workerAddress == null) {
      // TODO(calvin): Better exception usage.
      throw new RuntimeException(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage(hostname));
    }
    long clientId = Utils.getRandomNonNegativeLong();
    return new BlockWorkerClient(workerAddress, ClientContext.getExecutorService(),
        ClientContext.getConf(), clientId, false, new ClientMetrics());
  }

  /**
   * Releases the {@link BlockWorkerClient} back to the client pool, or destroys it if it was a
   * remote client.
   *
   * NOTE: the client pool is already thread-safe. Synchronizing on {@link BlockStoreContext} will
   * lead to deadlock: thread A acquired a client and awaits for {@link BlockStoreContext} to
   * release the client, while thread B holds the lock of {@link BlockStoreContext} but waits for
   * available clients.
   *
   * @param blockWorkerClient the worker client to release, the client should not be accessed after
   *        this method is called
   */
  public void releaseWorkerClient(BlockWorkerClient blockWorkerClient) {
    // If the client is local and the pool exists, release the client to the pool, otherwise just
    // close the client.
    if (blockWorkerClient.isLocal()) {
      // Return local worker client to its resource pool.
      Preconditions.checkState(mLocalBlockWorkerClientPool != null);
      mLocalBlockWorkerClientPool.release(blockWorkerClient);
    } else {
      // Destroy remote worker client.
      blockWorkerClient.close();
    }
  }

  /**
   * Determines if a local worker was available during the initialization of the client.
   *
   * @return true if there was a local worker, false otherwise
   */
  // TODO(calvin): Handle the case when the local worker starts up after the client or shuts down
  // before the client does. Also, when this is fixed, fix the TODO(cc) in
  // TachyonBlockStore#getInStream too.
  public synchronized boolean hasLocalWorker() {
    if (!mLocalBlockWorkerClientPoolInitialized) {
      initializeLocalBlockWorkerClientPool();
    }
    return mLocalBlockWorkerClientPool != null;
  }

  /**
   * Re-initializes the {@link BlockStoreContext}. This method should only be used in
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
