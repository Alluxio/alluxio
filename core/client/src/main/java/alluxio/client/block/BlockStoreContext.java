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
import alluxio.exception.ExceptionMessage;
import alluxio.exception.PreconditionMessage;
import alluxio.resource.CloseableResource;
import alluxio.util.IdUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.ClientMetrics;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A shared context in each client JVM for common block master client functionality such as a pool
 * of master clients and a pool of local worker clients. Any remote clients will be created and
 * destroyed on a per use basis.
 * <p/>
 * NOTE: The context maintains a pool of block master clients and a pool of block worker clients
 * that are already thread-safe. Synchronizing {@link BlockStoreContext} methods could lead to
 * deadlock: thread A attempts to acquire a client when there are no clients left in the pool and
 * blocks holding a lock on the {@link BlockStoreContext}, when thread B attempts to release a
 * client it owns, it is unable to do so, because thread A holds the lock on
 * {@link BlockStoreContext}.
 */
@ThreadSafe
public enum BlockStoreContext {
  INSTANCE;

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private BlockMasterClientPool mBlockMasterClientPool;

  /**
   * A map from the worker's address to its client pool. Guarded by
   * {@link #initializeLocalBlockWorkerClientPool()} for client acquisition. There is no guard for
   * releasing client, and client can be released anytime.
   */
  private final Map<WorkerNetAddress, BlockWorkerClientPool> mLocalBlockWorkerClientPoolMap =
      new ConcurrentHashMap<>();

  private boolean mLocalBlockWorkerClientPoolInitialized = false;

  /**
   * Creates a new block store context.
   */
  BlockStoreContext() {
    reset();
  }

  /**
   * Initializes {@link #mLocalBlockWorkerClientPoolMap}. This method is supposed be called in a
   * lazy manner.
   */
  private synchronized void initializeLocalBlockWorkerClientPool() {
    if (!mLocalBlockWorkerClientPoolInitialized) {
      for (WorkerNetAddress localWorkerAddress : getWorkerAddresses(
          NetworkAddressUtils.getLocalHostName(ClientContext.getConf()))) {
        mLocalBlockWorkerClientPoolMap.put(localWorkerAddress,
            new BlockWorkerClientPool(localWorkerAddress));
      }
      mLocalBlockWorkerClientPoolInitialized = true;
    }
  }

  /**
   * Gets the worker addresses with the given hostname by querying the master. Returns all the
   * addresses, if the hostname is an empty string.
   *
   * @param hostname hostname of the worker to query, empty string denotes any worker
   * @return List of {@link WorkerNetAddress} of hostname
   */
  private List<WorkerNetAddress> getWorkerAddresses(String hostname) {
    List<WorkerNetAddress> addresses = new ArrayList<>();
    try (CloseableResource<BlockMasterClient> masterClient = acquireMasterClientResource()) {
      List<WorkerInfo> workers = masterClient.get().getWorkerInfoList();
      for (WorkerInfo worker : workers) {
        if (hostname.isEmpty() || worker.getAddress().getHost().equals(hostname)) {
          addresses.add(worker.getAddress());
        }
      }
    } catch (Exception e) {
      Throwables.propagate(e);
    }
    return addresses;
  }

  /**
   * Acquires a block master client resource from the block master client pool. The resource is
   * {@code Closeable}.
   *
   * @return the acquired block master client resource
   */
  public CloseableResource<BlockMasterClient> acquireMasterClientResource() {
    return new CloseableResource<BlockMasterClient>(mBlockMasterClientPool.acquire()) {
      @Override
      public void close() {
        mBlockMasterClientPool.release(get());
      }
    };
  }

  /**
   * Obtains a client for a worker with the given address.
   *
   * @param address the address of the worker to get a client to
   * @return a {@link BlockWorkerClient} connected to the worker with the given hostname
   * @throws IOException if no Alluxio worker is available for the given hostname
   */
  public BlockWorkerClient acquireWorkerClient(WorkerNetAddress address) throws IOException {
    BlockWorkerClient client;
    if (address == null) {
      throw new RuntimeException(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage());
    }
    if (address.getHost().equals(NetworkAddressUtils.getLocalHostName(ClientContext.getConf()))) {
      client = acquireLocalWorkerClient(address);
      if (client == null) {
        throw new IOException(
            ExceptionMessage.NO_WORKER_AVAILABLE_ON_ADDRESS.getMessage(address));
      }
    } else {
      client = acquireRemoteWorkerClient(address);
    }
    return client;
  }

  /**
   * Obtains a worker client on the local worker in the system. For testing only.
   *
   * @return a {@link BlockWorkerClient} to a worker in the Alluxio system or null if failed
   */
  public BlockWorkerClient acquireLocalWorkerClient() {
    initializeLocalBlockWorkerClientPool();
    if (mLocalBlockWorkerClientPoolMap.isEmpty()) {
      return null;
    }
    // return any local worker
    return mLocalBlockWorkerClientPoolMap.values().iterator().next().acquire();
  }

  /**
   * Obtains a worker client for the given local worker address.
   *
   * @param address worker address
   *
   * @return a {@link BlockWorkerClient} to the given worker address or null if no such worker can
   *         be found
   */
  public BlockWorkerClient acquireLocalWorkerClient(WorkerNetAddress address) {
    initializeLocalBlockWorkerClientPool();
    if (!mLocalBlockWorkerClientPoolMap.containsKey(address)) {
      return null;
    }
    return mLocalBlockWorkerClientPoolMap.get(address).acquire();
  }

  /**
   * Obtains a client for a remote based on the given network address. Illegal argument exception is
   * thrown if the hostname is the local hostname. Runtime exception is thrown if the client cannot
   * be created with a connection to the hostname.
   *
   * @param address the address of the worker
   * @return a worker client with a connection to the specified hostname
   */
  private BlockWorkerClient acquireRemoteWorkerClient(WorkerNetAddress address) {
    // If we couldn't find a worker, crash.
    if (address == null) {
      // TODO(calvin): Better exception usage.
      throw new RuntimeException(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage());
    }
    Preconditions.checkArgument(
        !address.getHost().equals(NetworkAddressUtils.getLocalHostName(ClientContext.getConf())),
        PreconditionMessage.REMOTE_CLIENT_BUT_LOCAL_HOSTNAME);
    long clientId = IdUtils.getRandomNonNegativeLong();
    return new BlockWorkerClient(address, ClientContext.getExecutorService(),
        ClientContext.getConf(), clientId, false, new ClientMetrics());
  }

  /**
   * Releases the {@link BlockWorkerClient} back to the client pool, or destroys it if it was a
   * remote client.
   *
   * @param blockWorkerClient the worker client to release, the client should not be accessed after
   *        this method is called
   */
  public void releaseWorkerClient(BlockWorkerClient blockWorkerClient) {
    // If the client is local and the pool exists, release the client to the pool, otherwise just
    // close the client.
    if (blockWorkerClient.isLocal()) {
      // Return local worker client to its resource pool.
      WorkerNetAddress address = blockWorkerClient.getWorkerNetAddress();
      if (!mLocalBlockWorkerClientPoolMap.containsKey(address)) {
        LOG.error("The client to worker at {} to release is no longer registered in the context.",
            address);
        blockWorkerClient.close();
      } else {
        mLocalBlockWorkerClientPoolMap.get(address).release(blockWorkerClient);
      }
    } else {
      // Destroy remote worker client.
      blockWorkerClient.close();
    }
  }

  /**
   * Re-initializes the {@link BlockStoreContext}. This method should only be used in
   * {@link ClientContext}.
   */
  @SuppressFBWarnings
  public void reset() {
    if (mBlockMasterClientPool != null) {
      mBlockMasterClientPool.close();
    }
    for (BlockWorkerClientPool pool : mLocalBlockWorkerClientPoolMap.values()) {
      pool.close();
    }
    mLocalBlockWorkerClientPoolMap.clear();
    mBlockMasterClientPool = new BlockMasterClientPool(ClientContext.getMasterAddress());
    mLocalBlockWorkerClientPoolInitialized = false;
  }
}
