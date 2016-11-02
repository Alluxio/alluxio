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

package alluxio.client.block;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.ClientContext;
import alluxio.client.netty.NettyClient;
import alluxio.exception.ExceptionMessage;
import alluxio.metrics.MetricsSystem;
import alluxio.network.connection.NettyChannelPool;
import alluxio.resource.CloseableResource;
import alluxio.thrift.BlockWorkerClientService;
import alluxio.util.IdUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.codahale.metrics.Gauge;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.util.internal.chmv8.ConcurrentHashMapV8;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A shared context for each master for common block master client functionality such as a pool
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
public final class BlockStoreContext {
  private BlockMasterClientPool mBlockMasterClientPool;

  // The following maps never shrink in the current implementation because its
  // size is limited by the number of Alluxio workers in the cluster. This simplifies the
  // concurrent access to them.
  private static final ConcurrentHashMapV8<InetSocketAddress, BlockWorkerThriftClientPool>
      BLOCK_WORKER_THRIFT_CLIENT_POOL = new ConcurrentHashMapV8<>();
  private static final ConcurrentHashMapV8<InetSocketAddress, BlockWorkerThriftClientPool>
      BLOCK_WORKER_THRIFT_CLIENT_HEARTBEAT_POOL = new ConcurrentHashMapV8<>();
  private static final ConcurrentHashMapV8<InetSocketAddress, NettyChannelPool>
      NETTY_CHANNEL_POOL_MAP = new ConcurrentHashMapV8<>();

  /**
   * Only one context will be kept for each master address.
   */
  private static final Map<InetSocketAddress, BlockStoreContext> CACHED_CONTEXTS =
      new ConcurrentHashMap<>();

  /**
   * Indicates whether there is any Alluxio worker running in the local machine. This is initialized
   * lazily.
   */
  @GuardedBy("this")
  private Boolean mHasLocalWorker;

  static {
    Metrics.initializeGauges();
  }

  /**
   * Creates a new block store context.
   */
  private BlockStoreContext(InetSocketAddress masterAddress) {
    mBlockMasterClientPool = new BlockMasterClientPool(masterAddress);
  }

  /**
   * Gets a context with the specified master address from the cache if it's created before.
   * Otherwise creates a new one and puts it in the cache.
   *
   * @param masterAddress the master's address
   * @return the context created or cached before
   */
  public static synchronized BlockStoreContext get(InetSocketAddress masterAddress) {
    BlockStoreContext context = CACHED_CONTEXTS.get(masterAddress);
    if (context == null) {
      context = new BlockStoreContext(masterAddress);
      CACHED_CONTEXTS.put(masterAddress, context);
    }
    return context;
  }

  /**
   * Gets a context using the master address got from config.
   *
   * @return the context created or cached before
   */
  public static synchronized BlockStoreContext get() {
    return get(ClientContext.getMasterAddress());
  }

  /**
   * Gets the worker addresses with the given hostname by querying the master. Returns all the
   * addresses, if the hostname is an empty string.
   *
   * @param hostname hostname of the worker to query, empty string denotes any worker
   * @return a list of {@link WorkerNetAddress} with the given hostname
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
   * Creates a client for a worker with the given address.
   *
   * @param address the address of the worker to get a client to
   * @return a {@link BlockWorkerClient} connected to the worker with the given worker RPC address
   * @throws IOException if it fails to create a client for a given hostname (e.g. no Alluxio
   *         worker is available for the given worker RPC address)
   */
  public BlockWorkerClient createWorkerClient(WorkerNetAddress address) throws IOException {
    Preconditions.checkNotNull(address, ExceptionMessage.NO_WORKER_AVAILABLE.getMessage());
    long clientId = IdUtils.getRandomNonNegativeLong();
    return new RetryHandlingBlockWorkerClient(address, clientId);
  }

  /**
   * Acquires a netty channel from the channel pools. If there is no available client instance
   * available in the pool, it tries to create a new one. And an exception is thrown if it fails to
   * create a new one.
   *
   * @param address the network address of the channel
   * @return the acquired netty channel
   * @throws IOException if it fails to create a new client instance mostly because it fails to
   *         connect to remote worker
   */
  public static Channel acquireNettyChannel(final InetSocketAddress address) throws IOException {
    if (!NETTY_CHANNEL_POOL_MAP.containsKey(address)) {
      Bootstrap bs = NettyClient.createClientBootstrap();
      bs.remoteAddress(address);
      NettyChannelPool pool = new NettyChannelPool(bs,
          Configuration.getInt(PropertyKey.USER_NETWORK_NETTY_CHANNEL_POOL_SIZE_MAX),
          Configuration.getLong(PropertyKey.USER_NETWORK_NETTY_CHANNEL_POOL_GC_THRESHOLD_MS));
      if (NETTY_CHANNEL_POOL_MAP.putIfAbsent(address, pool) != null) {
        // This can happen if this function is called concurrently.
        pool.close();
      }
    }
    try {
      return NETTY_CHANNEL_POOL_MAP.get(address).acquire();
    } catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
  }

 /**
   * Releases a netty channel to the channel pools.
   *
   * @param address the network address of the channel
   * @param channel the channel to release
   */
  public static void releaseNettyChannel(InetSocketAddress address, Channel channel) {
    Preconditions.checkArgument(NETTY_CHANNEL_POOL_MAP.containsKey(address));
    NETTY_CHANNEL_POOL_MAP.get(address).release(channel);
  }

  /**
   * Acquires a block worker thrift client from the block worker thrift client pools. If there is
   * no available client instance available in the pool, it tries to create a new one. And an
   * exception is thrown if it fails to create a new one.
   *
   * @param address the address of the block worker
   * @return the block worker thrift client
   * @throws IOException if it fails to create a new client instance mostly because it fails to
   *         connect to remote worker
   */
  public static BlockWorkerClientService.Client acquireBlockWorkerThriftClient(
      final InetSocketAddress address) throws IOException {
    if (!BLOCK_WORKER_THRIFT_CLIENT_POOL.containsKey(address)) {
      BlockWorkerThriftClientPool pool = new BlockWorkerThriftClientPool(address,
          Configuration.getInt(PropertyKey.USER_BLOCK_WORKER_CLIENT_POOL_SIZE_MAX),
          Configuration.getLong(PropertyKey.USER_BLOCK_WORKER_CLIENT_POOL_GC_THRESHOLD_MS));
      if (BLOCK_WORKER_THRIFT_CLIENT_POOL.putIfAbsent(address, pool) != null) {
        pool.close();
      }
    }
    try {
      return BLOCK_WORKER_THRIFT_CLIENT_POOL.get(address).acquire();
    } catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Releases the block worker thrift client to the pool.
   *
   * @param address the network address of the block worker thrift client
   * @param client the block worker thrift client
   */
  public static void releaseBlockWorkerThriftClient(InetSocketAddress address,
      BlockWorkerClientService.Client client) {
    Preconditions.checkArgument(BLOCK_WORKER_THRIFT_CLIENT_POOL.containsKey(address));
    BLOCK_WORKER_THRIFT_CLIENT_POOL.get(address).release(client);
  }

  /**
   * Acquires a block worker thrift client from the block worker thrift client pools dedicated for
   * heartbeat. If there is no available client instance available in the pool, it tries to create
   * a new one. And an exception is thrown if it fails to create a new one.
   *
   * @param address the address of the block worker
   * @return the block worker thrift client
   * @throws IOException if it fails to create a new client instance mostly because it fails to
   *         connect to remote worker
   * @throws InterruptedException if this thread is interrupted
   */
  public static BlockWorkerClientService.Client acquireBlockWorkerThriftClientHeartbeat(
      final InetSocketAddress address) throws IOException, InterruptedException {
    if (!BLOCK_WORKER_THRIFT_CLIENT_HEARTBEAT_POOL.containsKey(address)) {
      BlockWorkerThriftClientPool pool = new BlockWorkerThriftClientPool(address,
          Configuration.getInt(PropertyKey.USER_BLOCK_WORKER_CLIENT_POOL_SIZE_MAX),
          Configuration.getLong(PropertyKey.USER_BLOCK_WORKER_CLIENT_POOL_GC_THRESHOLD_MS));
      if (BLOCK_WORKER_THRIFT_CLIENT_HEARTBEAT_POOL.putIfAbsent(address, pool) != null) {
        pool.close();
      }
    }
    return BLOCK_WORKER_THRIFT_CLIENT_HEARTBEAT_POOL.get(address).acquire();
  }

  /**
   * Releases the block worker thrift client to the heartbeat pool.
   *
   * @param address the network address of the block worker thrift client
   * @param client the block worker thrift client
   */
  public static void releaseBlockWorkerThriftClientHeartbeat(InetSocketAddress address,
      BlockWorkerClientService.Client client) {
    Preconditions.checkArgument(BLOCK_WORKER_THRIFT_CLIENT_HEARTBEAT_POOL.containsKey(address));
    BLOCK_WORKER_THRIFT_CLIENT_HEARTBEAT_POOL.get(address).release(client);
  }

  /**
   * @return if there is a local worker running the same machine
   */
  public synchronized boolean hasLocalWorker() {
    if (mHasLocalWorker == null) {
      mHasLocalWorker = !getWorkerAddresses(NetworkAddressUtils.getLocalHostName()).isEmpty();
    }
    return mHasLocalWorker;
  }

  /**
   * Class that contains metrics about BlockStoreContext.
   */
  @ThreadSafe
  private static final class Metrics {
    private static void initializeGauges() {
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getClientMetricName("NettyConnectionsOpen"),
          new Gauge<Long>() {
            @Override
            public Long getValue() {
              long ret = 0;
              for (NettyChannelPool pool : NETTY_CHANNEL_POOL_MAP.values()) {
                ret += pool.size();
              }
              return ret;
            }
          });
      MetricsSystem
          .registerGaugeIfAbsent(MetricsSystem.getClientMetricName("BlockWorkerClientsOpen"),
              new Gauge<Long>() {
                @Override
                public Long getValue() {
                  long ret = 0;
                  for (BlockWorkerThriftClientPool pool : BLOCK_WORKER_THRIFT_CLIENT_POOL
                      .values()) {
                    ret += pool.size();
                  }
                  return ret;
                }
              });
      MetricsSystem.registerGaugeIfAbsent(
          MetricsSystem.getClientMetricName("BlockWorkerHeartbeatClientsOpen"), new Gauge<Long>() {
            @Override
            public Long getValue() {
              long ret = 0;
              for (BlockWorkerThriftClientPool pool : BLOCK_WORKER_THRIFT_CLIENT_HEARTBEAT_POOL
                  .values()) {
                ret += pool.size();
              }
              return ret;
            }
          });
    }

    private Metrics() {} // prevent instantiation
  }
}

