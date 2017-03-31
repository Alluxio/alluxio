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

package alluxio.client.file;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.block.BlockMasterClientPool;
import alluxio.client.block.BlockWorkerClient;
import alluxio.client.block.BlockWorkerThriftClientPool;
import alluxio.client.netty.NettyClient;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.metrics.MetricsSystem;
import alluxio.network.connection.NettyChannelPool;
import alluxio.resource.CloseableResource;
import alluxio.util.IdUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.codahale.metrics.Gauge;
import com.google.common.base.Preconditions;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.util.internal.chmv8.ConcurrentHashMapV8;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;

/**
 * A shared context that isolates all operations within a {@link FileSystem}. Usually, one user
 * only needs one instance of {@link FileSystemContext}.
 *
 * <p>
 * NOTE: The context maintains a pool of file system master clients that is already thread-safe.
 * Synchronizing {@link FileSystemContext} methods could lead to deadlock: thread A attempts to
 * acquire a client when there are no clients left in the pool and blocks holding a lock on the
 * {@link FileSystemContext}, when thread B attempts to release a client it owns it is unable to do
 * so, because thread A holds the lock on {@link FileSystemContext}.
 */
@ThreadSafe
public final class FileSystemContext implements Closeable {
  public static final FileSystemContext INSTANCE = create(null);

  static {
    MetricsSystem.startSinks();
    Metrics.initializeGauges();
  }

  // Master client pools.
  private volatile FileSystemMasterClientPool mFileSystemMasterClientPool;
  private volatile BlockMasterClientPool mBlockMasterClientPool;

  // Block worker client pools.
  private final ConcurrentHashMapV8<InetSocketAddress, BlockWorkerThriftClientPool>
      mBlockWorkerClientPools = new ConcurrentHashMapV8<>();
  private final ConcurrentHashMapV8<InetSocketAddress, BlockWorkerThriftClientPool>
      mBlockWorkerClientHeartbeatPools = new ConcurrentHashMapV8<>();

  // The file system worker client pools.
  private final ConcurrentHashMapV8<InetSocketAddress, FileSystemWorkerThriftClientPool>
      mFileSystemWorkerClientPools = new ConcurrentHashMapV8<>();
  private final ConcurrentHashMapV8<InetSocketAddress, FileSystemWorkerThriftClientPool>
      mFileSystemWorkerClientHeartbeatPools = new ConcurrentHashMapV8<>();

  // The netty data server channel pools.
  private final ConcurrentHashMapV8<InetSocketAddress, NettyChannelPool>
      mNettyChannelPools = new ConcurrentHashMapV8<>();

  /** The shared master address associated with the {@link FileSystemContext}. */
  @GuardedBy("this")
  private InetSocketAddress mMasterAddress;

  /** A list of valid workers, if there is a local worker, only the local worker addresses. */
  @GuardedBy("this")
  private List<WorkerNetAddress> mWorkerAddresses;

  /**
   * Indicates whether the {@link #mLocalWorker} field has been lazily initialized yet.
   */
  @GuardedBy("this")
  private boolean mLocalWorkerInitialized;

  /**
   * The address of any Alluxio worker running on the local machine. This is initialized lazily.
   */
  @GuardedBy("this")
  private WorkerNetAddress mLocalWorker;

  /** The parent user associated with the {@link FileSystemContext}. */
  private final Subject mParentSubject;

  /**
   * Creates a new file system context.
   *
   * @return the context
   */
  public static FileSystemContext create() {
    return create(null);
  }

  /**
   * Creates a file system context with a subject.
   *
   * @param subject the parent subject, set to null if not present
   * @return the context
   */
  public static FileSystemContext create(Subject subject) {
    FileSystemContext context = new FileSystemContext(subject);
    context.init();
    return context;
  }

  /**
   * Creates a file system context with a subject.
   *
   * @param subject the parent subject, set to null if not present
   */
  private FileSystemContext(Subject subject) {
    mParentSubject = subject;
  }

  /**
   * Initializes the context. Only called in the factory methods and reset.
   */
  private void init() {
    mMasterAddress = NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC);
    mFileSystemMasterClientPool = new FileSystemMasterClientPool(mParentSubject, mMasterAddress);
    mBlockMasterClientPool = new BlockMasterClientPool(mParentSubject, mMasterAddress);
  }

  /**
   * Closes all the resources associated with the context. Make sure all the resources are released
   * back to this context before calling this close. After closing the context, all the resources
   * that acquired from this context might fail. Only call this when you are done with using
   * the {@link FileSystem} associated with this {@link FileSystemContext}.
   */
  public void close() {
    mFileSystemMasterClientPool.close();
    mFileSystemMasterClientPool = null;
    mBlockMasterClientPool.close();
    mBlockMasterClientPool = null;

    for (BlockWorkerThriftClientPool pool : mBlockWorkerClientPools.values()) {
      pool.close();
    }
    mBlockWorkerClientPools.clear();

    for (BlockWorkerThriftClientPool pool : mBlockWorkerClientHeartbeatPools.values()) {
      pool.close();
    }
    mBlockWorkerClientHeartbeatPools.clear();

    for (FileSystemWorkerThriftClientPool pool : mFileSystemWorkerClientPools.values()) {
      pool.close();
    }
    mFileSystemWorkerClientPools.clear();

    for (FileSystemWorkerThriftClientPool pool : mFileSystemWorkerClientHeartbeatPools.values()) {
      pool.close();
    }
    mFileSystemWorkerClientHeartbeatPools.clear();

    for (NettyChannelPool pool : mNettyChannelPools.values()) {
      pool.close();
    }
    mNettyChannelPools.clear();

    synchronized (this) {
      mMasterAddress = null;
      mWorkerAddresses = null;
      mLocalWorkerInitialized = false;
      mLocalWorker = null;
    }
  }

  /**
   * Resets the context. It is only used in {@link alluxio.hadoop.AbstractFileSystem} and
   * tests to reset the default file system context.
   */
  public synchronized void reset() {
    close();
    init();
  }

  /**
   * @return the parent subject
   */
  public Subject getParentSubject() {
    return mParentSubject;
  }

  /**
   * @return the master address
   */
  public synchronized InetSocketAddress getMasterAddress() {
    return mMasterAddress;
  }

  /**
   * Acquires a file system master client from the file system master client pool.
   *
   * @return the acquired file system master client
   */
  public FileSystemMasterClient acquireMasterClient() {
    return mFileSystemMasterClientPool.acquire();
  }

  /**
   * Releases a file system master client into the file system master client pool.
   *
   * @param masterClient a file system master client to release
   */
  public void releaseMasterClient(FileSystemMasterClient masterClient) {
    mFileSystemMasterClientPool.release(masterClient);
  }

  /**
   * Acquires a file system master client from the file system master client pool. The resource is
   * {@code Closeable}.
   *
   * @return the acquired file system master client resource
   */
  public CloseableResource<FileSystemMasterClient> acquireMasterClientResource() {
    return new CloseableResource<FileSystemMasterClient>(mFileSystemMasterClientPool.acquire()) {
      @Override
      public void close() {
        mFileSystemMasterClientPool.release(get());
      }
    };
  }

  /**
   * Acquires a block master client resource from the block master client pool. The resource is
   * {@code Closeable}.
   *
   * @return the acquired block master client resource
   */
  public CloseableResource<BlockMasterClient> acquireBlockMasterClientResource() {
    return new CloseableResource<BlockMasterClient>(mBlockMasterClientPool.acquire()) {
      @Override
      public void close() {
        mBlockMasterClientPool.release(get());
      }
    };
  }

  /**
   * Creates a client for a block worker with the given address.
   *
   * @param address the address of the worker to get a client to
   * @return a {@link BlockWorkerClient} connected to the worker with the given worker RPC address
   * @throws IOException if it fails to create a worker client for a given hostname (e.g. no block
   *         worker is available for the given worker RPC address)
   */
  public BlockWorkerClient createBlockWorkerClient(WorkerNetAddress address) throws IOException {
    return createBlockWorkerClient(address, IdUtils.getRandomNonNegativeLong());
  }

  /**
   * Creates a client for a block worker with the given address.
   *
   * @param address the address of the worker to get a client to
   * @param sessionId the session ID
   * @return a {@link BlockWorkerClient} connected to the worker with the given worker RPC address
   * @throws IOException if it fails to create a client for a given hostname (e.g. no Alluxio
   *         worker is available for the given worker RPC address)
   */
  // TODO(peis): Abstract the logic to operate on the pools.
  public BlockWorkerClient createBlockWorkerClient(WorkerNetAddress address,
      Long sessionId) throws IOException {
    Preconditions.checkNotNull(address, ExceptionMessage.NO_WORKER_AVAILABLE.getMessage());
    InetSocketAddress rpcAddress = NetworkAddressUtils.getRpcPortSocketAddress(address);

    if (!mBlockWorkerClientPools.containsKey(rpcAddress)) {
      BlockWorkerThriftClientPool pool = new BlockWorkerThriftClientPool(mParentSubject, rpcAddress,
          Configuration.getInt(PropertyKey.USER_BLOCK_WORKER_CLIENT_POOL_SIZE_MAX),
          Configuration.getLong(PropertyKey.USER_BLOCK_WORKER_CLIENT_POOL_GC_THRESHOLD_MS));
      if (mBlockWorkerClientPools.putIfAbsent(rpcAddress, pool) != null) {
        pool.close();
      }
    }

    if (!mBlockWorkerClientHeartbeatPools.containsKey(rpcAddress)) {
      BlockWorkerThriftClientPool pool = new BlockWorkerThriftClientPool(mParentSubject, rpcAddress,
          Configuration.getInt(PropertyKey.USER_BLOCK_WORKER_CLIENT_POOL_SIZE_MAX),
          Configuration.getLong(PropertyKey.USER_BLOCK_WORKER_CLIENT_POOL_GC_THRESHOLD_MS));
      if (mBlockWorkerClientHeartbeatPools.putIfAbsent(rpcAddress, pool) != null) {
        pool.close();
      }
    }

    return BlockWorkerClient.Factory.create(mBlockWorkerClientPools.get(rpcAddress),
        mBlockWorkerClientHeartbeatPools.get(rpcAddress), address, sessionId);
  }

  /**
   * Creates a new file system worker client, prioritizing local workers if available. This method
   * initializes the list of worker addresses if it has not been initialized.
   *
   * @return a file system worker client to a worker in the system
   * @throws IOException if an error occurs getting the list of workers in the system
   */
  public FileSystemWorkerClient createFileSystemWorkerClient() throws IOException {
    WorkerNetAddress address;
    synchronized (this) {
      if (mWorkerAddresses == null) {
        mWorkerAddresses = getWorkerAddresses();
      }
      address = mWorkerAddresses.get(ThreadLocalRandom.current().nextInt(mWorkerAddresses.size()));
    }

    InetSocketAddress rpcAddress = NetworkAddressUtils.getRpcPortSocketAddress(address);
    if (!mFileSystemWorkerClientPools.containsKey(rpcAddress)) {
      FileSystemWorkerThriftClientPool pool =
          new FileSystemWorkerThriftClientPool(mParentSubject, rpcAddress,
              Configuration.getInt(PropertyKey.USER_FILE_WORKER_CLIENT_POOL_SIZE_MAX),
              Configuration.getLong(PropertyKey.USER_FILE_WORKER_CLIENT_POOL_GC_THRESHOLD_MS));
      if (mFileSystemWorkerClientPools.putIfAbsent(rpcAddress, pool) != null) {
        pool.close();
      }
    }

    if (!mFileSystemWorkerClientHeartbeatPools.containsKey(rpcAddress)) {
      FileSystemWorkerThriftClientPool pool =
          new FileSystemWorkerThriftClientPool(mParentSubject, rpcAddress,
              Configuration.getInt(PropertyKey.USER_FILE_WORKER_CLIENT_POOL_SIZE_MAX),
              Configuration.getLong(PropertyKey.USER_FILE_WORKER_CLIENT_POOL_GC_THRESHOLD_MS));
      if (mFileSystemWorkerClientHeartbeatPools.putIfAbsent(rpcAddress, pool) != null) {
        pool.close();
      }
    }

    long sessionId = IdUtils.getRandomNonNegativeLong();
    return FileSystemWorkerClient.Factory.create(mFileSystemWorkerClientPools.get(rpcAddress),
        mFileSystemWorkerClientHeartbeatPools.get(rpcAddress), address, sessionId);
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
  public Channel acquireNettyChannel(final InetSocketAddress address) throws IOException {
    if (!mNettyChannelPools.containsKey(address)) {
      Bootstrap bs = NettyClient.createClientBootstrap();
      bs.remoteAddress(address);
      NettyChannelPool pool = new NettyChannelPool(bs,
          Configuration.getInt(PropertyKey.USER_NETWORK_NETTY_CHANNEL_POOL_SIZE_MAX),
          Configuration.getLong(PropertyKey.USER_NETWORK_NETTY_CHANNEL_POOL_GC_THRESHOLD_MS));
      if (mNettyChannelPools.putIfAbsent(address, pool) != null) {
        // This can happen if this function is called concurrently.
        pool.close();
      }
    }
    try {
      return mNettyChannelPools.get(address).acquire();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Releases a netty channel to the channel pools.
   *
   * @param address the network address of the channel
   * @param channel the channel to release
   */
  public void releaseNettyChannel(InetSocketAddress address, Channel channel) {
    Preconditions.checkArgument(mNettyChannelPools.containsKey(address));
    mNettyChannelPools.get(address).release(channel);
  }

  /**
   * @return if there is a local worker running the same machine
   * @throws IOException if it fails to get the workers
   */
  public synchronized boolean hasLocalWorker() throws IOException {
    if (!mLocalWorkerInitialized) {
      initializeLocalWorker();
    }
    return mLocalWorker != null;
  }

  /**
   * @return a local worker running the same machine, or null if none is found
   * @throws IOException if it fails to get the workers
   */
  public synchronized WorkerNetAddress getLocalWorker() throws IOException {
    if (!mLocalWorkerInitialized) {
      initializeLocalWorker();
    }
    return mLocalWorker;
  }

  private void initializeLocalWorker() throws IOException {
    List<WorkerNetAddress> addresses = getWorkerAddresses();
    if (!addresses.isEmpty()) {
      if (addresses.get(0).getHost().equals(NetworkAddressUtils.getClientHostName())) {
        mLocalWorker = addresses.get(0);
      }
    }
    mLocalWorkerInitialized = true;
  }

  /**
   * @return if there are any local workers, the returned list will ONLY contain the local workers,
   *         otherwise a list of all remote workers will be returned
   * @throws IOException if an error occurs communicating with the master
   */
  private List<WorkerNetAddress> getWorkerAddresses() throws IOException {
    List<WorkerInfo> infos;
    BlockMasterClient blockMasterClient = mBlockMasterClientPool.acquire();
    try {
      infos = blockMasterClient.getWorkerInfoList();
    } catch (AlluxioException e) {
      throw new IOException(e);
    } finally {
      mBlockMasterClientPool.release(blockMasterClient);
    }
    if (infos.isEmpty()) {
      throw new IOException(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage());
    }

    // Convert the worker infos into net addresses, if there are local addresses, only keep those
    List<WorkerNetAddress> workerNetAddresses = new ArrayList<>();
    List<WorkerNetAddress> localWorkerNetAddresses = new ArrayList<>();
    String localHostname = NetworkAddressUtils.getClientHostName();
    for (WorkerInfo info : infos) {
      WorkerNetAddress netAddress = info.getAddress();
      if (netAddress.getHost().equals(localHostname)) {
        localWorkerNetAddresses.add(netAddress);
      }
      workerNetAddresses.add(netAddress);
    }

    return localWorkerNetAddresses.isEmpty() ? workerNetAddresses : localWorkerNetAddresses;
  }

  /**
   * Class that contains metrics about FileSystemContext.
   */
  @ThreadSafe
  private static final class Metrics {
    private static void initializeGauges() {
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getClientMetricName("NettyConnectionsOpen"),
          new Gauge<Long>() {
            @Override
            public Long getValue() {
              long ret = 0;
              for (NettyChannelPool pool : INSTANCE.mNettyChannelPools.values()) {
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
                  for (BlockWorkerThriftClientPool pool : INSTANCE.mBlockWorkerClientPools
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
              for (BlockWorkerThriftClientPool pool : INSTANCE.mBlockWorkerClientHeartbeatPools
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
