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
import alluxio.client.metrics.ClientMasterSync;
import alluxio.client.metrics.MetricsMasterClient;
import alluxio.conf.InstancedConfiguration;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.UnavailableException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.MasterClientConfig;
import alluxio.master.MasterInquireClient;
import alluxio.metrics.MetricsSystem;
import alluxio.network.netty.NettyChannelPool;
import alluxio.network.netty.NettyClient;
import alluxio.resource.CloseableResource;
import alluxio.security.authentication.TransportProviderUtils;
import alluxio.util.CommonUtils;
import alluxio.util.IdUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.ThreadUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.codahale.metrics.Gauge;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

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
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemContext.class);

  @GuardedBy("CONTEXT_CACHE_LOCK")
  private static final Map<Subject, FileSystemContext> CONTEXT_CACHE = new HashMap<>();
  private static final Object CONTEXT_CACHE_LOCK = new Object();

  static {
    MetricsSystem.startSinks();
    Metrics.initializeGauges();
  }

  // Master client pools.
  private volatile FileSystemMasterClientPool mFileSystemMasterClientPool;
  private volatile BlockMasterClientPool mBlockMasterClientPool;

  // Closed flag for debugging information.
  private final AtomicBoolean mClosed;

  private ExecutorService mExecutorService;
  private MetricsMasterClient mMetricsMasterClient;
  private ClientMasterSync mClientMasterSync;

  private final String mAppId;
  @GuardedBy("CONTEXT_CACHE_LOCK")
  private int mRefCount;

  // The netty data server channel pools.
  private final ConcurrentHashMap<ChannelPoolKey, NettyChannelPool>
      mNettyChannelPools = new ConcurrentHashMap<>();

  /** The shared master inquire client associated with the {@link FileSystemContext}. */
  @GuardedBy("this")
  private MasterInquireClient mMasterInquireClient;

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
   * @return the instance of file system context with no subject associated
   */
  public static FileSystemContext get() {
    return get(null);
  }

  /**
   * @param subject the subject associated with this context
   * @return the instance of the file system context, possibly a shared context
   */
  public static FileSystemContext get(Subject subject) {
    synchronized (CONTEXT_CACHE_LOCK) {
      FileSystemContext ctx = CONTEXT_CACHE.computeIfAbsent(subject, FileSystemContext::create);
      ctx.mRefCount++;
      return ctx;
    }
  }

  /**
   * @param subject the parent subject, set to null if not present
   * @return the context
   */
  private static FileSystemContext create(Subject subject) {
    FileSystemContext context = new FileSystemContext(subject);
    context.init(MasterInquireClient.Factory.create(), Configuration.global());
    return context;
  }

  /**
   * This method is provided for testing, use the {@link FileSystemContext#get} methods. The
   * returned context object will not be cached automatically.
   *
   * @param subject the parent subject, set to null if not present
   * @param masterInquireClient the client to use for determining the master; note that if the
   *        context is reset, this client will be replaced with a new masterInquireClient based on
   *        global configuration
   * @return the context
   */
  @VisibleForTesting
  public static FileSystemContext create(Subject subject, MasterInquireClient masterInquireClient) {
    FileSystemContext context = new FileSystemContext(subject);
    context.init(masterInquireClient, Configuration.global());
    synchronized (CONTEXT_CACHE_LOCK) { // Not necessary, for code consistency
      context.mRefCount++;
    }
    return context;
  }

  /**
   * Clears the context cache. This method should only be called in test code.
   */
  @VisibleForTesting
  public static void clearCache() {
    synchronized (CONTEXT_CACHE_LOCK) {
      CONTEXT_CACHE.clear();
    }
  }

  /**
   * Creates a file system context with a subject.
   *
   * @param subject the parent subject, set to null if not present
   */
  private FileSystemContext(Subject subject) {
    mParentSubject = subject;
    mExecutorService = Executors.newFixedThreadPool(1,
        ThreadFactoryUtils.build("metrics-master-heartbeat-%d", true));
    mAppId = Configuration.containsKey(PropertyKey.USER_APP_ID)
        ? Configuration.get(PropertyKey.USER_APP_ID) : IdUtils.createFileSystemContextId();
    LOG.info("Created filesystem context with id {}. This ID will be used for identifying info "
        + "from the client, such as metrics. It can be set manually through the {} property",
        mAppId, PropertyKey.Name.USER_APP_ID);
    mClosed = new AtomicBoolean(false);
    mRefCount = 0;
  }

  /**
   * Initializes the context. Only called in the factory methods and reset.
   *
   * @param masterInquireClient the client to use for determining the master
   * @param configuration the instance configuration
   */
  private synchronized void init(MasterInquireClient masterInquireClient,
      InstancedConfiguration configuration) {
    mMasterInquireClient = masterInquireClient;
    mFileSystemMasterClientPool =
        new FileSystemMasterClientPool(mParentSubject, mMasterInquireClient);
    mBlockMasterClientPool = new BlockMasterClientPool(mParentSubject, mMasterInquireClient);
    mClosed.set(false);

    if (configuration.getBoolean(PropertyKey.USER_METRICS_COLLECTION_ENABLED)) {
      // setup metrics master client sync
      mMetricsMasterClient = new MetricsMasterClient(MasterClientConfig.defaults()
          .withSubject(mParentSubject).withMasterInquireClient(mMasterInquireClient));
      mClientMasterSync = new ClientMasterSync(mMetricsMasterClient, this);
      mExecutorService = Executors.newFixedThreadPool(1,
          ThreadFactoryUtils.build("metrics-master-heartbeat-%d", true));
      mExecutorService
          .submit(new HeartbeatThread(HeartbeatContext.MASTER_METRICS_SYNC, mClientMasterSync,
              (int) configuration.getMs(PropertyKey.USER_METRICS_HEARTBEAT_INTERVAL_MS)));
      // register the shutdown hook
      try {
        Runtime.getRuntime().addShutdownHook(new MetricsMasterSyncShutDownHook());
      } catch (IllegalStateException e) {
        // this exception is thrown when the system is already in the process of shutting down. In
        // such a situation, we are about to shutdown anyway and there is no need to register this
        // shutdown hook
      } catch (SecurityException e) {
        LOG.info("Not registering metrics shutdown hook due to security exception. Regular "
            + "heartbeats will still be performed to collect metrics data, but no final heartbeat "
            + "will be performed on JVM exit. Security exception: {}", e.toString());
      }
    }
  }

  /**
   * Closes all the resources associated with the context. Make sure all the resources are released
   * back to this context before calling this close. After closing the context, all the resources
   * that acquired from this context might fail. Only call this when you are done with using
   * the {@link FileSystem} associated with this {@link FileSystemContext}.
   */
  @Override
  public void close() throws IOException {
    synchronized (CONTEXT_CACHE_LOCK) {
      if (mRefCount == 0) {
        LOG.warn("Attempted to close FileSystem Context that is already closed, have you called "
            + "close multiple times?");
        return;
      }
      if (--mRefCount != 0) {
        return;
      } else {
        CONTEXT_CACHE.remove(mParentSubject);
      }
    }
    closeInternal();
  }

  private void closeInternal() throws IOException {
    mFileSystemMasterClientPool.close();
    mFileSystemMasterClientPool = null;
    mBlockMasterClientPool.close();
    mBlockMasterClientPool = null;
    mMasterInquireClient = null;

    for (NettyChannelPool pool : mNettyChannelPools.values()) {
      pool.close();
    }
    mNettyChannelPools.clear();

    synchronized (this) {
      if (mMetricsMasterClient != null) {
        ThreadUtils.shutdownAndAwaitTermination(mExecutorService);
        mMetricsMasterClient.close();
        mMetricsMasterClient = null;
        mClientMasterSync = null;
      }
      mLocalWorkerInitialized = false;
      mLocalWorker = null;
      mClosed.set(true);
    }
  }

  /**
   * Resets the context. It is only used in {@link alluxio.hadoop.AbstractFileSystem} and tests to
   * reset the default file system context.
   *
   * @param configuration the instance configuration
   *
   */
  public synchronized void reset(InstancedConfiguration configuration) throws IOException {
    closeInternal();
    init(MasterInquireClient.Factory.create(), configuration);
  }

  /**
   * @return the unique id of the context
   */
  public String getId() {
    return mAppId;
  }

  /**
   * @return the parent subject
   */
  public Subject getParentSubject() {
    return mParentSubject;
  }

  /**
   * @return the master address
   * @throws UnavailableException if the master address cannot be determined
   */
  public synchronized InetSocketAddress getMasterAddress() throws UnavailableException {
    return mMasterInquireClient.getPrimaryRpcAddress();
  }

  /**
   * @return the master inquire client
   */
  public synchronized MasterInquireClient getMasterInquireClient() {
    return mMasterInquireClient;
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
   * Acquires a netty channel from the channel pools. If there is no available client instance
   * available in the pool, it tries to create a new one. And an exception is thrown if it fails to
   * create a new one.
   *
   * @param workerNetAddress the network address of the channel
   * @return the acquired netty channel
   */
  public Channel acquireNettyChannel(final WorkerNetAddress workerNetAddress) throws IOException {
    SocketAddress address = NetworkAddressUtils.getDataPortSocketAddress(workerNetAddress);
    ChannelPoolKey key =
        new ChannelPoolKey(address, TransportProviderUtils.getImpersonationUser(mParentSubject));
    if (!mNettyChannelPools.containsKey(key)) {
      Bootstrap bs = NettyClient.createClientBootstrap(mParentSubject, address);
      bs.remoteAddress(address);
      NettyChannelPool pool = new NettyChannelPool(bs,
          Configuration.getInt(PropertyKey.USER_NETWORK_NETTY_CHANNEL_POOL_SIZE_MAX),
          Configuration.getMs(PropertyKey.USER_NETWORK_NETTY_CHANNEL_POOL_GC_THRESHOLD_MS));
      if (mNettyChannelPools.putIfAbsent(key, pool) != null) {
        // This can happen if this function is called concurrently.
        pool.close();
      }
    }
    return mNettyChannelPools.get(key).acquire();
  }

  /**
   * Releases a netty channel to the channel pools.
   *
   * @param workerNetAddress the address of the channel
   * @param channel the channel to release
   */
  public void releaseNettyChannel(WorkerNetAddress workerNetAddress, Channel channel) {
    SocketAddress address = NetworkAddressUtils.getDataPortSocketAddress(workerNetAddress);
    ChannelPoolKey key =
        new ChannelPoolKey(address, TransportProviderUtils.getImpersonationUser(mParentSubject));
    if (mNettyChannelPools.containsKey(key)) {
      mNettyChannelPools.get(key).release(channel);
    } else {
      LOG.warn("No channel pool for key {}, closing channel instead. Context is closed: {}",
          key, mClosed.get());
      CommonUtils.closeChannel(channel);
    }
  }

  /**
   * @return if there is a local worker running the same machine
   */
  public synchronized boolean hasLocalWorker() throws IOException {
    if (!mLocalWorkerInitialized) {
      initializeLocalWorker();
    }
    return mLocalWorker != null;
  }

  /**
   * @return a local worker running the same machine, or null if none is found
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
   */
  private List<WorkerNetAddress> getWorkerAddresses() throws IOException {
    List<WorkerInfo> infos;
    BlockMasterClient blockMasterClient = mBlockMasterClientPool.acquire();
    try {
      infos = blockMasterClient.getWorkerInfoList();
    } finally {
      mBlockMasterClientPool.release(blockMasterClient);
    }
    if (infos.isEmpty()) {
      throw new UnavailableException(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage());
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
   * Class that heartbeats to the metrics master before exit. The heartbeat is performed in a second
   * thread so that we can exit early if the heartbeat is taking too long.
   */
  private final class MetricsMasterSyncShutDownHook extends Thread {
    private final Thread mLastHeartbeatThread = new Thread() {
      @Override
      public void run() {
        if (mClientMasterSync != null) {
          try {
            mClientMasterSync.heartbeat();
          } catch (InterruptedException e) {
            return;
          }
        }
      }
    };

    @Override
    public void run() {
      mLastHeartbeatThread.start();
      try {
        // Shutdown hooks should run quickly, so we limit the wait time to 500ms. It isn't the end
        // of the world if the final heartbeat fails.
        mLastHeartbeatThread.join(500);
        if (mLastHeartbeatThread.isAlive()) {
          LOG.warn("Failed to heartbeat to the metrics master before exit");
        }
      } catch (InterruptedException e) {
        return;
      }
    }
  }

  /**
   * Class that contains metrics about FileSystemContext.
   */
  @ThreadSafe
  private static final class Metrics {
    private static void initializeGauges() {
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName("NettyConnectionsOpen"),
          new Gauge<Long>() {
            @Override
            public Long getValue() {
              long ret = 0;
              for (NettyChannelPool pool : get().mNettyChannelPools.values()) {
                ret += pool.size();
              }
              return ret;
            }
          });
    }

    private Metrics() {} // prevent instantiation
  }

  /**
   * Key for Netty channel pools. This requires both the worker address and the username, so that
   * netty channels are created for different users.
   */
  private static final class ChannelPoolKey {
    private final SocketAddress mSocketAddress;
    private final String mUsername;

    public ChannelPoolKey(SocketAddress socketAddress, String username) {
      mSocketAddress = socketAddress;
      mUsername = username;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(mSocketAddress, mUsername);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ChannelPoolKey)) {
        return false;
      }
      ChannelPoolKey that = (ChannelPoolKey) o;
      return Objects.equal(mSocketAddress, that.mSocketAddress)
          && Objects.equal(mUsername, that.mUsername);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("socketAddress", mSocketAddress)
          .add("username", mUsername)
          .toString();
    }
  }

}
