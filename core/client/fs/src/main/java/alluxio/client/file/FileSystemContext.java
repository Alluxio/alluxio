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

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.block.BlockMasterClientPool;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.block.stream.BlockWorkerClientPool;
import alluxio.client.metrics.ClientMasterSync;
import alluxio.client.metrics.MetricsMasterClient;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.path.SpecificPathConfiguration;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.GrpcServerAddress;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.MasterClientContext;
import alluxio.master.MasterInquireClient;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.CloseableResource;
import alluxio.security.authentication.AuthenticationUserUtils;
import alluxio.util.IdUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.ThreadUtils;
import alluxio.util.network.NettyUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.codahale.metrics.Gauge;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;

/**
 * An object which houses resources and information for performing {@link FileSystem} operations.
 * Typically, a single JVM should only need one instance of a {@link FileSystem} to connect to
 * Alluxio. The reference to that client object should be shared among threads.
 *
 * A second {@link FileSystemContext} object should only be created when a user needs to connect to
 * Alluxio with a different {@link Subject} and/or {@link AlluxioConfiguration}.
 * {@link FileSystemContext} instances should be created sparingly because each instance creates
 * its own thread pools of {@link FileSystemMasterClient} and {@link BlockMasterClient} which can
 * lead to inefficient use of client machine resources.
 *
 * A {@link FileSystemContext} should be closed once the user is done performing operations with
 * Alluxio and no more connections need to be made. Once a {@link FileSystemContext} is closed it
 * is preferred that the user of the class create a new instance with
 * {@link FileSystemContext#create} to create a new context, rather than reinitializing using the
 * {@link FileSystemContext#init} method.
 *
 * NOTE: Each context maintains a pool of file system master clients that is already thread-safe.
 * Synchronizing {@link FileSystemContext} methods could lead to deadlock: thread A attempts to
 * acquire a client when there are no clients left in the pool and blocks holding a lock on the
 * {@link FileSystemContext}, when thread B attempts to release a client it owns it is unable to do
 * so, because thread A holds the lock on {@link FileSystemContext}.
 */
@ThreadSafe
public final class FileSystemContext implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemContext.class);

  // Master client pools.
  private volatile FileSystemMasterClientPool mFileSystemMasterClientPool;
  private volatile BlockMasterClientPool mBlockMasterClientPool;

  // Marks whether the context has been closed, closing the context means releasing all resources
  // in the context like clients and thread pools.
  private final AtomicBoolean mClosed = new AtomicBoolean(true);

  @GuardedBy("this")
  private ExecutorService mMetricsExecutorService;
  @GuardedBy("this")
  private MetricsMasterClient mMetricsMasterClient;
  // volatile because it's used in MetricsMasterSyncShutDownHook.
  private volatile ClientMasterSync mClientMasterSync;

  // The data server channel pools. This pool will only grow and keys are not removed.
  private final ConcurrentHashMap<ClientPoolKey, BlockWorkerClientPool>
      mBlockWorkerClientPool = new ConcurrentHashMap<>();
  private volatile EventLoopGroup mWorkerGroup;

  /** The shared master inquire client associated with the {@link FileSystemContext}. */
  @GuardedBy("this")
  private MasterInquireClient mMasterInquireClient;
  /** The master client context holding the inquire client. */
  private volatile MasterClientContext mMasterClientContext;

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

  private final ClientContext mClientContext;
  private final String mAppId;

  /**
   * Reinitializer contains a daemon heartbeat thread to reinitialize this context when
   * configuration hashes change.
   */
  private volatile FileSystemContextReinitializer mReinitializer;

  /**
   * Creates a {@link FileSystemContext} with a null subject.
   *
   * @param alluxioConf Alluxio configuration
   * @return an instance of file system context with no subject associated
   */
  public static FileSystemContext create(AlluxioConfiguration alluxioConf) {
    return create(null, alluxioConf);
  }

  /**
   * Creates a {@link FileSystemContext} with the configuration loaded from site-properties.
   *
   * @param subject The subject to connect to Alluxio with
   * @return an instance of file system context with the given subject and site-properties config
   */
  public static FileSystemContext create(Subject subject) {
    return create(subject, null);
  }

  /**
   * @param subject the parent subject, set to null if not present
   * @param alluxioConf Alluxio configuration
   * @return a context
   */
  public static FileSystemContext create(Subject subject, AlluxioConfiguration alluxioConf) {
    FileSystemContext context = new FileSystemContext(subject, alluxioConf);
    context.init(MasterInquireClient.Factory.create(context.mClientContext.getConf()));
    return context;
  }

  /**
   * @param clientContext the {@link alluxio.ClientContext} containing the subject and configuration
   * @return the {@link alluxio.client.file.FileSystemContext}
   */
  public static FileSystemContext create(ClientContext clientContext) {
    FileSystemContext ctx = new FileSystemContext(clientContext);
    ctx.init(MasterInquireClient.Factory.create(clientContext.getConf()));
    return ctx;
  }

  /**
   * This method is provided for testing, use the {@link FileSystemContext#create} methods. The
   * returned context object will not be cached automatically.
   *
   * @param subject the parent subject, set to null if not present
   * @param masterInquireClient the client to use for determining the master; note that if the
   *        context is reset, this client will be replaced with a new masterInquireClient based on
   *        the original configuration.
   * @param alluxioConf Alluxio configuration
   * @return the context
   */
  @VisibleForTesting
  public static FileSystemContext create(Subject subject, MasterInquireClient masterInquireClient,
      AlluxioConfiguration alluxioConf) {
    FileSystemContext context = new FileSystemContext(subject, alluxioConf);
    context.init(masterInquireClient);
    return context;
  }

  /**
   * Creates a file system context with a subject.
   *
   * @param subject the parent subject, set to null if not present
   */
  private FileSystemContext(Subject subject, AlluxioConfiguration alluxioConf) {
      this(ClientContext.create(subject, alluxioConf));
  }

  /**
   * Creates a file system context with a subject.
   *
   * @param ctx the parent subject, set to null if not present
   */
  private FileSystemContext(ClientContext ctx) {
    Preconditions.checkNotNull(ctx, "ctx");
    mClientContext = ctx;
    mAppId = IdUtils.createOrGetAppIdFromConfig(ctx.getConf());
    LOG.info("Created filesystem context with id {}. This ID will be used for identifying info "
            + "from the client, such as metrics. It can be set manually through the {} property",
        mAppId, PropertyKey.Name.USER_APP_ID);
  }

  /**
   * @return the unique application ID for each context
   */
  public String getAppId() {
    return mAppId;
  }

  /**
   * Initializes the context. Only called in the factory methods.
   *
   * @param masterInquireClient the client to use for determining the master
   */
  private synchronized void init(MasterInquireClient masterInquireClient) {
    initWithoutReinitializer(masterInquireClient);
    mReinitializer = new FileSystemContextReinitializer(this);
  }

  private synchronized void initWithoutReinitializer(MasterInquireClient masterInquireClient) {
    mClosed.set(false);

    mWorkerGroup = NettyUtils.createEventLoop(NettyUtils.getUserChannel(mClientContext.getConf()),
        mClientContext.getConf().getInt(PropertyKey.USER_NETWORK_NETTY_WORKER_THREADS),
        String.format("alluxio-client-nettyPool-%s-%%d", mAppId), true);

    mMasterInquireClient = masterInquireClient;

    mFileSystemMasterClientPool =
        new FileSystemMasterClientPool(mClientContext, mMasterInquireClient);
    mBlockMasterClientPool = new BlockMasterClientPool(mClientContext, mMasterInquireClient);

    mMasterClientContext = MasterClientContext.newBuilder(mClientContext)
        .setMasterInquireClient(mMasterInquireClient).build();

    if (mClientContext.getConf().getBoolean(PropertyKey.USER_METRICS_COLLECTION_ENABLED)) {
      // setup metrics master client sync
      mMetricsMasterClient = new MetricsMasterClient(mMasterClientContext);
      mClientMasterSync = new ClientMasterSync(mMetricsMasterClient, mAppId);
      mMetricsExecutorService = Executors.newFixedThreadPool(1,
          ThreadFactoryUtils.build("metrics-master-heartbeat-%d", true));
      mMetricsExecutorService
          .submit(new HeartbeatThread(HeartbeatContext.MASTER_METRICS_SYNC, mClientMasterSync,
              (int) mClientContext.getConf().getMs(PropertyKey.USER_METRICS_HEARTBEAT_INTERVAL_MS),
              mClientContext.getConf()));
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
  public synchronized void close() throws IOException {
    mReinitializer.close();
    closeWithoutReinitializer();
  }

  private synchronized void closeWithoutReinitializer() throws IOException {
    if (!mClosed.get()) {
      // Setting closed should be the first thing we do because if any of the close operations
      // fail we'll only have a half-closed object and performing any more operations or closing
      // again on a half-closed object can possibly result in more errors (i.e. NPE). Setting
      // closed first is also recommended by the JDK that in implementations of #close() that
      // developers should first mark their resources as closed prior to any exceptions being
      // thrown.
      mClosed.set(true);
      mWorkerGroup.shutdownGracefully(1L, 10L, TimeUnit.SECONDS);
      mFileSystemMasterClientPool.close();
      mFileSystemMasterClientPool = null;
      mBlockMasterClientPool.close();
      mBlockMasterClientPool = null;
      mMasterInquireClient = null;
      for (BlockWorkerClientPool pool : mBlockWorkerClientPool.values()) {
        pool.close();
      }
      mBlockWorkerClientPool.clear();

      if (mMetricsMasterClient != null) {
        ThreadUtils.shutdownAndAwaitTermination(mMetricsExecutorService,
            mClientContext.getConf().getMs(PropertyKey.METRICS_CONTEXT_SHUTDOWN_TIMEOUT));
        mMetricsMasterClient.close();
        mMetricsMasterClient = null;
        mClientMasterSync = null;
      }
      mLocalWorkerInitialized = false;
      mLocalWorker = null;
    } else {
      LOG.warn("Attempted to close FileSystemContext with app ID {} which has already been closed"
          + " or not initialized.", mAppId);
    }
  }

  /**
   * Closes the context, updates configuration from meta master, then re-initializes the context.
   *
   * The reinitializer is not closed, which means the heartbeat thread inside it is not stopped.
   * The reinitializer will be reset with the updated context if reinitialization succeeds,
   * otherwise, the reinitializer is not reset.
   *
   * @param updateClusterConf whether cluster level configuration should be updated
   * @param updatePathConf whether path level configuration should be updated
   * @throws IOException when failed to close the context or update configuration
   */
  public synchronized void reinit(boolean updateClusterConf, boolean updatePathConf)
      throws IOException {
    try {
      mReinitializer.begin();
      InetSocketAddress masterAddr = getMasterAddress();
      closeWithoutReinitializer();
      if (updateClusterConf && updatePathConf) {
        mClientContext.updateClusterAndPathConf(masterAddr);
      } else if (updateClusterConf) {
        mClientContext.updateClusterConf(masterAddr);
      } else {
        mClientContext.updatePathConf(masterAddr);
      }
      initWithoutReinitializer(MasterInquireClient.Factory.create(mClientContext.getConf()));
    } finally {
      mReinitializer.end();
    }
    mReinitializer.reset(this);
  }

  /**
   * @return the {@link MasterClientContext} backing this context
   */
  public MasterClientContext getMasterClientContext() {
    return mMasterClientContext;
  }

  /**
   * @return the {@link ClientContext} backing this {@link FileSystemContext}
   */
  public ClientContext getClientContext() {
    return mClientContext;
  }

  /**
   * @return the cluster level configuration backing this {@link FileSystemContext}
   */
  public AlluxioConfiguration getClusterConf() {
    return mClientContext.getConf();
  }

  /**
   * The path level configuration is a {@link SpecificPathConfiguration}.
   *
   * If path level configuration has never been loaded from meta master yet, it will be loaded.
   *
   * @param path the path to get the configuration for
   * @return the path level configuration for the specific path
   */
  public AlluxioConfiguration getPathConf(AlluxioURI path) {
    try {
      mClientContext.loadPathConfIfNotLoaded(getMasterAddress());
    } catch (AlluxioStatusException e) {
      throw new RuntimeException("Failed to load path level configuration from meta master", e);
    }
    return new SpecificPathConfiguration(mClientContext.getConf(), mClientContext.getPathConf(),
        path);
  }

  /**
   * @return the master address
   * @throws UnavailableException if the master address cannot be determined
   */
  public synchronized InetSocketAddress getMasterAddress() throws UnavailableException {
    return mMasterInquireClient.getPrimaryRpcAddress();
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
   * Acquires a block worker client from the client pools. If there is no available client instance
   * available in the pool, it tries to create a new one. And an exception is thrown if it fails to
   * create a new one.
   *
   * @param workerNetAddress the network address of the channel
   * @return the acquired block worker
   */
  public BlockWorkerClient acquireBlockWorkerClient(final WorkerNetAddress workerNetAddress)
      throws IOException {
    return acquireBlockWorkerClientInternal(workerNetAddress, mClientContext.getSubject());
  }

  private BlockWorkerClient acquireBlockWorkerClientInternal(
      final WorkerNetAddress workerNetAddress, final Subject subject) throws IOException {
    SocketAddress address =
        NetworkAddressUtils.getDataPortSocketAddress(workerNetAddress, getClusterConf());
    GrpcServerAddress serverAddress = new GrpcServerAddress(workerNetAddress.getHost(), address);
    ClientPoolKey key = new ClientPoolKey(address,
        AuthenticationUserUtils.getImpersonationUser(subject, getClusterConf()));
    return mBlockWorkerClientPool.computeIfAbsent(key,
        k -> new BlockWorkerClientPool(subject, serverAddress,
            getClusterConf().getInt(PropertyKey.USER_BLOCK_WORKER_CLIENT_POOL_SIZE),
            getClusterConf(), mWorkerGroup))
        .acquire();
  }

  /**
   * Releases a block worker client to the client pools.
   *
   * @param workerNetAddress the address of the channel
   * @param client the client to release
   */
  public void releaseBlockWorkerClient(WorkerNetAddress workerNetAddress,
      BlockWorkerClient client) {
    SocketAddress address = NetworkAddressUtils.getDataPortSocketAddress(workerNetAddress,
        getClusterConf());
    ClientPoolKey key = new ClientPoolKey(address, AuthenticationUserUtils.getImpersonationUser(
        mClientContext.getSubject(), getClusterConf()));
    if (mBlockWorkerClientPool.containsKey(key)) {
      mBlockWorkerClientPool.get(key).release(client);
    } else {
      LOG.warn("No client pool for key {}, closing client instead. Context is closed: {}",
          key, mClosed.get());
      try {
        client.close();
      } catch (IOException e) {
        LOG.warn("Error closing block worker client for key {}", key, e);
      }
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
      if (addresses.get(0).getHost().equals(NetworkAddressUtils.getClientHostName(mClientContext
          .getConf()))) {
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
    String localHostname = NetworkAddressUtils.getClientHostName(mClientContext.getConf());
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
    private final Thread mLastHeartbeatThread;

    /**
     * Creates a new metrics master shutdown hook.
     */
    public MetricsMasterSyncShutDownHook() {
      mLastHeartbeatThread = new Thread(() -> {
        if (mClientMasterSync != null) {
          try {
            mClientMasterSync.heartbeat();
          } catch (InterruptedException e) {
            return;
          }
        }
      });
      mLastHeartbeatThread.setDaemon(true);
    }

    @Override
    public void run() {
      mLastHeartbeatThread.start();
      try {
        // Shutdown hooks should run quickly, so we limit the wait time to 500ms. It isn't the end
        // of the world if the final heartbeat fails.
        mLastHeartbeatThread.join(500);
      } catch (InterruptedException e) {
        return;
      } finally {
        if (mLastHeartbeatThread.isAlive()) {
          LOG.warn("Failed to heartbeat to the metrics master before exit");
        }
      }
    }
  }

  /**
   * Class that contains metrics about FileSystemContext.
   */
  @ThreadSafe
  private static final class Metrics {
    private static void initializeGauges() {
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName("GrpcConnectionsOpen"),
          new Gauge<Long>() {
            @Override
            public Long getValue() {
              long ret = 0;
              // TODO(feng): use gRPC API to collect metrics for connections
              return ret;
            }
          });
    }

    private Metrics() {} // prevent instantiation
  }

  /**
   * Key for block worker client pools. This requires both the worker address and the username, so
   * that block workers are created for different users.
   */
  private static final class ClientPoolKey {
    private final SocketAddress mSocketAddress;
    private final String mUsername;

    public ClientPoolKey(SocketAddress socketAddress, String username) {
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
      if (!(o instanceof ClientPoolKey)) {
        return false;
      }
      ClientPoolKey that = (ClientPoolKey) o;
      return Objects.equal(mSocketAddress, that.mSocketAddress)
          && Objects.equal(mUsername, that.mUsername);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("socketAddress", mSocketAddress)
          .add("username", mUsername)
          .toString();
    }
  }
}
