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

import static java.util.stream.Collectors.toList;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.annotation.SuppressFBWarnings;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.block.BlockMasterClientPool;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.block.stream.BlockWorkerClientPool;
import alluxio.client.file.FileSystemContextReinitializer.ReinitBlockerResource;
import alluxio.client.metrics.MetricsHeartbeatContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ReconfigurableRegistry;
import alluxio.conf.path.SpecificPathConfiguration;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.GrpcServerAddress;
import alluxio.master.MasterClientContext;
import alluxio.master.MasterInquireClient;
import alluxio.membership.MembershipManager;
import alluxio.membership.NoOpMembershipManager;
import alluxio.metrics.MetricsSystem;
import alluxio.network.netty.NettyChannelPool;
import alluxio.network.netty.NettyClient;
import alluxio.refresh.RefreshPolicy;
import alluxio.refresh.TimeoutRefresh;
import alluxio.resource.CloseableResource;
import alluxio.resource.DynamicResourcePool;
import alluxio.security.authentication.AuthenticationUtils;
import alluxio.security.user.UserState;
import alluxio.util.CommonUtils;
import alluxio.util.IdUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.BlockWorker;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
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
@SuppressFBWarnings(
    value = "MS_SHOULD_BE_FINAL",
    justification = "Only applied to sFileSystemContextFactory, "
        + "sFileSystemContextFactory is for extension")
public class FileSystemContext implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemContext.class);

  public static FileSystemContextFactory sFileSystemContextFactory
      = new FileSystemContextFactory();

  /**
   * Unique ID for each FileSystemContext.
   * One example usage is to uniquely identify the heartbeat thread for ConfigHashSync.
   */
  private final String mId;

  /**
   * The block worker for worker internal clients to call worker operation directly
   * without going through the external RPC frameworks.
   */
  private final BlockWorker mBlockWorker;

  /**
   * Marks whether the context has been closed, closing the context means releasing all resources
   * in the context like clients and thread pools.
   */
  private final AtomicBoolean mClosed = new AtomicBoolean(false);

  // The netty data server channel pools.
  private final ConcurrentHashMap<SocketAddress, NettyChannelPool>
      mNettyChannelPools = new ConcurrentHashMap<>();

  @GuardedBy("this")
  private boolean mMetricsEnabled;

  //
  // Master related resources.
  //
  /**
   * The master client context holding the inquire client.
   */
  private volatile MasterClientContext mMasterClientContext;
  /**
   * Master client pools.
   */
  private volatile FileSystemMasterClientPool mFileSystemMasterClientPool;
  private volatile BlockMasterClientPool mBlockMasterClientPool;

  //
  // Worker related resources.
  //
  /**
   * The data server channel pools. This pool will only grow and keys are not removed.
   */
  private volatile ConcurrentHashMap<ClientPoolKey, BlockWorkerClientPool>
      mBlockWorkerClientPoolMap;
  @Nullable
  private MembershipManager mMembershipManager;

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

  /**
   * Reinitializer contains a daemon heartbeat thread to reinitialize this context when
   * configuration hashes change.
   */
  private volatile FileSystemContextReinitializer mReinitializer;

  /** Whether to do URI scheme validation for file systems using this context.  */
  private boolean mUriValidationEnabled = true;

  /** Cached map for workers. */
  @GuardedBy("mWorkerInfoList")
  private final AtomicReference<List<BlockWorkerInfo>> mWorkerInfoList = new AtomicReference<>();

  /** The policy to refresh workers list. */
  @GuardedBy("mWorkerInfoList")
  private final RefreshPolicy mWorkerRefreshPolicy;

  private final List<InetSocketAddress> mMasterAddresses;

  /**
   * FileSystemContextFactory, it can be extended.
   */
  public static class FileSystemContextFactory {
    /**
     * Default constructor.
     */
    public FileSystemContextFactory() {}

    /**
     * Creates a {@link FileSystemContext} with an empty subject
     * , a null local block worker, and the given master addresses.
     *
     * @param conf Alluxio configuration
     * @param masterAddresses the master addresses to use, this addresses will be
     *                      used across reinitialization
     * @return an instance of file system context with no subject associated
     */
    public FileSystemContext create(
        AlluxioConfiguration conf, List<InetSocketAddress> masterAddresses) {
      return FileSystemContext.create(conf, masterAddresses);
    }

    /**
     * Creates a {@link FileSystemContext} with an empty subject, default config
     * and a null local block worker.
     *
     * @return an instance of file system context with no subject associated
     */
    public FileSystemContext create() {
      return FileSystemContext.create();
    }

    /**
     * Creates a {@link FileSystemContext} with an empty subject
     * and a null local block worker.
     *
     * @param conf Alluxio configuration
     * @return an instance of file system context with no subject associated
     */
    public FileSystemContext create(AlluxioConfiguration conf) {
      return FileSystemContext.create(conf);
    }

    /**
     * @param subject the parent subject
     * @param conf Alluxio configuration
     * @return a context
     */
    public FileSystemContext create(Subject subject,
        AlluxioConfiguration conf) {
      return FileSystemContext.create(subject, conf);
    }

    /**
     * @param clientContext the {@link alluxio.ClientContext} containing the subject
     *  and configuration
     * @return the {@link alluxio.client.file.FileSystemContext}
     */
    public FileSystemContext create(ClientContext clientContext) {
      return FileSystemContext.create(clientContext);
    }

    /**
     * @param ctx client context
     * @param blockWorker block worker
     * @return a context
     */
    public FileSystemContext create(ClientContext ctx,
        @Nullable BlockWorker blockWorker) {
      return FileSystemContext.create(ctx, blockWorker);
    }

    /**
     * @param ctx client context
     * @param blockWorker block worker
     * @param masterAddresses is non-null then the addresses used to connect to the master
     * @return a context
     */
    public FileSystemContext create(ClientContext ctx,
        @Nullable BlockWorker blockWorker, @Nullable List<InetSocketAddress> masterAddresses) {
      return FileSystemContext.create(ctx, blockWorker, masterAddresses);
    }

    /**
     * This method is provided for testing, use the {@link FileSystemContext#create} methods. The
     * returned context object will not be cached automatically.
     *
     * @param subject the parent subject
     * @param masterInquireClient the client to use for determining the master; note that if the
     *        context is reset, this client will be replaced with a new masterInquireClient based on
     *        the original configuration.
     * @param alluxioConf Alluxio configuration
     * @return the context
     */
    public FileSystemContext create(Subject subject, MasterInquireClient masterInquireClient,
        AlluxioConfiguration alluxioConf) {
      return FileSystemContext.create(subject, masterInquireClient, alluxioConf);
    }
  }

  /**
   * Creates a {@link FileSystemContext} with an empty subject
   * , a null local block worker, and the given master addresses.
   *
   * @param conf Alluxio configuration
   * @param masterAddresses the master addresses to use, this addresses will be
   *                      used across reinitialization
   * @return an instance of file system context with no subject associated
   */
  public static FileSystemContext create(
      AlluxioConfiguration conf, List<InetSocketAddress> masterAddresses) {
    return create(ClientContext.create(conf), null, masterAddresses);
  }

  /**
   * Creates a {@link FileSystemContext} with an empty subject, default config
   * and a null local block worker.
   *
   * @return an instance of file system context with no subject associated
   */
  public static FileSystemContext create() {
    return create(ClientContext.create());
  }

  /**
   * Creates a {@link FileSystemContext} with an empty subject
   * and a null local block worker.
   *
   * @param conf Alluxio configuration
   * @return an instance of file system context with no subject associated
   */
  public static FileSystemContext create(AlluxioConfiguration conf) {
    Preconditions.checkNotNull(conf);
    return create(ClientContext.create(conf));
  }

  /**
   * @param subject the parent subject
   * @param conf Alluxio configuration
   * @return a context
   */
  public static FileSystemContext create(Subject subject,
      AlluxioConfiguration conf) {
    return create(ClientContext.create(subject, conf));
  }

  /**
   * @param clientContext the {@link alluxio.ClientContext} containing the subject and configuration
   * @return the {@link alluxio.client.file.FileSystemContext}
   */
  public static FileSystemContext create(ClientContext clientContext) {
    return create(clientContext, null, null);
  }

  /**
   * @param ctx client context
   * @param blockWorker block worker
   * @return a context
   */
  public static FileSystemContext create(ClientContext ctx,
      @Nullable BlockWorker blockWorker) {
    return create(ctx, blockWorker, null);
  }

  /**
   * @param ctx client context
   * @param blockWorker block worker
   * @param masterAddresses is non-null then the addresses used to connect to the master
   * @return a context
   */
  public static FileSystemContext create(ClientContext ctx,
      @Nullable BlockWorker blockWorker, @Nullable List<InetSocketAddress> masterAddresses) {
    FileSystemContext context = new FileSystemContext(ctx.getClusterConf(), blockWorker,
        masterAddresses);
    MasterInquireClient inquireClient;
    if (masterAddresses != null) {
      inquireClient = MasterInquireClient.Factory.createForAddresses(masterAddresses,
          ctx.getClusterConf(), ctx.getUserState());
    } else {
      inquireClient = MasterInquireClient.Factory.create(
          ctx.getClusterConf(), ctx.getUserState());
    }
    context.init(ctx, inquireClient);
    return context;
  }

  /**
   * This method is provided for testing, use the {@link FileSystemContext#create} methods. The
   * returned context object will not be cached automatically.
   *
   * @param subject the parent subject
   * @param masterInquireClient the client to use for determining the master; note that if the
   *        context is reset, this client will be replaced with a new masterInquireClient based on
   *        the original configuration.
   * @param alluxioConf Alluxio configuration
   * @return the context
   */
  @VisibleForTesting
  public static FileSystemContext create(Subject subject, MasterInquireClient masterInquireClient,
      AlluxioConfiguration alluxioConf) {
    FileSystemContext context = new FileSystemContext(alluxioConf, null, null);
    ClientContext ctx = ClientContext.create(subject, alluxioConf);
    context.init(ctx, masterInquireClient);
    return context;
  }

  /**
   * Initializes FileSystemContext ID.
   *
   * @param conf Alluxio configuration
   * @param blockWorker block worker
   */
  protected FileSystemContext(AlluxioConfiguration conf, @Nullable BlockWorker blockWorker,
                            @Nullable List<InetSocketAddress> masterAddresses) {
    mId = IdUtils.createFileSystemContextId();
    mBlockWorker = blockWorker;
    mMasterAddresses = masterAddresses;
    mWorkerRefreshPolicy =
        new TimeoutRefresh(conf.getMs(PropertyKey.USER_WORKER_LIST_REFRESH_INTERVAL));
    LOG.debug("Created context with id: {}, with local block worker: {}",
        mId, mBlockWorker != null);
  }

  /**
   * Initializes the context. Only called in the factory methods.
   *
   * @param masterInquireClient the client to use for determining the master
   */
  protected synchronized void init(ClientContext clientContext,
      MasterInquireClient masterInquireClient) {
    initContext(clientContext, masterInquireClient);
    reCreateReinitialize(null);
  }

  protected void reCreateReinitialize(
      @Nullable FileSystemContextReinitializer fileSystemContextReinitializer) {
    if (fileSystemContextReinitializer == null) {
      mReinitializer = new FileSystemContextReinitializer(this);
    } else {
      mReinitializer = fileSystemContextReinitializer;
    }
  }

  protected synchronized void initContext(ClientContext ctx,
      MasterInquireClient masterInquireClient) {
    mClosed.set(false);
    mMasterClientContext = MasterClientContext.newBuilder(ctx)
        .setMasterInquireClient(masterInquireClient).build();
    mMetricsEnabled = getClusterConf().getBoolean(PropertyKey.USER_METRICS_COLLECTION_ENABLED);
    if (mMetricsEnabled) {
      MetricsSystem.startSinks(getClusterConf().getString(PropertyKey.METRICS_CONF_FILE));
      MetricsHeartbeatContext.addHeartbeat(getClientContext(), masterInquireClient);
    }
    mFileSystemMasterClientPool = new FileSystemMasterClientPool(mMasterClientContext);
    mBlockMasterClientPool = new BlockMasterClientPool(mMasterClientContext);
    mBlockWorkerClientPoolMap = new ConcurrentHashMap<>();
    mUriValidationEnabled = ctx.getUriValidationEnabled();
    mMembershipManager = MembershipManager.Factory.create(getClusterConf());
  }

  /**
   * Closes all the resources associated with the context. Make sure all the resources are released
   * back to this context before calling this close. After closing the context, all the resources
   * that acquired from this context might fail. Only call this when you are done with using
   * the {@link FileSystem} associated with this {@link FileSystemContext}.
   */
  @Override
  public synchronized void close() throws IOException {
    LOG.debug("Closing context with id: {}", mId);
    mReinitializer.close();
    closeContext();
    LOG.debug("Closed context with id: {}", mId);
  }

  private synchronized void closeContext() throws IOException {
    if (!mClosed.get()) {
      // Setting closed should be the first thing we do because if any of the close operations
      // fail we'll only have a half-closed object and performing any more operations or closing
      // again on a half-closed object can possibly result in more errors (i.e. NPE). Setting
      // closed first is also recommended by the JDK that in implementations of #close() that
      // developers should first mark their resources as closed prior to any exceptions being
      // thrown.
      mClosed.set(true);
      LOG.debug("Closing fs master client pool with current size: {} for id: {}",
          mFileSystemMasterClientPool.size(), mId);
      mFileSystemMasterClientPool.close();
      mFileSystemMasterClientPool = null;
      LOG.debug("Closing block master client pool with size: {} for id: {}",
          mBlockMasterClientPool.size(), mId);
      mBlockMasterClientPool.close();
      mBlockMasterClientPool = null;
      for (BlockWorkerClientPool pool : mBlockWorkerClientPoolMap.values()) {
        LOG.debug("Closing block worker client pool with size: {} for id: {}", pool.size(), mId);
        pool.close();
      }
      // Close worker group after block master clients in order to allow
      // clean termination for open streams.
      mBlockWorkerClientPoolMap.clear();
      mBlockWorkerClientPoolMap = null;
      mLocalWorkerInitialized = false;
      mLocalWorker = null;

      if (mMetricsEnabled) {
        MetricsHeartbeatContext.removeHeartbeat(getClientContext());
      }
      LOG.debug("Closing membership manager.");
      try (AutoCloseable ignoredCloser = mMembershipManager) {
        // do nothing as we are closing
      } catch (Exception e) {
        throw new IOException(e);
      }
    } else {
      LOG.warn("Attempted to close FileSystemContext which has already been closed or not "
          + "initialized.");
    }
  }

  /**
   * Acquires the resource to block reinitialization.
   *
   * If reinitialization is happening, this method will block until reinitialization succeeds or
   * fails, if it fails, a RuntimeException will be thrown explaining the
   * reinitialization's failure and automatically closes the resource.
   *
   * RuntimeException is thrown because this method is called before requiring resources from the
   * context, if reinitialization fails, the resources might be half closed, to prevent resource
   * leaks, we thrown RuntimeException here to force callers to fail since there is no way to
   * recover.
   *
   * @return the resource
   */
  public ReinitBlockerResource blockReinit() {
    try {
      return mReinitializer.block();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Closes the context, updates configuration from meta master, then re-initializes the context.
   *
   * The reinitializer is not closed, which means the heartbeat thread inside it is not stopped.
   * The reinitializer will be reset with the updated context if reinitialization succeeds,
   * otherwise, the reinitializer is not reset.
   *
   * Blocks until there is no active RPCs.
   *
   * @param updateClusterConf whether cluster level configuration should be updated
   * @param updatePathConf whether path level configuration should be updated
   * @throws UnavailableException when failed to load configuration from master
   * @throws IOException when failed to close the context
   */
  public void reinit(boolean updateClusterConf, boolean updatePathConf)
      throws UnavailableException, IOException {
    try (Closeable r = mReinitializer.allow()) {
      InetSocketAddress masterAddr;
      try {
        masterAddr = getMasterAddress();
      } catch (IOException e) {
        throw new UnavailableException("Failed to get master address during reinitialization", e);
      }
      try {
        getClientContext().loadConf(masterAddr, updateClusterConf, updatePathConf);
      } catch (AlluxioStatusException e) {
        // Failed to load configuration from meta master, maybe master is being restarted,
        // or their is a temporary network problem, give up reinitialization. The heartbeat thread
        // will try to reinitialize in the next heartbeat.
        throw new UnavailableException(String.format("Failed to load configuration from "
            + "meta master (%s) during reinitialization", masterAddr), e);
      }
      LOG.debug("Reinitializing FileSystemContext: update cluster conf: {}, update path conf:"
          + " {}", updateClusterConf, updatePathConf);
      closeContext();
      ReconfigurableRegistry.update();
      initContext(getClientContext(), mMasterAddresses != null
          ? MasterInquireClient.Factory.createForAddresses(mMasterAddresses,
          getClusterConf(), getClientContext().getUserState())
          : MasterInquireClient.Factory.create(getClusterConf(),
          getClientContext().getUserState()));
      LOG.debug("FileSystemContext re-initialized");
      mReinitializer.onSuccess();
    }
  }

  /**
   * @return the unique ID of this context
   */
  public String getId() {
    return mId;
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
    return mMasterClientContext;
  }

  /**
   * @return the cluster level configuration backing this {@link FileSystemContext}
   */
  public AlluxioConfiguration getClusterConf() {
    return getClientContext().getClusterConf();
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
    return new SpecificPathConfiguration(getClientContext().getClusterConf(),
        getClientContext().getPathConf(), path);
  }

  /**
   * @return the master address
   * @throws UnavailableException if the master address cannot be determined
   */
  public synchronized InetSocketAddress getMasterAddress() throws UnavailableException {
    return mMasterClientContext.getMasterInquireClient().getPrimaryRpcAddress();
  }

  /**
   * @return {@code true} if URI validation is enabled
   */
  public synchronized boolean getUriValidationEnabled() {
    return mUriValidationEnabled;
  }

  /**
   * Acquires a file system master client from the file system master client pool. The resource is
   * {@code Closeable}.
   *
   * @return the acquired file system master client resource
   */
  public CloseableResource<FileSystemMasterClient> acquireMasterClientResource() {
    try (ReinitBlockerResource r = blockReinit()) {
      return acquireClosableClientResource(mFileSystemMasterClientPool);
    }
  }

  /**
   * Acquires a block master client resource from the block master client pool. The resource is
   * {@code Closeable}.
   *
   * @return the acquired block master client resource
   */
  public CloseableResource<BlockMasterClient> acquireBlockMasterClientResource() {
    try (ReinitBlockerResource r = blockReinit()) {
      return acquireClosableClientResource(mBlockMasterClientPool);
    }
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
    SocketAddress address = NetworkAddressUtils.getDataPortSocketAddress(workerNetAddress,
        Configuration.global());
    if (!mNettyChannelPools.containsKey(address)) {
      Bootstrap bs = NettyClient.createClientBootstrap(address);
      bs.remoteAddress(address);
      NettyChannelPool pool = new NettyChannelPool(bs,
          Configuration.getInt(PropertyKey.USER_NETWORK_NETTY_CHANNEL_POOL_SIZE_MAX),
          Configuration.getMs(PropertyKey.USER_NETWORK_NETTY_CHANNEL_POOL_GC_THRESHOLD_MS));
      if (mNettyChannelPools.putIfAbsent(address, pool) != null) {
        // This can happen if this function is called concurrently.
        pool.close();
      }
    }
    return mNettyChannelPools.get(address).acquire();
  }

  protected ConcurrentHashMap<SocketAddress, NettyChannelPool> getNettyChannelPools() {
    return mNettyChannelPools;
  }

  /**
   * Releases a netty channel to the channel pools.
   *
   * @param workerNetAddress the address of the channel
   * @param channel the channel to release
   */
  public void releaseNettyChannel(WorkerNetAddress workerNetAddress, Channel channel) {
    SocketAddress address = NetworkAddressUtils.getDataPortSocketAddress(workerNetAddress,
        Configuration.global());
    if (mNettyChannelPools.containsKey(address)) {
      mNettyChannelPools.get(address).release(channel);
    } else {
      LOG.warn("No channel pool for address {}, closing channel instead. Context is closed: {}",
          address, mClosed.get());
      CommonUtils.closeChannel(channel);
    }
  }

  /**
   * Acquire a client resource from {@link #mBlockMasterClientPool} or
   * {@link #mFileSystemMasterClientPool}.
   *
   * Because it's possible for a context re-initialization to occur while the resource is
   * acquired this method uses an inline class which will save the reference to the pool used to
   * acquire the resource.
   *
   * There are three different cases to which may occur during the release of the resource
   *
   * 1. release while the context is re-initializing
   *    - The original {@link #mBlockMasterClientPool} or {@link #mFileSystemMasterClientPool}
   *    may be null, closed, or overwritten with a difference pool. The inner class here saves
   *    the original pool from being GCed because it holds a reference to the pool that was used
   *    to acquire the client initially. Releasing into the closed pool is harmless.
   * 2. release after the context has been re-initialized
   *    - Similar to the above scenario the original {@link #mBlockMasterClientPool} or
   *    {@link #mFileSystemMasterClientPool} are going to be using an entirely new pool. Since
   *    this method will save the original pool reference, this method would result in releasing
   *    into a closed pool which is harmless
   * 3. release before any re-initialization
   *    - This is the normal case. There are no special considerations
   *
   * @param pool the pool to acquire from and release to
   * @param <T> the resource type
   * @return a {@link CloseableResource}
   */
  private <T> CloseableResource<T> acquireClosableClientResource(DynamicResourcePool<T> pool) {
    try {
      return new CloseableResource<T>(pool.acquire()) {
        @Override
        public void closeResource() {
          pool.release(get());
        }
      };
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Acquires a block worker client from the client pools. If there is no available client instance
   * available in the pool, it tries to create a new one. And an exception is thrown if it fails to
   * create a new one.
   *
   * @param workerNetAddress the network address of the channel
   * @return the acquired block worker resource
   */
  public CloseableResource<BlockWorkerClient> acquireBlockWorkerClient(
      final WorkerNetAddress workerNetAddress)
      throws IOException {
    try (ReinitBlockerResource r = blockReinit()) {
      return acquireBlockWorkerClientInternal(workerNetAddress, getClientContext(),
          getClientContext().getUserState());
    }
  }

  private CloseableResource<BlockWorkerClient> acquireBlockWorkerClientInternal(
      final WorkerNetAddress workerNetAddress, final ClientContext context, UserState userState)
      throws IOException {
    SocketAddress address = NetworkAddressUtils
        .getRpcPortSocketAddress(workerNetAddress, context.getClusterConf());
    GrpcServerAddress serverAddress = GrpcServerAddress.create(workerNetAddress.getHost(), address);
    final ClientPoolKey key = new ClientPoolKey(address, AuthenticationUtils
            .getImpersonationUser(userState.getSubject(), context.getClusterConf()));
    final ConcurrentHashMap<ClientPoolKey, BlockWorkerClientPool> poolMap =
        mBlockWorkerClientPoolMap;
    BlockWorkerClientPool pool = poolMap.computeIfAbsent(
        key,
        k -> new BlockWorkerClientPool(userState, serverAddress,
            context.getClusterConf().getInt(PropertyKey.USER_BLOCK_WORKER_CLIENT_POOL_MIN),
            context.getClusterConf().getInt(PropertyKey.USER_BLOCK_WORKER_CLIENT_POOL_MAX),
            context.getClusterConf())
    );
    return new CloseableResource<BlockWorkerClient>(pool.acquire()) {
      @Override
      public void closeResource() {
        releaseBlockWorkerClient(get(), key, poolMap);
      }
    };
  }

  /**
   * Releases a block worker client to the client pools.
   *
   * @param client the client to release
   * @param key the key in the map of the pool from which the client was acquired
   * @param poolMap the client pool map
   */
  private static void releaseBlockWorkerClient(BlockWorkerClient client, final ClientPoolKey key,
      ConcurrentHashMap<ClientPoolKey, BlockWorkerClientPool> poolMap) {
    if (client == null) {
      return;
    }
    if (poolMap.containsKey(key)) {
      poolMap.get(key).release(client);
    } else {
      LOG.warn("No client pool for key {}, closing client instead. Context may have been closed",
          key);
      try {
        client.close();
      } catch (IOException e) {
        LOG.warn("Error closing block worker client for key {}", key, e);
      }
    }
  }

  /**
   * @return if the current client is embedded in a worker
   */
  public synchronized boolean hasProcessLocalWorker() {
    return mBlockWorker != null;
  }

  /**
   * Acquires the internal block worker as a gateway for worker internal clients to communicate
   * with the local worker directly without going through external RPC frameworks.
   *
   * @return the acquired block worker or null if this client is not interal to a block worker
   */
  public Optional<BlockWorker> getProcessLocalWorker() {
    return Optional.ofNullable(mBlockWorker);
  }

  /**
   * @return if there is a local worker running the same machine
   */
  public synchronized boolean hasNodeLocalWorker() throws IOException {
    if (!mLocalWorkerInitialized) {
      initializeLocalWorker();
    }
    return mLocalWorker != null;
  }

  /**
   * @return a local worker running the same machine, or null if none is found
   */
  public synchronized WorkerNetAddress getNodeLocalWorker() throws IOException {
    if (!mLocalWorkerInitialized) {
      initializeLocalWorker();
    }
    return mLocalWorker;
  }

  /**
   * Gets the cached worker information list.
   * This method is relatively cheap as the result is cached, but may not
   * be up-to-date. If up-to-date worker info list is required,
   * use {@link #getAllWorkers()} instead.
   *
   * @return the info of all block workers eligible for reads and writes
   */
  public List<BlockWorkerInfo> getCachedWorkers() throws IOException {
    synchronized (mWorkerInfoList) {
      if (mWorkerInfoList.get() == null || mWorkerInfoList.get().isEmpty()
          || mWorkerRefreshPolicy.attempt()) {
        mWorkerInfoList.set(getAllWorkers());
      }
      return mWorkerInfoList.get();
    }
  }

  /**
   * Gets the worker information list.
   * This method is more expensive than {@link #getCachedWorkers()}.
   * Used when more up-to-date data is needed.
   *
   * @return the info of all block workers
   */
  protected List<BlockWorkerInfo> getAllWorkers() throws IOException {
    // TODO(lucy) once ConfigHashSync reinit is gotten rid of, will remove the blockReinit
    // guard altogether
    try (ReinitBlockerResource r = blockReinit()) {
      // Use membership mgr
      if (mMembershipManager != null && !(mMembershipManager instanceof NoOpMembershipManager)) {
        return mMembershipManager.getAllMembers().stream()
            .map(w -> new BlockWorkerInfo(w.getAddress(), w.getCapacityBytes(), w.getUsedBytes()))
            .collect(toList());
      }
    }
    // Fall back to old way
    try (CloseableResource<BlockMasterClient> masterClientResource =
             acquireBlockMasterClientResource()) {
      return masterClientResource.get().getWorkerInfoList().stream()
          .map(w -> new BlockWorkerInfo(w.getAddress(), w.getCapacityBytes(), w.getUsedBytes()))
          .collect(toList());
    }
  }

  protected ConcurrentHashMap<ClientPoolKey, BlockWorkerClientPool> getBlockWorkerClientPoolMap() {
    return mBlockWorkerClientPoolMap;
  }

  private void initializeLocalWorker() throws IOException {
    List<WorkerNetAddress> addresses = getWorkerAddresses();
    if (!addresses.isEmpty()) {
      if (addresses.get(0).getHost().equals(NetworkAddressUtils.getClientHostName(
          getClusterConf()))) {
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
    try (CloseableResource<BlockMasterClient> masterClientResource =
        acquireBlockMasterClientResource()) {
      infos = masterClientResource.get().getWorkerInfoList();
    }
    if (infos.isEmpty()) {
      throw new UnavailableException(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage());
    }

    // Convert the worker infos into net addresses, if there are local addresses, only keep those
    List<WorkerNetAddress> workerNetAddresses = new ArrayList<>();
    List<WorkerNetAddress> localWorkerNetAddresses = new ArrayList<>();
    String localHostname = NetworkAddressUtils.getClientHostName(getClusterConf());
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
   * Key for block worker client pools. This requires both the worker address and the username, so
   * that block workers are created for different users.
   */
  protected static class ClientPoolKey {
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
