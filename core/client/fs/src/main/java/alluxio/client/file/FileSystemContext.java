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
import alluxio.client.file.FileSystemContextReinitializer.ReinitBlockerResource;
import alluxio.client.metrics.MetricsHeartbeatContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.path.SpecificPathConfiguration;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.GrpcServerAddress;
import alluxio.master.MasterClientContext;
import alluxio.master.MasterInquireClient;
import alluxio.resource.CloseableResource;
import alluxio.security.authentication.AuthenticationUserUtils;
import alluxio.util.IdUtils;
import alluxio.util.network.NettyUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
public final class FileSystemContext implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemContext.class);

  /**
   * Unique ID for each FileSystemContext.
   * One example usage is to uniquely identify the heartbeat thread for ConfigHashSync.
   */
  private final String mId;

  /**
   * Marks whether the context has been closed, closing the context means releasing all resources
   * in the context like clients and thread pools.
   */
  private AtomicBoolean mClosed = new AtomicBoolean(false);

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
  private final ConcurrentHashMap<ClientPoolKey, BlockWorkerClientPool>
      mBlockWorkerClientPool = new ConcurrentHashMap<>();

  /**
   * Used in {@link #mBlockWorkerClientPool}.
   */
  private volatile EventLoopGroup mWorkerGroup;
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

  /**
   * Creates a {@link FileSystemContext} with a null subject.
   *
   * @param conf Alluxio configuration
   * @return an instance of file system context with no subject associated
   */
  public static FileSystemContext create(AlluxioConfiguration conf) {
    Preconditions.checkNotNull(conf);
    return create(null, conf);
  }

  /**
   * @param subject the parent subject, set to null if not present
   * @param conf Alluxio configuration
   * @return a context
   */
  public static FileSystemContext create(@Nullable Subject subject,
      @Nullable AlluxioConfiguration conf) {
    FileSystemContext context = new FileSystemContext();
    ClientContext ctx = ClientContext.create(subject, conf);
    MasterInquireClient inquireClient = MasterInquireClient.Factory.create(ctx.getClusterConf());
    context.init(ctx, inquireClient);
    return context;
  }

  /**
   * @param clientContext the {@link alluxio.ClientContext} containing the subject and configuration
   * @return the {@link alluxio.client.file.FileSystemContext}
   */
  public static FileSystemContext create(ClientContext clientContext) {
    FileSystemContext ctx = new FileSystemContext();
    ctx.init(clientContext, MasterInquireClient.Factory.create(clientContext.getClusterConf()));
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
    FileSystemContext context = new FileSystemContext();
    ClientContext ctx = ClientContext.create(subject, alluxioConf);
    context.init(ctx, masterInquireClient);
    return context;
  }

  /**
   * Initializes FileSystemContext ID.
   */
  private FileSystemContext() {
    mId = IdUtils.createFileSystemContextId();
  }

  /**
   * Initializes the context. Only called in the factory methods.
   *
   * @param masterInquireClient the client to use for determining the master
   */
  private synchronized void init(ClientContext clientContext,
      MasterInquireClient masterInquireClient) {
    initContext(clientContext, masterInquireClient);
    mReinitializer = new FileSystemContextReinitializer(this);
  }

  private synchronized void initContext(ClientContext ctx,
      MasterInquireClient masterInquireClient) {
    mClosed.set(false);
    mMasterClientContext = MasterClientContext.newBuilder(ctx)
        .setMasterInquireClient(masterInquireClient).build();
    mFileSystemMasterClientPool = new FileSystemMasterClientPool(mMasterClientContext);
    mBlockMasterClientPool = new BlockMasterClientPool(mMasterClientContext);
    mWorkerGroup = NettyUtils.createEventLoop(NettyUtils.getUserChannel(getClusterConf()),
        getClusterConf().getInt(PropertyKey.USER_NETWORK_NETTY_WORKER_THREADS),
        String.format("alluxio-client-nettyPool-%s-%%d", mId), true);
    mMetricsEnabled = getClusterConf().getBoolean(PropertyKey.USER_METRICS_COLLECTION_ENABLED);
    if (mMetricsEnabled) {
      MetricsHeartbeatContext.addHeartbeat(getClientContext(), masterInquireClient);
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
    closeContext();
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
      mWorkerGroup.shutdownGracefully(1L, 10L, TimeUnit.SECONDS);
      mFileSystemMasterClientPool.close();
      mFileSystemMasterClientPool = null;
      mBlockMasterClientPool.close();
      mBlockMasterClientPool = null;
      for (BlockWorkerClientPool pool : mBlockWorkerClientPool.values()) {
        pool.close();
      }
      mBlockWorkerClientPool.clear();
      mLocalWorkerInitialized = false;
      mLocalWorker = null;

      if (mMetricsEnabled) {
        MetricsHeartbeatContext.removeHeartbeat(getClientContext());
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
      closeContext();
      initContext(getClientContext(), MasterInquireClient.Factory.create(getClusterConf()));
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

  private FileSystemMasterClient acquireMasterClient() {
    try (ReinitBlockerResource r = blockReinit()) {
      return mFileSystemMasterClientPool.acquire();
    }
  }

  private void releaseMasterClient(FileSystemMasterClient client) {
    try (ReinitBlockerResource r = blockReinit()) {
      if (!client.isClosed()) {
        // The client might have been closed during reinitialization.
        mFileSystemMasterClientPool.release(client);
      }
    }
  }

  /**
   * Acquires a file system master client from the file system master client pool. The resource is
   * {@code Closeable}.
   *
   * @return the acquired file system master client resource
   */
  public CloseableResource<FileSystemMasterClient> acquireMasterClientResource() {
    return new CloseableResource<FileSystemMasterClient>(acquireMasterClient()) {
      @Override
      public void close() {
        releaseMasterClient(get());
      }
    };
  }

  private BlockMasterClient acquireBlockMasterClient() {
    try (ReinitBlockerResource r = blockReinit()) {
      return mBlockMasterClientPool.acquire();
    }
  }

  private void releaseBlockMasterClient(BlockMasterClient client) {
    try (ReinitBlockerResource r = blockReinit()) {
      if (!client.isClosed()) {
        // The client might have been closed during reinitialization.
        mBlockMasterClientPool.release(client);
      }
    }
  }

  /**
   * Acquires a block master client resource from the block master client pool. The resource is
   * {@code Closeable}.
   *
   * @return the acquired block master client resource
   */
  public CloseableResource<BlockMasterClient> acquireBlockMasterClientResource() {
    return new CloseableResource<BlockMasterClient>(acquireBlockMasterClient()) {
      @Override
      public void close() {
        releaseBlockMasterClient(get());
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
    try (ReinitBlockerResource r = blockReinit()) {
      return acquireBlockWorkerClientInternal(workerNetAddress, getClientContext().getSubject());
    }
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
    if (client.isShutdown()) {
      // Client might have been shutdown during reinitialization.
      return;
    }
    try (ReinitBlockerResource r = blockReinit()) {
      SocketAddress address = NetworkAddressUtils.getDataPortSocketAddress(workerNetAddress,
          getClusterConf());
      ClientPoolKey key = new ClientPoolKey(address, AuthenticationUserUtils.getImpersonationUser(
          getClientContext().getSubject(), getClusterConf()));
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
