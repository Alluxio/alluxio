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

import alluxio.ClientContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.PropertyKey;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.block.BlockMasterClientPool;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.metrics.ClientMasterSync;
import alluxio.client.metrics.MetricsMasterClient;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.UnavailableException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.MasterClientConfig;
import alluxio.master.MasterInquireClient;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.CloseableResource;
import alluxio.util.IdUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.ThreadUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.codahale.metrics.Gauge;
import com.google.common.annotations.VisibleForTesting;
import jline.internal.Nullable;
import jline.internal.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;

/**
 * A context that isolates all operations within a {@link FileSystem}. Typically a user
 * will only need more than one instance of a {@link FileSystemContext} if they wish to connect
 * to Alluxio under different configuration values within the same JVM. {@link FileSystemContext}
 * instances should be created sparingly as each instance creates its own thread pool of
 * {@link FileSystemMasterClient} which can lead to inefficient use of client machine resources.
 *
 * <p>
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

  // Closed flag for debugging information.
  private final AtomicBoolean mClosed;

  private ExecutorService mExecutorService;
  private MetricsMasterClient mMetricsMasterClient;
  private ClientMasterSync mClientMasterSync;

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

  private final ClientContext mClientContext;
  private final String mAppId;

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
  public static FileSystemContext create(Subject subject,
      @Nullable AlluxioConfiguration alluxioConf) {
    AlluxioProperties props = null;
    if (alluxioConf != null) {
      props = alluxioConf.getProperties();
    }
    FileSystemContext context = new FileSystemContext(subject, props);
    context.init(MasterInquireClient.Factory.create(context.mClientContext.getConf()));
    return context;
  }

  /**
   * @param clientContext the {@link alluxio.ClientContext} containing the subject and configuration
   * @return the {@link alluxio.client.file.FileSystemContext}
   */
  public static FileSystemContext create(ClientContext clientContext) {
    Preconditions.checkNotNull(clientContext);
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
    AlluxioProperties props = null;
    if (alluxioConf != null) {
      props = alluxioConf.getProperties();
    }
    FileSystemContext context = new FileSystemContext(subject, props);
    context.init(masterInquireClient);
    return context;
  }

  /**
   * Creates a file system context with a subject.
   *
   * @param subject the parent subject, set to null if not present
   */
  private FileSystemContext(Subject subject, @Nullable AlluxioProperties alluxioProps) {
      this(ClientContext.create(subject, alluxioProps));
  }

  /**
   * Creates a file system context with a subject.
   *
   * @param ctx the parent subject, set to null if not present
   */
  private FileSystemContext(ClientContext ctx) {
    mClientContext = ctx;
    mExecutorService = Executors.newFixedThreadPool(1,
        ThreadFactoryUtils.build("metrics-master-heartbeat-%d", true));
    mClosed = new AtomicBoolean(false);

    mAppId = ctx.getConf().isSet(PropertyKey.USER_APP_ID)
                 ? ctx.getConf().get(PropertyKey.USER_APP_ID) : IdUtils.createFileSystemContextId();
    LOG.info("Created filesystem context with id {}. This ID will be used for identifying info "
            + "from the client, such as metrics. It can be set manually through the {} property",
        mAppId, PropertyKey.Name.USER_APP_ID);
  }

  /**
   * Initializes the context. Only called in the factory methods and reset.
   *
   * @param masterInquireClient the client to use for determining the master
   */
  private synchronized void init(MasterInquireClient masterInquireClient) {
    mMasterInquireClient = masterInquireClient;
    mFileSystemMasterClientPool =
        new FileSystemMasterClientPool(mClientContext.getSubject(), mMasterInquireClient,
            mClientContext.getConf());
    mBlockMasterClientPool = new BlockMasterClientPool(mClientContext.getSubject(),
        mMasterInquireClient, mClientContext.getConf());
    mClosed.set(false);

    if (mClientContext.getConf().getBoolean(PropertyKey.USER_METRICS_COLLECTION_ENABLED)) {
      // setup metrics master client sync
      mMetricsMasterClient = new MetricsMasterClient(MasterClientConfig
          .newBuilder(mClientContext.getConf())
          .setSubject(mClientContext.getSubject())
          .setMasterInquireClient(mMasterInquireClient)
          .build());
      mClientMasterSync = new ClientMasterSync(mMetricsMasterClient, mAppId);
      mExecutorService = Executors.newFixedThreadPool(1,
          ThreadFactoryUtils.build("metrics-master-heartbeat-%d", true));
      mExecutorService
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
    if (!mClosed.get()) {
      mFileSystemMasterClientPool.close();
      mFileSystemMasterClientPool = null;
      mBlockMasterClientPool.close();
      mBlockMasterClientPool = null;
      mMasterInquireClient = null;

      if (mMetricsMasterClient != null) {
        ThreadUtils.shutdownAndAwaitTermination(mExecutorService,
            mClientContext.getConf().getMs(PropertyKey.METRICS_CONTEXT_SHUTDOWN_TIMEOUT));
        mMetricsMasterClient.close();
        mMetricsMasterClient = null;
        mClientMasterSync = null;
      }
      mLocalWorkerInitialized = false;
      mLocalWorker = null;
      mClosed.set(true);
    } else {
      LOG.warn("Attempted to close FileSystemContext with app ID {} which has already been closed"
          + " or not initialized.", mAppId);
    }
  }

  /**
   * Resets the context. It is only used in {@link alluxio.hadoop.AbstractFileSystem} and tests to
   * reset the default file system context.
   *
   */
  public synchronized void reset() throws IOException {
    close();
    init(MasterInquireClient.Factory.create(mClientContext.getConf()));
  }

  /**
   * @return the {@link ClientContext} backing this {@link FileSystemContext}
   */
  public ClientContext getClientContext() {
    return mClientContext;
  }

  /**
   * @return the {@link AlluxioConfiguration} backing this {@link FileSystemContext}
   */
  public AlluxioConfiguration getConf() {
    return mClientContext.getConf();
  }

  /**
   * @return The application ID associated with this {@link FileSystemContext}
   */
  public String getAppId() {
    return mAppId;
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
   * Acquires a block worker client. It may reuse the same connection if possible.
   *
   * @param workerNetAddress the network address of the channel
   * @return the acquired block worker
   */
  public BlockWorkerClient acquireBlockWorkerClient(final WorkerNetAddress workerNetAddress)
      throws IOException {
    SocketAddress address = NetworkAddressUtils.getDataPortSocketAddress(workerNetAddress,
        mClientContext.getConf());
    return BlockWorkerClient.Factory.create(mClientContext.getSubject(), address,
        mClientContext.getConf());
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
        mClientContext.getConf());
    try {
      client.close();
    } catch (IOException e) {
      LOG.warn("Error closing block worker client for address {}", address, e);
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
}
