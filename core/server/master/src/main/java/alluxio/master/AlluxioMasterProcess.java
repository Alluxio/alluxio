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

package alluxio.master;

import static alluxio.util.network.NetworkAddressUtils.ServiceType;

import alluxio.AlluxioURI;
import alluxio.RuntimeConstants;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.executor.ExecutorServiceBuilder;
import alluxio.grpc.GrpcServer;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.grpc.GrpcService;
import alluxio.grpc.JournalDomain;
import alluxio.master.journal.DefaultJournalMaster;
import alluxio.master.journal.JournalMasterClientServiceHandler;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalUtils;
import alluxio.master.journal.raft.RaftJournalSystem;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.CloseableResource;
import alluxio.security.user.ServerUserState;
import alluxio.underfs.MasterUfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.CommonUtils;
import alluxio.util.CommonUtils.ProcessType;
import alluxio.util.JvmPauseMonitor;
import alluxio.util.URIUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.web.MasterWebServer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * An Alluxio Master which runs a web and rpc server to handle FileSystem operations.
 */
@NotThreadSafe
public class AlluxioMasterProcess extends MasterProcess {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioMasterProcess.class);

  /** The master registry. */
  private final MasterRegistry mRegistry = new MasterRegistry();

  /** The JVMMonitor Progress. */
  private JvmPauseMonitor mJvmPauseMonitor;

  /** The connection address for the rpc server. */
  final InetSocketAddress mRpcConnectAddress =
      NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, ServerConfiguration.global());

  /** The manager of safe mode state. */
  protected final SafeModeManager mSafeModeManager = new DefaultSafeModeManager();

  /** Master context. */
  protected final CoreMasterContext mContext;

  /** The manager for creating and restoring backups. */
  private final BackupManager mBackupManager = new BackupManager(mRegistry);

  /** The manager of all ufs. */
  private final MasterUfsManager mUfsManager = new MasterUfsManager();

  private AlluxioExecutorService mRPCExecutor = null;
  /** See {@link #isStopped()}. */
  protected final AtomicBoolean mIsStopped = new AtomicBoolean(false);

  /**
   * Creates a new {@link AlluxioMasterProcess}.
   */
  AlluxioMasterProcess(JournalSystem journalSystem) {
    super(journalSystem, ServiceType.MASTER_RPC, ServiceType.MASTER_WEB);
    try {
      if (!mJournalSystem.isFormatted()) {
        throw new RuntimeException(
            String.format("Journal %s has not been formatted!", mJournalSystem));
      }
      // Create masters.
      String baseDir = ServerConfiguration.getString(PropertyKey.MASTER_METASTORE_DIR);
      mContext = CoreMasterContext.newBuilder()
          .setJournalSystem(mJournalSystem)
          .setSafeModeManager(mSafeModeManager)
          .setBackupManager(mBackupManager)
          .setBlockStoreFactory(MasterUtils.getBlockStoreFactory(baseDir))
          .setInodeStoreFactory(MasterUtils.getInodeStoreFactory(baseDir))
          .setStartTimeMs(mStartTimeMs)
          .setPort(NetworkAddressUtils
              .getPort(ServiceType.MASTER_RPC, ServerConfiguration.global()))
          .setUfsManager(mUfsManager)
          .build();
      MasterUtils.createMasters(mRegistry, mContext);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <T extends Master> T getMaster(Class<T> clazz) {
    return mRegistry.get(clazz);
  }

  /**
   * @return true if Alluxio is running in safe mode, false otherwise
   */
  public boolean isInSafeMode() {
    return mSafeModeManager.isInSafeMode();
  }

  @Override
  @Nullable
  public InetSocketAddress getWebAddress() {
    if (mWebServer != null) {
      return new InetSocketAddress(mWebServer.getBindHost(), mWebServer.getLocalPort());
    }
    return null;
  }

  @Override
  public InetSocketAddress getRpcAddress() {
    return mRpcConnectAddress;
  }

  @Override
  public void start() throws Exception {
    LOG.info("Starting...");
    mJournalSystem.start();
    mJournalSystem.gainPrimacy();
    startMasters(true);
    startServing();
  }

  @Override
  public void stop() throws Exception {
    synchronized (mIsStopped) {
      if (mIsStopped.get()) {
        return;
      }
      LOG.info("Stopping...");
      stopCommonServices();
      mIsStopped.set(true);
      LOG.info("Stopped.");
    }
  }

  protected void stopCommonServices() throws Exception {
    stopRejectingServers();
    stopServing();
    mJournalSystem.stop();
    LOG.info("Closing all masters.");
    mRegistry.close();
    LOG.info("Closed all masters.");
  }

  private void initFromBackup(AlluxioURI backup) throws IOException {
    CloseableResource<UnderFileSystem> ufsResource;
    if (URIUtils.isLocalFilesystem(backup.toString())) {
      UnderFileSystem ufs = UnderFileSystem.Factory.create("/",
          UnderFileSystemConfiguration.defaults(ServerConfiguration.global()));
      ufsResource = new CloseableResource<UnderFileSystem>(ufs) {
        @Override
        public void closeResource() { }
      };
    } else {
      ufsResource = mUfsManager.getRoot().acquireUfsResource();
    }
    try (CloseableResource<UnderFileSystem> closeUfs = ufsResource;
         InputStream ufsIn = closeUfs.get().open(backup.getPath())) {
      LOG.info("Initializing metadata from backup {}", backup);
      mBackupManager.initFromBackup(ufsIn);
    }
  }

  /**
   * Starts all masters, including block master, FileSystem master, and additional masters.
   *
   * @param isLeader if the Master is leader
   */
  protected void startMasters(boolean isLeader) throws IOException {
    LOG.info("Starting all masters as: {}.", (isLeader) ? "leader" : "follower");
    if (isLeader) {
      if (ServerConfiguration.isSet(PropertyKey.MASTER_JOURNAL_INIT_FROM_BACKUP)) {
        AlluxioURI backup =
            new AlluxioURI(ServerConfiguration.getString(
                PropertyKey.MASTER_JOURNAL_INIT_FROM_BACKUP));
        if (mJournalSystem.isEmpty()) {
          initFromBackup(backup);
        } else {
          LOG.info("The journal system is not freshly formatted, skipping restoring backup from "
              + backup);
        }
      }
      mSafeModeManager.notifyPrimaryMasterStarted();
    } else {
      startRejectingServers();
    }
    mRegistry.start(isLeader);
    // Signal state-lock-manager that masters are ready.
    mContext.getStateLockManager().mastersStartedCallback();
    LOG.info("All masters started.");
  }

  /**
   * Stops all masters, including block master, fileSystem master and additional masters.
   */
  protected void stopMasters() {
    try {
      LOG.info("Stopping all masters.");
      mRegistry.stop();
      LOG.info("All masters stopped.");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Starts serving web ui server, resetting master web port, adding the metrics servlet to the web
   * server and starting web ui.
   */
  protected void startServingWebServer() {
    LOG.info("Alluxio master web server version {}. webAddress={}",
        RuntimeConstants.VERSION, mWebBindAddress);
    stopRejectingWebServer();
    mWebServer =
        new MasterWebServer(ServiceType.MASTER_WEB.getServiceName(), mWebBindAddress, this);
    // reset master web port
    // start web ui
    mWebServer.start();
  }

  /**
   * Starts jvm monitor process, to monitor jvm.
   */
  protected void startJvmMonitorProcess() {
    if (ServerConfiguration.getBoolean(PropertyKey.MASTER_JVM_MONITOR_ENABLED)) {
      mJvmPauseMonitor = new JvmPauseMonitor(
          ServerConfiguration.getMs(PropertyKey.JVM_MONITOR_SLEEP_INTERVAL_MS),
          ServerConfiguration.getMs(PropertyKey.JVM_MONITOR_WARN_THRESHOLD_MS),
          ServerConfiguration.getMs(PropertyKey.JVM_MONITOR_INFO_THRESHOLD_MS));
      mJvmPauseMonitor.start();
      MetricsSystem.registerGaugeIfAbsent(
              MetricsSystem.getMetricName(MetricKey.TOTAL_EXTRA_TIME.getName()),
              mJvmPauseMonitor::getTotalExtraTime);
      MetricsSystem.registerGaugeIfAbsent(
              MetricsSystem.getMetricName(MetricKey.INFO_TIME_EXCEEDED.getName()),
              mJvmPauseMonitor::getInfoTimeExceeded);
      MetricsSystem.registerGaugeIfAbsent(
              MetricsSystem.getMetricName(MetricKey.WARN_TIME_EXCEEDED.getName()),
              mJvmPauseMonitor::getWarnTimeExceeded);
    }
  }

  /**
   * Starts serving, letting {@link MetricsSystem} start sink and starting the web ui server and RPC
   * Server.
   *
   * @param startMessage empty string or the message that the master gains the leadership
   * @param stopMessage empty string or the message that the master loses the leadership
   */
  @Override
  protected void startServing(String startMessage, String stopMessage) {
    // start all common services for non-ha master or leader master
    startCommonServices();
    startJvmMonitorProcess();
    startLeaderServing(startMessage, stopMessage);
  }

  protected void startLeaderServing(String startMessage, String stopMessage) {
    startServingRPCServer();
    LOG.info(
        "Alluxio master version {} started{}. bindAddress={}, connectAddress={}, webAddress={}",
        RuntimeConstants.VERSION, startMessage, mRpcBindAddress, mRpcConnectAddress,
        mWebBindAddress);
    // Blocks until RPC server is shut down. (via #stopServing)
    mGrpcServer.awaitTermination();
    LOG.info("Alluxio master ended {}", stopMessage);
  }

  /**
   * Entrance of the services that can run whether the master state is the primary or standby.
   */
  protected void startCommonServices() {
    MetricsSystem.startSinks(
        ServerConfiguration.getString(PropertyKey.METRICS_CONF_FILE));
    startServingWebServer();
  }

  /**
   * Starts the gRPC server. The AlluxioMaster registers the Services of registered
   * {@link Master}s and meta services.
   */
  protected void startServingRPCServer() {
    stopRejectingRpcServer();

    LOG.info("Starting gRPC server on address:{}", mRpcBindAddress);
    mGrpcServer = createRPCServer();

    try {
      // Start serving.
      mGrpcServer.start();
      mSafeModeManager.notifyRpcServerStarted();
      // Acquire and log bind port from newly started server.
      InetSocketAddress listeningAddress = InetSocketAddress
          .createUnresolved(mRpcBindAddress.getHostName(), mGrpcServer.getBindPort());
      LOG.info("gRPC server listening on: {}", listeningAddress);
    } catch (IOException e) {
      LOG.error("gRPC serving failed.", e);
      throw new RuntimeException("gRPC serving failed");
    }
  }

  private GrpcServer createRPCServer() {
    // Create an executor for Master RPC server.
    mRPCExecutor = ExecutorServiceBuilder.buildExecutorService(
        ExecutorServiceBuilder.RpcExecutorHost.MASTER);
    MetricsSystem.registerGaugeIfAbsent(MetricKey.MASTER_RPC_QUEUE_LENGTH.getName(),
        mRPCExecutor::getRpcQueueLength);
    // Create underlying gRPC server.
    GrpcServerBuilder builder = GrpcServerBuilder
        .forAddress(GrpcServerAddress.create(mRpcConnectAddress.getHostName(), mRpcBindAddress),
            ServerConfiguration.global(), ServerUserState.global())
        .executor(mRPCExecutor)
        .flowControlWindow(
            (int) ServerConfiguration.getBytes(PropertyKey.MASTER_NETWORK_FLOWCONTROL_WINDOW))
        .keepAliveTime(
            ServerConfiguration.getMs(PropertyKey.MASTER_NETWORK_KEEPALIVE_TIME_MS),
            TimeUnit.MILLISECONDS)
        .keepAliveTimeout(
            ServerConfiguration.getMs(PropertyKey.MASTER_NETWORK_KEEPALIVE_TIMEOUT_MS),
            TimeUnit.MILLISECONDS)
        .permitKeepAlive(
            ServerConfiguration.getMs(PropertyKey.MASTER_NETWORK_PERMIT_KEEPALIVE_TIME_MS),
            TimeUnit.MILLISECONDS)
        .maxInboundMessageSize((int) ServerConfiguration.getBytes(
            PropertyKey.MASTER_NETWORK_MAX_INBOUND_MESSAGE_SIZE));
    // Bind manifests of each Alluxio master to RPC server.
    for (Master master : mRegistry.getServers()) {
      registerServices(builder, master.getServices());
    }
    // Bind manifest of Alluxio JournalMaster service.
    // TODO(ggezer) Merge this with registerServices() logic.
    builder.addService(alluxio.grpc.ServiceType.JOURNAL_MASTER_CLIENT_SERVICE,
        new GrpcService(new JournalMasterClientServiceHandler(
            new DefaultJournalMaster(JournalDomain.MASTER, mJournalSystem))));
    // Builds a server that is not started yet.
    return builder.build();
  }

  protected void stopLeaderServing() {
    if (isGrpcServing()) {
      if (!mGrpcServer.shutdown()) {
        LOG.warn("Alluxio master RPC server shutdown timed out.");
      }
    }
    if (mRPCExecutor != null) {
      mRPCExecutor.shutdownNow();
      try {
        mRPCExecutor.awaitTermination(
            ServerConfiguration.getMs(PropertyKey.NETWORK_CONNECTION_SERVER_SHUTDOWN_TIMEOUT),
            TimeUnit.MILLISECONDS);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Stops all services.
   */
  protected void stopServing() throws Exception {
    stopLeaderServing();
    MetricsSystem.stopSinks();
    stopServingWebServer();
    // stop JVM monitor process
    if (mJvmPauseMonitor != null) {
      mJvmPauseMonitor.stop();
    }
  }

  protected void stopServingWebServer() throws Exception {
    if (mWebServer != null) {
      mWebServer.stop();
      mWebServer = null;
    }
  }

  /**
   * Waits until the web server is ready to serve requests.
   *
   * @param timeoutMs how long to wait in milliseconds
   * @return whether the web server became ready before the specified timeout
   */
  @VisibleForTesting
  public boolean waitForWebServerReady(int timeoutMs) {
    try {
      CommonUtils.waitFor(this + " to start",
          this::isWebServing, WaitForOptions.defaults().setTimeoutMs(timeoutMs));
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (TimeoutException e) {
      return false;
    }
  }

  /**
   * Indicates if all master resources have been successfully released when stopping.
   * An assumption made here is that a first call to {@link #stop()} might fail while a second call
   * might succeed.
   * @return whether {@link #stop()} has concluded successfully at least once
   */
  public boolean isStopped() {
    return mIsStopped.get();
  }

  @Override
  public String toString() {
    return "Alluxio master @" + mRpcConnectAddress;
  }

  /**
   * Factory for creating {@link AlluxioMasterProcess}.
   */
  @ThreadSafe
  public static final class Factory {
    /**
     * Creates a new {@link AlluxioMasterProcess}.
     *
     * @return a new instance of {@link MasterProcess} using the given sockets for the master
     */
    public static AlluxioMasterProcess create() {
      URI journalLocation = JournalUtils.getJournalLocation();
      JournalSystem journalSystem = new JournalSystem.Builder()
          .setLocation(journalLocation).build(ProcessType.MASTER);
      if (ServerConfiguration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
        Preconditions.checkState(!(journalSystem instanceof RaftJournalSystem),
            "Raft-based embedded journal and Zookeeper cannot be used at the same time.");
        PrimarySelector primarySelector = PrimarySelector.Factory.createZkPrimarySelector();
        return new FaultTolerantAlluxioMasterProcess(journalSystem, primarySelector);
      } else if (journalSystem instanceof RaftJournalSystem) {
        PrimarySelector primarySelector = ((RaftJournalSystem) journalSystem).getPrimarySelector();
        return new FaultTolerantAlluxioMasterProcess(journalSystem, primarySelector);
      }
      return new AlluxioMasterProcess(journalSystem);
    }

    private Factory() {} // prevent instantiation
  }
}
