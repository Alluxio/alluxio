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
import alluxio.grpc.GrpcServerBuilder;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalUtils;
import alluxio.master.journal.raft.RaftJournalSystem;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.sink.MetricsServlet;
import alluxio.metrics.sink.PrometheusMetricsServlet;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.JvmPauseMonitor;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.URIUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.web.MasterWebServer;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * An Alluxio Master which runs a web and rpc server to handle FileSystem operations.
 */
@NotThreadSafe
public class AlluxioMasterProcess extends MasterProcess {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioMasterProcess.class);

  /**
   * Lock for pausing modifications to master state. Holding the this lock allows a thread to
   * guarantee that no other threads will modify master state.
   */
  private final Lock mPauseStateLock;

  private final MetricsServlet mMetricsServlet = new MetricsServlet(MetricsSystem.METRIC_REGISTRY);
  private final PrometheusMetricsServlet mPMetricsServlet = new PrometheusMetricsServlet(
      MetricsSystem.METRIC_REGISTRY);

  /** The master registry. */
  private final MasterRegistry mRegistry;

  /** The JVMMonitor Progress. */
  private JvmPauseMonitor mJvmPauseMonitor;

  /** The connect address for the rpc server. */
  final InetSocketAddress mRpcConnectAddress;

  /** The manager of safe mode state. */
  protected final SafeModeManager mSafeModeManager;

  /** The manager for creating and restoring backups. */
  private final BackupManager mBackupManager;

  private ExecutorService mRPCExecutor = null;

  /**
   * Creates a new {@link AlluxioMasterProcess}.
   */
  AlluxioMasterProcess(JournalSystem journalSystem) {
    super(journalSystem, ServiceType.MASTER_RPC, ServiceType.MASTER_WEB);
    mRpcConnectAddress = NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC,
        ServerConfiguration.global());
    try {
      if (!mJournalSystem.isFormatted()) {
        throw new RuntimeException(
            String.format("Journal %s has not been formatted!", mJournalSystem));
      }
      // Create masters.
      mRegistry = new MasterRegistry();
      mSafeModeManager = new DefaultSafeModeManager();
      mBackupManager = new BackupManager(mRegistry);
      String baseDir = ServerConfiguration.get(PropertyKey.MASTER_METASTORE_DIR);
      MasterContext context = CoreMasterContext.newBuilder()
          .setJournalSystem(mJournalSystem)
          .setSafeModeManager(mSafeModeManager)
          .setBackupManager(mBackupManager)
          .setBlockStoreFactory(MasterUtils.getBlockStoreFactory(baseDir))
          .setInodeStoreFactory(MasterUtils.getInodeStoreFactory(baseDir))
          .setStartTimeMs(mStartTimeMs)
          .setPort(NetworkAddressUtils
              .getPort(ServiceType.MASTER_RPC, ServerConfiguration.global()))
          .build();
      mPauseStateLock = context.pauseStateLock();
      MasterUtils.createMasters(mRegistry, context);
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
    mJournalSystem.start();
    mJournalSystem.gainPrimacy();
    startMasters(true);
    startServing();
  }

  @Override
  public void stop() throws Exception {
    stopRejectingServers();
    if (isServing()) {
      stopMasters();
      stopServing();
      mJournalSystem.stop();
    }
  }

  private void initFromBackup(AlluxioURI backup) throws IOException {
    UnderFileSystem ufs;
    if (URIUtils.isLocalFilesystem(backup.toString())) {
      ufs = UnderFileSystem.Factory.create("/",
          UnderFileSystemConfiguration.defaults(ServerConfiguration.global()));
    } else {
      ufs = UnderFileSystem.Factory.createForRoot(ServerConfiguration.global());
    }
    try (UnderFileSystem closeUfs = ufs;
        InputStream ufsIn = ufs.open(backup.getPath())) {
      LOG.info("Initializing metadata from backup {}", backup);
      mBackupManager.initFromBackup(ufsIn);
    }
  }

  /**
   * Starts all masters, including block master, FileSystem master, and additional masters.
   *
   * @param isLeader if the Master is leader
   */
  protected void startMasters(boolean isLeader) {
    try {
      if (isLeader) {
        if (ServerConfiguration.isSet(PropertyKey.MASTER_JOURNAL_INIT_FROM_BACKUP)) {
          AlluxioURI backup =
              new AlluxioURI(ServerConfiguration.get(PropertyKey.MASTER_JOURNAL_INIT_FROM_BACKUP));
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
      LOG.info("All masters started");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Stops all masters, including block master, fileSystem master and additional masters.
   */
  protected void stopMasters() {
    try {
      mRegistry.stop();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Starts serving web ui server, resetting master web port, adding the metrics servlet to the web
   * server and starting web ui.
   */
  protected void startServingWebServer() {
    stopRejectingWebServer();
    mWebServer =
        new MasterWebServer(ServiceType.MASTER_WEB.getServiceName(), mWebBindAddress, this);
    // reset master web port
    // Add the metrics servlet to the web server.
    mWebServer.addHandler(mMetricsServlet.getHandler());
    // Add the prometheus metrics servlet to the web server.
    mWebServer.addHandler(mPMetricsServlet.getHandler());
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
          ServerConfiguration.getMs(PropertyKey.JVM_MONITOR_INFO_THRESHOLD_MS),
          ServerConfiguration.getMs(PropertyKey.JVM_MONITOR_WARN_THRESHOLD_MS));
      mJvmPauseMonitor.start();
    }
  }

  /**
   * Starts serving, letting {@link MetricsSystem} start sink and starting the web ui server and RPC
   * Server.
   *
   * @param startMessage empty string or the message that the master gains the leadership
   * @param stopMessage empty string or the message that the master loses the leadership
   */
  protected void startServing(String startMessage, String stopMessage) {
    MetricsSystem.startSinks(ServerConfiguration.get(PropertyKey.METRICS_CONF_FILE));
    startServingWebServer();
    startJvmMonitorProcess();
    LOG.info(
        "Alluxio master version {} started{}. bindAddress={}, connectAddress={}, webAddress={}",
        RuntimeConstants.VERSION, startMessage, mRpcBindAddress, mRpcConnectAddress,
        mWebBindAddress);
    startServingRPCServer();
    LOG.info("Alluxio master ended{}", stopMessage);
  }

  /**
   * Starts the gRPC server. The AlluxioMaster registers the Services of registered
   * {@link Master}s and meta services.
   */
  protected void startServingRPCServer() {
    try {
      stopRejectingRpcServer();
      LOG.info("Starting gRPC server on address {}", mRpcBindAddress);
      GrpcServerBuilder serverBuilder = GrpcServerBuilder.forAddress(
          mRpcConnectAddress.getHostName(), mRpcBindAddress, ServerConfiguration.global());

      mRPCExecutor = Executors.newFixedThreadPool(mMaxWorkerThreads,
          ThreadFactoryUtils.build("grpc-rpc-%d", true));

      serverBuilder.executor(mRPCExecutor);
      for (Master master : mRegistry.getServers()) {
        registerServices(serverBuilder, master.getServices());
      }

      mGrpcServer = serverBuilder.build().start();
      mSafeModeManager.notifyRpcServerStarted();
      LOG.info("Started gRPC server on address {}", mRpcConnectAddress);

      // Wait until the server is shut down.
      mGrpcServer.awaitTermination();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Stops serving, trying stop RPC server and web ui server and letting {@link MetricsSystem} stop
   * all the sinks.
   */
  protected void stopServing() throws Exception {
    if (isServing()) {
      if (!mGrpcServer.shutdown()) {
        LOG.warn("RPC Server shutdown timed out.");
      }
    }
    if (mRPCExecutor != null) {
      mRPCExecutor.shutdown();
      try {
        mRPCExecutor.awaitTermination(
            ServerConfiguration.getMs(PropertyKey.MASTER_GRPC_SERVER_SHUTDOWN_TIMEOUT),
            TimeUnit.MILLISECONDS);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      } finally {
        mRPCExecutor.shutdownNow();
      }
    }
    if (mJvmPauseMonitor != null) {
      mJvmPauseMonitor.stop();
    }
    if (mWebServer != null) {
      mWebServer.stop();
      mWebServer = null;
    }
    MetricsSystem.stopSinks();
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
      JournalSystem journalSystem =
          new JournalSystem.Builder().setLocation(journalLocation).build();
      if (ServerConfiguration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
        Preconditions.checkState(!(journalSystem instanceof RaftJournalSystem),
            "Raft journal cannot be used with Zookeeper enabled");
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
