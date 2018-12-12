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

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.master.journal.JournalSystem;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.sink.MetricsServlet;
import alluxio.metrics.sink.PrometheusMetricsServlet;
import alluxio.security.authentication.ClientIpAddressInjector;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.CommonUtils;
import alluxio.util.JvmPauseMonitor;
import alluxio.util.URIUtils;
import alluxio.util.WaitForOptions;
import alluxio.grpc.GrpcServer;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.web.MasterWebServer;
import alluxio.web.WebServer;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.grpc.BindableService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class encapsulates the different master services that are configured to run.
 */
@NotThreadSafe
public class AlluxioMasterProcess implements MasterProcess {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioMasterProcess.class);

  /** Maximum number of threads to awaitTermination the rpc server. */
  private final int mMaxWorkerThreads;

  /** Minimum number of threads to awaitTermination the rpc server. */
  private final int mMinWorkerThreads;

  /** The port for the RPC server. */
  private final int mPort;

  /**
   * Lock for pausing modifications to master state. Holding the this lock allows a thread to
   * guarantee that no other threads will modify master state.
   */
  private final Lock mPauseStateLock;

  /** The bind address for the rpc server. */
  private final InetSocketAddress mRpcBindAddress;

  /** The connect address for the rpc server. */
  private final InetSocketAddress mRpcConnectAddress;

  private final MetricsServlet mMetricsServlet = new MetricsServlet(MetricsSystem.METRIC_REGISTRY);
  private final PrometheusMetricsServlet mPMetricsServlet = new PrometheusMetricsServlet(
      MetricsSystem.METRIC_REGISTRY);

  /** The master registry. */
  private final MasterRegistry mRegistry;

  /** The web ui server. */
  private WebServer mWebServer;

  /** The RPC server. */
  private GrpcServer mGrpcServer;

  /** The start time for when the master started. */
  private final long mStartTimeMs = System.currentTimeMillis();

  /** The journal system for writing journal entries and restoring master state. */
  protected final JournalSystem mJournalSystem;

  /** The JVMMonitor Progress. */
  private JvmPauseMonitor mJvmPauseMonitor;

  /** The manager of safe mode state. */
  protected final SafeModeManager mSafeModeManager;

  /** The manager for creating and restoring backups. */
  private final BackupManager mBackupManager;

  /**
   * Creates a new {@link AlluxioMasterProcess}.
   */
  AlluxioMasterProcess(JournalSystem journalSystem) {
    mJournalSystem = Preconditions.checkNotNull(journalSystem, "journalSystem");
    mMinWorkerThreads = Configuration.getInt(PropertyKey.MASTER_WORKER_THREADS_MIN);
    mMaxWorkerThreads = Configuration.getInt(PropertyKey.MASTER_WORKER_THREADS_MAX);
    int connectionTimeout = (int) Configuration.getMs(PropertyKey.MASTER_CONNECTION_TIMEOUT_MS);

    Preconditions.checkArgument(mMaxWorkerThreads >= mMinWorkerThreads,
        PropertyKey.MASTER_WORKER_THREADS_MAX + " can not be less than "
            + PropertyKey.MASTER_WORKER_THREADS_MIN);

    if (connectionTimeout > 0) {
      LOG.debug("{} connection timeout[{}] is {}", this, PropertyKey.MASTER_CONNECTION_TIMEOUT_MS,
          connectionTimeout);
    }
    try {
      // Extract the port from the generated socket.
      // When running tests, it is fine to use port '0' so the system will figure out what port to
      // use (any random free port).
      // In a production or any real deployment setup, port '0' should not be used as it will make
      // deployment more complicated.
      if (!Configuration.getBoolean(PropertyKey.TEST_MODE)) {
        Preconditions.checkState(Configuration.getInt(PropertyKey.MASTER_RPC_PORT) > 0,
            this + " rpc port is only allowed to be zero in test mode.");
        Preconditions.checkState(Configuration.getInt(PropertyKey.MASTER_WEB_PORT) > 0,
            this + " web port is only allowed to be zero in test mode.");
      }

      // Random port binding.
      InetSocketAddress configuredBindAddress =
          NetworkAddressUtils.getBindAddress(ServiceType.MASTER_RPC);
      if (configuredBindAddress.getPort() == 0) {
        mGrpcServer = GrpcServerBuilder.forAddress(configuredBindAddress).build().start();
        mPort = mGrpcServer.getPort();
        Configuration.set(PropertyKey.MASTER_RPC_PORT, Integer.toString(mPort));
      } else {
        mPort = configuredBindAddress.getPort();
      }

      mRpcBindAddress = NetworkAddressUtils.getBindAddress(ServiceType.MASTER_RPC);
      mRpcConnectAddress = NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC);

      if (!mJournalSystem.isFormatted()) {
        throw new RuntimeException(
            String.format("Journal %s has not been formatted!", mJournalSystem));
      }
      // Create masters.
      mRegistry = new MasterRegistry();
      mSafeModeManager = new DefaultSafeModeManager();
      mBackupManager = new BackupManager(mRegistry);
      MasterContext context =
          new MasterContext(mJournalSystem, mSafeModeManager, mBackupManager, mStartTimeMs, mPort);
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

  @Override
  public InetSocketAddress getRpcAddress() {
    return mRpcConnectAddress;
  }

  @Override
  public long getStartTimeMs() {
    return mStartTimeMs;
  }

  @Override
  public long getUptimeMs() {
    return System.currentTimeMillis() - mStartTimeMs;
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
  public boolean isInSafeMode() {
    return mSafeModeManager.isInSafeMode();
  }

  @Override
  public boolean isServing() {
    return mGrpcServer != null && mGrpcServer.isServing();
  }

  @Override
  public boolean waitForReady(int timeoutMs) {
    try {
      CommonUtils.waitFor(this + " to start",
          () -> mGrpcServer != null && mGrpcServer.isServing() && mWebServer != null
              && mWebServer.getServer().isRunning(),
          WaitForOptions.defaults().setTimeoutMs(timeoutMs));
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (TimeoutException e) {
      return false;
    }
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
    if (isServing()) {
      stopServing();
      stopMasters();
      mJournalSystem.stop();
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
        if (Configuration.isSet(PropertyKey.MASTER_JOURNAL_INIT_FROM_BACKUP)) {
          AlluxioURI backup =
              new AlluxioURI(Configuration.get(PropertyKey.MASTER_JOURNAL_INIT_FROM_BACKUP));
          if (mJournalSystem.isEmpty()) {
            initFromBackup(backup);
          } else {
            LOG.info("The journal system is not freshly formatted, skipping restoring backup from "
                + backup);
          }
        }
        mSafeModeManager.notifyPrimaryMasterStarted();
      }
      mRegistry.start(isLeader);
      LOG.info("All masters started");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void initFromBackup(AlluxioURI backup) throws IOException {
    UnderFileSystem ufs;
    if (URIUtils.isLocalFilesystem(backup.toString())) {
      ufs = UnderFileSystem.Factory.create("/", UnderFileSystemConfiguration.defaults());
    } else {
      ufs = UnderFileSystem.Factory.createForRoot();
    }
    try (UnderFileSystem closeUfs = ufs;
         InputStream ufsIn = ufs.open(backup.getPath())) {
      LOG.info("Initializing metadata from backup {}", backup);
      mBackupManager.initFromBackup(ufsIn);
    }
  }

  /**
   * Stops all masters, including block master, fileSystem master and additional masters.
   */
  protected void stopMasters() {
    try {
      mRegistry.stop();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private void startServing() {
    startServing("", "");
  }

  /**
   * Starts serving, letting {@link MetricsSystem} start sink and starting the web ui server and RPC
   * Server.
   *
   * @param startMessage empty string or the message that the master gains the leadership
   * @param stopMessage empty string or the message that the master loses the leadership
   */
  protected void startServing(String startMessage, String stopMessage) {
    MetricsSystem.startSinks();
    startServingWebServer();
    startJvmMonitorProcess();
    LOG.info("Alluxio master version {} started{}. "
            + "bindHost={}, connectHost={}, rpcPort={}, webPort={}",
        RuntimeConstants.VERSION,
        startMessage,
        NetworkAddressUtils.getBindAddress(ServiceType.MASTER_RPC),
        NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC),
        NetworkAddressUtils.getPort(ServiceType.MASTER_RPC),
        NetworkAddressUtils.getPort(ServiceType.MASTER_WEB));
    startServingRPCServer();
    LOG.info("Alluxio master ended{}", stopMessage);
  }

  /**
   * Starts serving web ui server, resetting master web port, adding the metrics servlet to the web
   * server and starting web ui.
   */
  protected void startServingWebServer() {
    mWebServer = new MasterWebServer(ServiceType.MASTER_WEB.getServiceName(),
        NetworkAddressUtils.getBindAddress(ServiceType.MASTER_WEB), this);
    // reset master web port
    Configuration.set(PropertyKey.MASTER_WEB_PORT, Integer.toString(mWebServer.getLocalPort()));
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
    if (Configuration.getBoolean(PropertyKey.MASTER_JVM_MONITOR_ENABLED)) {
      mJvmPauseMonitor = new JvmPauseMonitor();
      mJvmPauseMonitor.start();
    }
  }

  private void registerServices(GrpcServerBuilder serverBuilder, Map<String, BindableService> services) {
    for (Map.Entry<String, BindableService> service : services.entrySet()) {
      serverBuilder.addService( service.getValue());
      LOG.info("registered service {}", service.getKey());
    }
  }

  /**
   * Starts the gRPC server. The AlluxioMaster registers the Services of registered
   * {@link Master}s and meta services.
   */
  protected void startServingRPCServer() {
    // TODO(ggezer) gRPC is leaking executor's threads.
    //ExecutorService executorService = Executors.newFixedThreadPool(mMaxWorkerThreads);
    try {
      if(mGrpcServer!= null) {
        // Server launched for auto bind.
        // Terminate it.
        mGrpcServer.shutdown();
        mGrpcServer.awaitTermination();
        mGrpcServer.shutdownNow();
      }

      LOG.info("Starting gRPC server on address {}", mRpcBindAddress);
      GrpcServerBuilder serverBuilder = GrpcServerBuilder.forAddress(mRpcBindAddress);
      for (Master master : mRegistry.getServers()) {
        registerServices(serverBuilder, master.getServices());
      }
      // Expose version service from the server.
      serverBuilder.addService(new AlluxioVersionServiceHandler());
      // Register interceptor for providing audit context with remote client's IP Address
      serverBuilder.intercept(new ClientIpAddressInjector());

      mGrpcServer = serverBuilder.build().start();
      mSafeModeManager.notifyRpcServerStarted();
      LOG.info("Started gRPC server on address {}", mRpcBindAddress);

      // Wait until the server is shut down.
      mGrpcServer.awaitTermination();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Stops serving, trying shutdown RPC server and web ui server and letting {@link MetricsSystem} shutdown
   * all the sinks.
   */
  protected void stopServing() throws Exception {
    if (mGrpcServer != null) {
      mGrpcServer.shutdown();
      mGrpcServer.awaitTermination();
      mGrpcServer.shutdownNow();
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
}
