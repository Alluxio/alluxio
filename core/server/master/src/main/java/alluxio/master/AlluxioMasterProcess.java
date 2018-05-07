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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.clock.SystemClock;
import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.NoMasterException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.block.BlockMaster;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalSystem.Mode;
import alluxio.master.meta.MetaMasterClientServiceHandler;
import alluxio.master.meta.MasterInfo;
import alluxio.master.meta.MetaMasterMasterClient;
import alluxio.master.meta.MetaMasterSync;
import alluxio.master.meta.ServerConfigurationChecker;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.sink.MetricsServlet;
import alluxio.metrics.sink.PrometheusMetricsServlet;
import alluxio.retry.ExponentialTimeBoundedRetry;
import alluxio.retry.RetryUtils;
import alluxio.security.authentication.TransportProvider;
import alluxio.thrift.MetaCommand;
import alluxio.thrift.MetaMasterClientService;
import alluxio.thrift.RegisterMasterTOptions;
import alluxio.util.CommonUtils;
import alluxio.util.IdUtils;
import alluxio.util.JvmPauseMonitor;
import alluxio.util.WaitForOptions;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.executor.ExecutorServiceFactory;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.web.MasterWebServer;
import alluxio.web.WebServer;
import alluxio.wire.ConfigProperty;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class encapsulates the different master services that are configured to run.
 */
@NotThreadSafe
public class AlluxioMasterProcess implements MasterProcess {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioMasterProcess.class);
  private static final long SHUTDOWN_TIMEOUT_MS = 10 * Constants.SECOND_MS;

  // Master metadata management.
  private static final IndexDefinition<MasterInfo> ID_INDEX =
      new IndexDefinition<MasterInfo>(true) {
        @Override
        public Object getFieldValue(MasterInfo o) {
          return o.getId();
        }
      };

  private static final IndexDefinition<MasterInfo> HOSTNAME_INDEX =
      new IndexDefinition<MasterInfo>(true) {
        @Override
        public Object getFieldValue(MasterInfo o) {
          return o.getHostname();
        }
      };

  /** Maximum number of threads to serve the rpc server. */
  private final int mMaxWorkerThreads;

  /** Minimum number of threads to serve the rpc server. */
  private final int mMinWorkerThreads;

  /** The port for the RPC server. */
  private final int mPort;

  /** The socket for thrift rpc server. */
  private TServerSocket mTServerSocket;

  /** The transport provider to create thrift server transport. */
  private final TransportProvider mTransportProvider;

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
  private TServer mThriftServer;

  /** is true if the master is serving the RPC server. */
  private boolean mIsServing;

  /** The start time for when the master started serving the RPC server. */
  private long mStartTimeMs = -1;

  /** The journal system for writing journal entries and restoring master state. */
  protected final JournalSystem mJournalSystem;

  /** The JVMMonitor Progress. */
  private JvmPauseMonitor mJvmPauseMonitor;

  /** The manager of safe mode state. */
  protected final SafeModeManager mSafeModeManager;

  /** The clock to use for determining the time. */
  private final Clock mClock;

  /** The master configuration checker. */
  private final ServerConfigurationChecker mMasterConfigChecker;

  /** The worker configuration checker. */
  private final ServerConfigurationChecker mWorkerConfigChecker;

  /** A factory for creating executor services when they are needed. */
  private ExecutorServiceFactory mExecutorServiceFactory;

  /** The executor used for running maintenance threads for the meta master. */
  private ExecutorService mExecutorService;

  /** The hostname of this master. */
  private String mMasterHostname;

  /** The master ID for this master. */
  private AtomicReference<Long> mMasterId;

  /** Client for all meta master communication. */
  private final MetaMasterMasterClient mMetaMasterClient;

  /** Runnable responsible for heartbeating and registration with leader master. */
  private MetaMasterSync mMetaMasterSync;

  /** Keeps track of standby masters which are in communication with the leader master. */
  private final IndexedSet<MasterInfo> mMasters =
      new IndexedSet<>(ID_INDEX, HOSTNAME_INDEX);
  /** Keeps track of standby masters which are no longer in communication with the leader master. */
  private final IndexedSet<MasterInfo> mLostMasters =
      new IndexedSet<>(ID_INDEX, HOSTNAME_INDEX);

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

      mTransportProvider = TransportProvider.Factory.create();
      mTServerSocket = new TServerSocket(NetworkAddressUtils.getBindAddress(ServiceType.MASTER_RPC),
          (int) Configuration.getMs(PropertyKey.MASTER_CONNECTION_TIMEOUT_MS));
      mPort = NetworkAddressUtils.getThriftPort(mTServerSocket);
      // reset master rpc port
      Configuration.set(PropertyKey.MASTER_RPC_PORT, Integer.toString(mPort));
      mRpcBindAddress = NetworkAddressUtils.getBindAddress(ServiceType.MASTER_RPC);
      mRpcConnectAddress = NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC);

      if (!mJournalSystem.isFormatted()) {
        throw new RuntimeException(
            String.format("Journal %s has not been formatted!", mJournalSystem));
      }
      // Create masters.
      mRegistry = new MasterRegistry();
      mSafeModeManager = new DefaultSafeModeManager();
      MasterUtils.createMasters(mJournalSystem, mRegistry, mSafeModeManager);

      // Create config checker
      mMasterConfigChecker = new ServerConfigurationChecker();
      mWorkerConfigChecker = new ServerConfigurationChecker();
      mClock = new SystemClock();
      mExecutorServiceFactory = ExecutorServiceFactories
          .fixedThreadPoolExecutorServiceFactory(Constants.META_MASTER_NAME, 2);
      mMetaMasterClient = new MetaMasterMasterClient(MasterClientConfig.defaults());
      setMasterIdAndHostname();

      // Register listeners for BlockMaster to interact with config checker
      BlockMaster blockMaster = mRegistry.get(BlockMaster.class);
      blockMaster.registerLostWorkerFoundListener(this::lostWorkerFoundHandler);
      blockMaster.registerWorkerLostListener(this::workerLostHandler);
      blockMaster.registerNewWorkerConfListener(this::registerNewWorkerConfHandler);
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
    return mIsServing;
  }

  @Override
  public List<ConfigProperty> getConfiguration() {
    List<ConfigProperty> configInfoList = new ArrayList<>();
    String alluxioConfPrefix = "alluxio";
    for (Map.Entry<String, String> entry : Configuration.toMap().entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(alluxioConfPrefix)) {
        PropertyKey propertyKey = PropertyKey.fromString(key);
        String source = Configuration.getFormattedSource(propertyKey);
        configInfoList.add(new ConfigProperty()
            .setName(key).setValue(entry.getValue()).setSource(source));
      }
    }
    return configInfoList;
  }

  @Override
  public long getMasterId(String hostname) {
    MasterInfo existingMaster = mMasters.getFirstByField(HOSTNAME_INDEX, hostname);
    if (existingMaster != null) {
      // This master hostname is already mapped to a master id.
      long oldMasterId = existingMaster.getId();
      LOG.warn("The master {} already exists as id {}.", hostname, oldMasterId);
      return oldMasterId;
    }

    MasterInfo lostMaster = mLostMasters.getFirstByField(HOSTNAME_INDEX, hostname);
    if (lostMaster != null) {
      // This is one of the lost masters
      mMasterConfigChecker.lostNodeFound(lostMaster.getId());
      synchronized (lostMaster) {
        final long lostMasterId = lostMaster.getId();
        LOG.warn("A lost master {} has requested its old id {}.", hostname, lostMasterId);

        // Update the timestamp of the master before it is considered an active master.
        lostMaster.updateLastUpdatedTimeMs();
        mMasters.add(lostMaster);
        mLostMasters.remove(lostMaster);
        return lostMasterId;
      }
    }

    // Generate a new master id.
    long masterId = IdUtils.getRandomNonNegativeLong();
    while (!mMasters.add(new MasterInfo(masterId, hostname))) {
      masterId = IdUtils.getRandomNonNegativeLong();
    }

    LOG.info("getMasterId(): Hostname: {} id: {}", hostname, masterId);
    return masterId;
  }

  @Override
  public void masterRegister(long masterId, RegisterMasterTOptions options)
      throws NoMasterException {
    MasterInfo master = mMasters.getFirstByField(ID_INDEX, masterId);
    if (master == null) {
      throw new NoMasterException(ExceptionMessage.NO_MASTER_FOUND.getMessage(masterId));
    }

    master.updateLastUpdatedTimeMs();

    List<ConfigProperty> configList = options.getConfigList().stream()
        .map(ConfigProperty::fromThrift).collect(Collectors.toList());
    mMasterConfigChecker.registerNewConf(masterId, configList);

    LOG.info("registerMaster(): master: {} options: {}", master, options);
  }

  @Override
  public MetaCommand masterHeartbeat(long masterId) {
    MasterInfo master = mMasters.getFirstByField(ID_INDEX, masterId);
    if (master == null) {
      LOG.warn("Could not find master id: {} for heartbeat.", masterId);
      return MetaCommand.Register;
    }

    synchronized (master) {
      master.updateLastUpdatedTimeMs();
    }
    return MetaCommand.Nothing;
  }

  @Override
  public void waitForReady() {
    CommonUtils.waitFor(this + " to start", new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        return mThriftServer != null && mThriftServer.isServing() && mWebServer != null
            && mWebServer.getServer().isRunning();
      }
    }, WaitForOptions.defaults().setTimeoutMs(10000));
  }

  @Override
  public void start() throws Exception {
    mJournalSystem.start();
    mJournalSystem.setMode(Mode.PRIMARY);
    startMasters(true);
    startServing();
  }

  @Override
  public void stop() throws Exception {
    if (mIsServing) {
      stopServing();
      stopMasters();
      mJournalSystem.stop();
      mIsServing = false;
    }
  }

  /**
   * Starts all masters, including block master, FileSystem master, lineage master and additional
   * masters.
   *
   * @param isLeader if the Master is leader
   */
  protected void startMasters(boolean isLeader) {
    try {
      mWorkerConfigChecker.reset();
      if (isLeader) {
        mSafeModeManager.notifyPrimaryMasterStarted();

        stopExecutorService();
        mExecutorService = mExecutorServiceFactory.create();

        //  The service that detects lost standby master nodes
        mExecutorService.submit(new HeartbeatThread(
            HeartbeatContext.MASTER_LOST_MASTER_DETECTION,
            new LostMasterDetectionHeartbeatExecutor(),
            (int) Configuration.getMs(PropertyKey.MASTER_HEARTBEAT_INTERVAL_MS)));
      } else {
        stopExecutorService();
        mExecutorService = mExecutorServiceFactory.create();

        // Standby master should setup MetaMasterSync to communicate with the leader master
        mMetaMasterSync = new MetaMasterSync(mMasterId, mMasterHostname, mMetaMasterClient);
        mExecutorService.submit(new HeartbeatThread(HeartbeatContext.MASTER_SYNC, mMetaMasterSync,
            (int) Configuration.getMs(PropertyKey.MASTER_HEARTBEAT_INTERVAL_MS)));
      }
      mRegistry.start(isLeader);
      LOG.info("All masters started");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Stops all masters, including lineage master, block master and fileSystem master and additional
   * masters.
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

  private void registerServices(TMultiplexedProcessor processor, Map<String, TProcessor> services) {
    for (Map.Entry<String, TProcessor> service : services.entrySet()) {
      processor.registerProcessor(service.getKey(), service.getValue());
    }
  }

  /**
   * Starts the Thrift RPC server. The AlluxioMaster registers the Services of registered
   * {@link Master}s and meta services to a multiplexed processor, then creates the master thrift
   * service with the multiplexed processor.
   */
  protected void startServingRPCServer() {
    // set up multiplexed thrift processors
    TMultiplexedProcessor processor = new TMultiplexedProcessor();
    // register master services
    for (Master master : mRegistry.getServers()) {
      registerServices(processor, master.getServices());
    }
    // register meta services
    processor.registerProcessor(Constants.META_MASTER_CLIENT_SERVICE_NAME,
        new MetaMasterClientService.Processor<>(new MetaMasterClientServiceHandler(this)));

    // Return a TTransportFactory based on the authentication type
    TTransportFactory transportFactory;
    try {
      String serverName = NetworkAddressUtils.getConnectHost(ServiceType.MASTER_RPC);
      transportFactory = mTransportProvider.getServerTransportFactory(serverName);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    try {
      if (mTServerSocket != null) {
        mTServerSocket.close();
      }
      mTServerSocket = new TServerSocket(mRpcBindAddress,
          (int) Configuration.getMs(PropertyKey.MASTER_CONNECTION_TIMEOUT_MS));
    } catch (TTransportException e) {
      throw new RuntimeException(e);
    }
    // create master thrift service with the multiplexed processor.
    Args args = new TThreadPoolServer.Args(mTServerSocket).maxWorkerThreads(mMaxWorkerThreads)
        .minWorkerThreads(mMinWorkerThreads).processor(processor).transportFactory(transportFactory)
        .protocolFactory(new TBinaryProtocol.Factory(true, true));

    args.stopTimeoutVal = (int) Configuration.getMs(PropertyKey.MASTER_THRIFT_SHUTDOWN_TIMEOUT);
    mThriftServer = new TThreadPoolServer(args);

    // start thrift rpc server
    mIsServing = true;
    mStartTimeMs = System.currentTimeMillis();
    mSafeModeManager.notifyRpcServerStarted();
    mThriftServer.serve();
  }

  /**
   * Stops serving, trying stop RPC server and web ui server and letting {@link MetricsSystem} stop
   * all the sinks.
   */
  protected void stopServing() throws Exception {
    if (mThriftServer != null) {
      mThriftServer.stop();
      mThriftServer = null;
    }
    if (mTServerSocket != null) {
      mTServerSocket.close();
      mTServerSocket = null;
    }
    if (mJvmPauseMonitor != null) {
      mJvmPauseMonitor.stop();
    }
    if (mWebServer != null) {
      mWebServer.stop();
      mWebServer = null;
    }
    MetricsSystem.stopSinks();
    stopExecutorService();
    mIsServing = false;
  }

  @Override
  public String toString() {
    return "Alluxio master @" + mRpcConnectAddress;
  }

  /**
   * Lost master periodic check.
   */
  private final class LostMasterDetectionHeartbeatExecutor implements HeartbeatExecutor {

    /**
     * Constructs a new {@link LostMasterDetectionHeartbeatExecutor}.
     */
    public LostMasterDetectionHeartbeatExecutor() {
    }

    @Override
    public void heartbeat() {
      int masterTimeoutMs = (int) Configuration.getMs(PropertyKey.MASTER_HEARTBEAT_TIMEOUT_MS);
      for (MasterInfo master : mMasters) {
        synchronized (master) {
          final long lastUpdate = mClock.millis() - master.getLastUpdatedTimeMs();
          if (lastUpdate > masterTimeoutMs) {
            LOG.error("The master {}({}) timed out after {}ms without a heartbeat!", master.getId(),
                master.getHostname(), lastUpdate);
            mLostMasters.add(master);
            mMasters.remove(master);
            mMasterConfigChecker.handleNodeLost(master.getId());
          }
        }
      }
    }

    @Override
    public void close() {
      // Nothing to clean up
    }
  }

  /**
   * Updates the config checker when a lost worker becomes alive.
   *
   * @param id the id of the worker
   */
  private void lostWorkerFoundHandler(long id) {
    mWorkerConfigChecker.lostNodeFound(id);
  }

  /**
   * Updates the config checker when a live worker becomes lost.
   *
   * @param id the id of the worker
   */
  private void workerLostHandler(long id) {
    mWorkerConfigChecker.handleNodeLost(id);
  }

  /**
   * Updates the config checker when a worker registers with configuration.
   *
   * @param id the id of the worker
   * @param configList the configuration of this worker
   */
  private void registerNewWorkerConfHandler(long id, List<ConfigProperty> configList) {
    mWorkerConfigChecker.registerNewConf(id, configList);
  }

  /**
   * Sets the master id and hostname.
   */
  private void setMasterIdAndHostname() {
    mMasterHostname = Configuration.get(PropertyKey.MASTER_HOSTNAME);
    try {
      RetryUtils.retry("get master id",
          () -> mMasterId.set(mMetaMasterClient.getId(mMasterHostname)),
          ExponentialTimeBoundedRetry.builder()
              .withMaxDuration(Duration
                  .ofMillis(Configuration.getMs(PropertyKey.USER_RPC_RETRY_MAX_NUM_RETRY)))
              .withInitialSleep(Duration.ofMillis(100))
              .withMaxSleep(Duration.ofSeconds(5))
              .build());
    } catch (Exception e) {
      throw new RuntimeException("Failed to get a master id from leader master: " + e.getMessage());
    }

    Preconditions.checkNotNull(mMasterId, "mMasterId");
    Preconditions.checkNotNull(mMasterHostname, "mMasterHostname");
  }

  /**
   * Stops the executor service.
   */
  private void stopExecutorService() {
    // Shut down the executor service, interrupting any running threads.
    if (mExecutorService != null) {
      try {
        mExecutorService.shutdownNow();
        String awaitFailureMessage =
            "waiting for {} executor service to shut down. Daemons may still be running";
        try {
          if (!mExecutorService.awaitTermination(SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            LOG.warn("Timed out " + awaitFailureMessage, this.getClass().getSimpleName());
          }
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while " + awaitFailureMessage, this.getClass().getSimpleName());
        }
      } finally {
        mExecutorService = null;
      }
    }
  }
}
