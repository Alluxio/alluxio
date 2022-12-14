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
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.UnavailableException;
import alluxio.executor.ExecutorServiceBuilder;
import alluxio.grpc.BackupStatusPRequest;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.grpc.NodeState;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalUtils;
import alluxio.master.journal.raft.RaftJournalSystem;
import alluxio.master.journal.ufs.UfsJournalSingleMasterPrimarySelector;
import alluxio.master.meta.DefaultMetaMaster;
import alluxio.master.meta.MetaMaster;
import alluxio.master.service.SimpleService;
import alluxio.master.service.jvmmonitor.JvmMonitorService;
import alluxio.master.service.metrics.MetricsService;
import alluxio.master.service.rpc.RpcServerService;
import alluxio.master.service.web.WebServerService;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.CloseableResource;
import alluxio.underfs.MasterUfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.CommonUtils;
import alluxio.util.CommonUtils.ProcessType;
import alluxio.util.URIUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.interfaces.Scoped;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.web.MasterWebServer;
import alluxio.web.WebServer;
import alluxio.wire.BackupStatus;

import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * An Alluxio Master which runs a web and rpc server to handle FileSystem operations.
 */
@NotThreadSafe
public class AlluxioMasterProcess extends MasterProcess {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioMasterProcess.class);

  /** The manager of safe mode state. */
  protected final SafeModeManager mSafeModeManager = new DefaultSafeModeManager();

  /** Master context. */
  protected final CoreMasterContext mContext;

  /** The manager for creating and restoring backups. */
  private final BackupManager mBackupManager = new BackupManager(mRegistry);

  /** The manager of all ufs. */
  private final MasterUfsManager mUfsManager = new MasterUfsManager();

  /** See {@link #isStopped()}. */
  protected final AtomicBoolean mIsStopped = new AtomicBoolean(false);

  /** See {@link #isRunning()}. */
  private volatile boolean mRunning = false;

  /** last time this process gain primacy in ms. */
  private long mLastGainPrimacyTime = 0;

  /** last time this process lose primacy in ms. */
  private long mLastLosePrimacyTime = 0;

  /**
   * Creates a new {@link AlluxioMasterProcess}.
   */
  AlluxioMasterProcess(JournalSystem journalSystem, PrimarySelector leaderSelector) {
    super(journalSystem, leaderSelector, ServiceType.MASTER_WEB, ServiceType.MASTER_RPC);
    if (!mJournalSystem.isFormatted()) {
      throw new RuntimeException(
          String.format("Journal %s has not been formatted!", mJournalSystem));
    }
    // Create masters.
    mContext = createBaseMasterContext().build();
    MasterUtils.createMasters(mRegistry, mContext);
    if (Configuration.getBoolean(PropertyKey.MASTER_THROTTLE_ENABLED)) {
      mRegistry.get(alluxio.master.throttle.DefaultThrottleMaster.class).setMaster(this);
    }
    MetricsSystem.registerGaugeIfAbsent(
        MetricKey.MASTER_LAST_GAIN_PRIMACY_TIME.getName(),
        () -> mLastGainPrimacyTime);
    MetricsSystem.registerGaugeIfAbsent(
        MetricKey.MASTER_LAST_LOSE_PRIMACY_TIME.getName(),
        () -> mLastLosePrimacyTime);
    LOG.info("New process created.");
  }

  protected CoreMasterContext.Builder createBaseMasterContext() {
    String inodeStoreBaseDir = Configuration.getString(PropertyKey.MASTER_METASTORE_DIR_INODE);
    String blockStoreBaseDir = Configuration.getString(PropertyKey.MASTER_METASTORE_DIR_BLOCK);
    return CoreMasterContext.newBuilder()
        .setJournalSystem(mJournalSystem)
        .setPrimarySelector(mLeaderSelector)
        .setSafeModeManager(mSafeModeManager)
        .setBackupManager(mBackupManager)
        .setBlockStoreFactory(MasterUtils.getBlockStoreFactory(blockStoreBaseDir))
        .setInodeStoreFactory(MasterUtils.getInodeStoreFactory(inodeStoreBaseDir))
        .setStartTimeMs(mStartTimeMs)
        .setPort(NetworkAddressUtils.getPort(ServiceType.MASTER_RPC, Configuration.global()))
        .setUfsManager(mUfsManager);
  }

  @Override
  public WebServer createWebServer() {
    return new MasterWebServer(ServiceType.MASTER_WEB.getServiceName(), mWebBindAddress, this);
  }

  @Override
  public GrpcServerBuilder createBaseRpcServer() {
    return GrpcServerBuilder
        .forAddress(GrpcServerAddress.create(mRpcConnectAddress.getHostName(), mRpcBindAddress),
            Configuration.global())
        .flowControlWindow(
            (int) Configuration.getBytes(PropertyKey.MASTER_NETWORK_FLOWCONTROL_WINDOW))
        .keepAliveTime(
            Configuration.getMs(PropertyKey.MASTER_NETWORK_KEEPALIVE_TIME_MS),
            TimeUnit.MILLISECONDS)
        .keepAliveTimeout(
            Configuration.getMs(PropertyKey.MASTER_NETWORK_KEEPALIVE_TIMEOUT_MS),
            TimeUnit.MILLISECONDS)
        .permitKeepAlive(
            Configuration.getMs(PropertyKey.MASTER_NETWORK_PERMIT_KEEPALIVE_TIME_MS),
            TimeUnit.MILLISECONDS)
        .maxInboundMessageSize((int) Configuration.getBytes(
            PropertyKey.MASTER_NETWORK_MAX_INBOUND_MESSAGE_SIZE));
  }

  @Override
  public Optional<AlluxioExecutorService> createRpcExecutorService() {
    AlluxioExecutorService executor = ExecutorServiceBuilder.buildExecutorService(
        ExecutorServiceBuilder.RpcExecutorHost.MASTER);
    MetricsSystem.removeMetrics(MetricKey.MASTER_RPC_QUEUE_LENGTH.getName());
    MetricsSystem.registerGaugeIfAbsent(MetricKey.MASTER_RPC_QUEUE_LENGTH.getName(),
        executor::getRpcQueueLength);
    MetricsSystem.removeMetrics(MetricKey.MASTER_RPC_THREAD_ACTIVE_COUNT.getName());
    MetricsSystem.registerGaugeIfAbsent(MetricKey.MASTER_RPC_THREAD_ACTIVE_COUNT.getName(),
        executor::getActiveCount);
    MetricsSystem.removeMetrics(MetricKey.MASTER_RPC_THREAD_CURRENT_COUNT.getName());
    MetricsSystem.registerGaugeIfAbsent(MetricKey.MASTER_RPC_THREAD_CURRENT_COUNT.getName(),
        executor::getPoolSize);
    return Optional.of(executor);
  }

  @Override
  public Optional<SafeModeManager> getSafeModeManager() {
    return Optional.of(mSafeModeManager);
  }

  /**
   * @return true if Alluxio is running in safe mode, false otherwise
   */
  public boolean isInSafeMode() {
    return mSafeModeManager.isInSafeMode();
  }

  @Override
  public void start() throws Exception {
    LOG.info("Process starting.");
    mRunning = true;
    mServices.forEach(SimpleService::start);
    mJournalSystem.start();
    startMasterComponents(false);

    // Perform the initial catchup before joining leader election,
    // to avoid potential delay if this master is selected as leader
    if (Configuration.getBoolean(PropertyKey.MASTER_JOURNAL_CATCHUP_PROTECT_ENABLED)) {
      LOG.info("Waiting for journals to catch up.");
      mJournalSystem.waitForCatchup();
    }

    LOG.info("Starting leader selector.");
    mLeaderSelector.start(getRpcAddress());

    while (!Thread.interrupted()) {
      if (!mRunning) {
        LOG.info("master process is not running. Breaking out");
        break;
      }
      if (Configuration.getBoolean(PropertyKey.MASTER_JOURNAL_CATCHUP_PROTECT_ENABLED)) {
        LOG.info("Waiting for journals to catch up.");
        mJournalSystem.waitForCatchup();
      }

      LOG.info("Started in stand-by mode.");
      mLeaderSelector.waitForState(NodeState.PRIMARY);
      mLastGainPrimacyTime = CommonUtils.getCurrentMs();
      if (!mRunning) {
        break;
      }
      try {
        if (!promote()) {
          continue;
        }
        mServices.forEach(SimpleService::promote);
        LOG.info("Primary started");
      } catch (Throwable t) {
        if (Configuration.getBoolean(PropertyKey.MASTER_JOURNAL_BACKUP_WHEN_CORRUPTED)) {
          takeEmergencyBackup();
        }
        throw t;
      }
      mLeaderSelector.waitForState(NodeState.STANDBY);
      mLastLosePrimacyTime = CommonUtils.getCurrentMs();
      if (Configuration.getBoolean(PropertyKey.MASTER_JOURNAL_EXIT_ON_DEMOTION)) {
        stop();
      } else {
        if (!mRunning) {
          break;
        }
        LOG.info("Losing the leadership.");
        mServices.forEach(SimpleService::demote);
        demote();
      }
    }
  }

  /**
   * Upgrades the master to primary mode.
   * If the master loses primacy during the journal upgrade, this method will clean up the partial
   * upgrade, leaving the master in standby mode.
   *
   * @return whether the master successfully upgraded to primary
   */
  private boolean promote() throws Exception {
    LOG.info("Becoming a leader.");
    // Don't upgrade if this master's primacy is unstable.
    AtomicBoolean unstable = new AtomicBoolean(false);
    try (Scoped scoped = mLeaderSelector.onStateChange(state -> unstable.set(true))) {
      if (mLeaderSelector.getState() != NodeState.PRIMARY) {
        LOG.info("Lost leadership while becoming a leader.");
        unstable.set(true);
      }
      stopMasterComponents();
      LOG.info("Standby stopped");
      try (Timer.Context ctx = MetricsSystem
          .timer(MetricKey.MASTER_JOURNAL_GAIN_PRIMACY_TIMER.getName()).time()) {
        mJournalSystem.gainPrimacy();
      }
      // We only check unstable here because mJournalSystem.gainPrimacy() is the only slow method
      if (unstable.get()) {
        LOG.info("Terminating an unstable attempt to become a leader.");
        if (Configuration.getBoolean(PropertyKey.MASTER_JOURNAL_EXIT_ON_DEMOTION)) {
          stop();
        } else {
          demote();
        }
        return false;
      }
    }
    try {
      startMasterComponents(true);
    } catch (UnavailableException e) {
      LOG.warn("Error starting masters: {}", e.toString());
      mJournalSystem.losePrimacy();
      stopMasterComponents();
      return false;
    }
    return true;
  }

  private void demote() throws Exception {
    // Put the journal in standby mode ASAP to avoid interfering with the new primary. This must
    // happen after stopServing because downgrading the journal system will reset master state,
    // which could cause NPEs for outstanding RPC threads. We need to first close all client
    // sockets in stopServing so that clients don't see NPEs.
    mJournalSystem.losePrimacy();
    stopMasterComponents();
    LOG.info("Primary stopped");
    startMasterComponents(false);
    LOG.info("Standby started");
  }

  private void initFromBackup(AlluxioURI backup) throws IOException {
    CloseableResource<UnderFileSystem> ufsResource;
    if (URIUtils.isLocalFilesystem(backup.toString())) {
      UnderFileSystem ufs = UnderFileSystem.Factory.create("/",
          UnderFileSystemConfiguration.defaults(Configuration.global()));
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
    // When restoring from backup, some fs modifications exist only in UFS. We invalidate the root
    // to force new accesses to sync with UFS first to update our picture of the UFS.
    if (Configuration.getBoolean(PropertyKey.MASTER_JOURNAL_SYNC_ROOT_AFTER_INIT_FROM_BACKUP)) {
      try {
        mRegistry.get(FileSystemMaster.class).needsSync(new AlluxioURI("/"));
        LOG.info("Marked root as needing sync after backup restore");
      } catch (InvalidPathException e) {
        LOG.warn("Failed to mark root as needing syncing after backup restore");
      }
    }
  }

  protected void takeEmergencyBackup() throws AlluxioException, InterruptedException,
      TimeoutException {
    LOG.warn("Emergency backup triggered");
    DefaultMetaMaster metaMaster = (DefaultMetaMaster) mRegistry.get(MetaMaster.class);
    BackupStatus backup = metaMaster.takeEmergencyBackup();
    BackupStatusPRequest statusRequest =
        BackupStatusPRequest.newBuilder().setBackupId(backup.getBackupId().toString()).build();
    final int requestIntervalMs = 2_000;
    CommonUtils.waitFor("emergency backup to complete", () -> {
      try {
        BackupStatus status = metaMaster.getBackupStatus(statusRequest);
        LOG.info("Auto backup state: {} | Entries processed: {}.", status.getState(),
            status.getEntryCount());
        return status.isCompleted();
      } catch (AlluxioException e) {
        return false;
      }
      // no need for timeout on shutdown, we must wait until the backup is complete
    }, WaitForOptions.defaults().setInterval(requestIntervalMs).setTimeoutMs(Integer.MAX_VALUE));
  }

  /**
   * Starts all masters, including block master, FileSystem master, and additional masters.
   *
   * @param isLeader if the Master is leader
   */
  protected void startMasterComponents(boolean isLeader) throws IOException {
    LOG.info("Starting all master components as: {}.", (isLeader) ? "leader" : "follower");
    if (isLeader) {
      if (Configuration.isSet(PropertyKey.MASTER_JOURNAL_INIT_FROM_BACKUP)) {
        AlluxioURI backup =
            new AlluxioURI(Configuration.getString(PropertyKey.MASTER_JOURNAL_INIT_FROM_BACKUP));
        if (mJournalSystem.isEmpty()) {
          initFromBackup(backup);
        } else {
          LOG.info("The journal system is not freshly formatted, skipping restoring backup from {}",
              backup);
        }
      }
      mSafeModeManager.notifyPrimaryMasterStarted();
    }
    mRegistry.start(isLeader);
    // Signal state-lock-manager that masters are ready.
    mContext.getStateLockManager().mastersStartedCallback();
    LOG.info("All masters started.");
  }

  /**
   * Stops all masters, including block master, fileSystem master and additional masters.
   */
  protected void stopMasterComponents() {
    try {
      LOG.info("Stopping all masters components.");
      mRegistry.stop();
      LOG.info("All master components stopped.");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void stop() throws Exception {
    synchronized (mIsStopped) {
      if (mIsStopped.get()) {
        return;
      }
      LOG.info("Stopping...");
      mRunning = false;
      mServices.forEach(SimpleService::stop);
      mJournalSystem.stop();
      LOG.info("Closing all master components.");
      mRegistry.close();
      LOG.info("Closed all master components.");
      mLeaderSelector.stop();
      mIsStopped.set(true);
      LOG.info("Stopped.");
    }
  }

  /**
   * @return {@code true} when {@link #start()} has been called and {@link #stop()} has not yet
   * been called, {@code false} otherwise
   */
  boolean isRunning() {
    return mRunning;
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
      final PrimarySelector primarySelector;
      if (Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
        Preconditions.checkState(!(journalSystem instanceof RaftJournalSystem),
            "Raft-based embedded journal and Zookeeper cannot be used at the same time.");
        primarySelector = PrimarySelector.Factory.createZkPrimarySelector();
      } else if (journalSystem instanceof RaftJournalSystem) {
        primarySelector = ((RaftJournalSystem) journalSystem).getPrimarySelector();
      } else {
        primarySelector = new UfsJournalSingleMasterPrimarySelector();
      }
      AlluxioMasterProcess amp = new AlluxioMasterProcess(journalSystem, primarySelector);
      amp.registerService(
          RpcServerService.Factory.create(amp.getRpcBindAddress(), amp, amp.getRegistry()));
      amp.registerService(WebServerService.Factory.create(amp.getWebBindAddress(), amp));
      amp.registerService(MetricsService.Factory.create());
      amp.registerService(JvmMonitorService.Factory.create());
      return amp;
    }

    private Factory() {} // prevent instantiation
  }
}
