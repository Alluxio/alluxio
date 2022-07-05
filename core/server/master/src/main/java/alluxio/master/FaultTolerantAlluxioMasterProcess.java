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

import alluxio.Constants;
import alluxio.ProcessUtils;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.UnavailableException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.PrimarySelector.State;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalUtils;
import alluxio.master.journal.raft.RaftJournalSystem;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.security.user.ServerUserState;
import alluxio.util.CommonUtils;
import alluxio.util.ThreadUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.interfaces.Scoped;

import alluxio.util.io.FileUtils;
import alluxio.util.network.NetworkAddressUtils;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.ratis.protocol.RaftGroupId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The fault-tolerant version of {@link AlluxioMaster} that uses zookeeper and standby masters.
 */
@NotThreadSafe
final class FaultTolerantAlluxioMasterProcess extends AlluxioMasterProcess {
  private static final Logger LOG =
      LoggerFactory.getLogger(FaultTolerantAlluxioMasterProcess.class);

  private final long mServingThreadTimeoutMs =
      Configuration.getMs(PropertyKey.MASTER_SERVING_THREAD_TIMEOUT);

  private final PrimarySelector mLeaderSelector;
  private Thread mServingThread = null;

  /** See {@link #isRunning()}. */
  private volatile boolean mRunning = false;

  private ExecutorService mHeartbeatPool;
  private RatisHealthChecker mRatisHealthChecker;

  public volatile boolean restarted = false;

  /**
   * Creates a {@link FaultTolerantAlluxioMasterProcess}.
   */
  FaultTolerantAlluxioMasterProcess(JournalSystem journalSystem, PrimarySelector leaderSelector) {
    super(journalSystem);
    try {
      stopServing();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    mLeaderSelector = Preconditions.checkNotNull(leaderSelector, "leaderSelector");
    LOG.info("New process created.");
  }

  @Override
  public void start() throws Exception {
    LOG.info("Process starting.");
    mRunning = true;
    startCommonServices();
    mJournalSystem.start();
    startMasters(false);
    startJvmMonitorProcess();

    // Perform the initial catchup before joining leader election,
    // to avoid potential delay if this master is selected as leader
    if (Configuration.getBoolean(PropertyKey.MASTER_JOURNAL_CATCHUP_PROTECT_ENABLED)) {
      LOG.info("Waiting for journals to catch up.");
      mJournalSystem.waitForCatchup();
    }

    // Add a guard for Ratis Server
    if (mJournalSystem instanceof RaftJournalSystem) {
      // Create a health check thread
      mHeartbeatPool = ExecutorServiceFactories.cachedThreadPool("embedded-journal").create();
      LOG.info("Journal is RAFT, create a monitor for Ratis health status");

      // Find the RaftServer
      RaftJournalSystem raftJournalSystem = (RaftJournalSystem) mJournalSystem;
      mRatisHealthChecker = new RatisHealthChecker(raftJournalSystem.getServer(), this);
      mRatisHealthChecker.setGroupId(raftJournalSystem.getCurrentGroup().getGroupId());

      // TODO(jiacheng): This should be above the journal level so it can restart the journal
      mHeartbeatPool.submit(new HeartbeatThread(HeartbeatContext.MASTER_RATIS_SERVER_HEALTH_CHECK,
              mRatisHealthChecker, 1000, Configuration.global(), ServerUserState.global()));
      LOG.info("Ratis health checker thread created");
    }


    try {
      LOG.info("Starting leader selector.");
      mLeaderSelector.start(getRpcAddress());
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw new RuntimeException(e);
    }

    while (!Thread.interrupted()) {
      if (!mRunning) {
        LOG.info("FT is not running. Breaking out");
        break;
      }
      if (Configuration.getBoolean(PropertyKey.MASTER_JOURNAL_CATCHUP_PROTECT_ENABLED)) {
        LOG.info("Waiting for journals to catch up.");
        mJournalSystem.waitForCatchup();
      }

      LOG.info("Started in stand-by mode.");
      mLeaderSelector.waitForState(State.PRIMARY);
      if (!mRunning) {
        break;
      }
      try {
        if (!gainPrimacy()) {
          continue;
        }
      } catch (Throwable t) {
        if (Configuration.getBoolean(PropertyKey.MASTER_JOURNAL_BACKUP_WHEN_CORRUPTED)) {
          takeEmergencyBackup();
        }
        throw t;
      }
      mLeaderSelector.waitForState(State.STANDBY);
      if (Configuration.getBoolean(PropertyKey.MASTER_JOURNAL_EXIT_ON_DEMOTION)) {
        stop();
      } else {
        if (!mRunning) {
          break;
        }
        losePrimacy();
      }
    }
  }

  /**
   * Upgrades the master to primary mode.
   *
   * If the master loses primacy during the journal upgrade, this method will clean up the partial
   * upgrade, leaving the master in standby mode.
   *
   * @return whether the master successfully upgraded to primary
   */
  private boolean gainPrimacy() throws Exception {
    LOG.info("Becoming a leader.");
    // Don't upgrade if this master's primacy is unstable.
    AtomicBoolean unstable = new AtomicBoolean(false);
    try (Scoped scoped = mLeaderSelector.onStateChange(state -> unstable.set(true))) {
      if (mLeaderSelector.getState() != State.PRIMARY) {
        LOG.info("Lost leadership while becoming a leader.");
        unstable.set(true);
      }
      stopMasters();
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
          losePrimacy();
        }
        return false;
      }
    }
    try {
      startMasters(true);
    } catch (UnavailableException e) {
      LOG.warn("Error starting masters: {}", e.toString());
      stopMasters();
      return false;
    }
    mServingThread = new Thread(() -> {
      try {
        startCommonServices();
        startLeaderServing(" (gained leadership)", " (lost leadership)");
      } catch (Throwable t) {
        Throwable root = Throwables.getRootCause(t);
        if (root instanceof InterruptedException || Thread.interrupted()) {
          return;
        }
        ProcessUtils.fatalError(LOG, t, "Exception thrown in main serving thread");
      }
    }, "MasterServingThread");
    LOG.info("Starting a server thread.");
    mServingThread.start();
    if (!waitForReady(10 * Constants.MINUTE_MS)) {
      ThreadUtils.logAllThreads();
      throw new RuntimeException("Alluxio master failed to come up");
    }
    LOG.info("Primary started");
    return true;
  }

  private void losePrimacy() throws Exception {
    LOG.info("Losing the leadership.");
    if (mServingThread != null) {
      stopLeaderServing();
      stopCommonServicesIfNecessary();
    }
    // Put the journal in standby mode ASAP to avoid interfering with the new primary. This must
    // happen after stopServing because downgrading the journal system will reset master state,
    // which could cause NPEs for outstanding RPC threads. We need to first close all client
    // sockets in stopServing so that clients don't see NPEs.
    mJournalSystem.losePrimacy();
    if (mServingThread != null) {
      mServingThread.join(mServingThreadTimeoutMs);
      if (mServingThread.isAlive()) {
        ProcessUtils.fatalError(LOG,
            "Failed to stop serving thread after %dms. Serving thread stack trace:%n%s",
            mServingThreadTimeoutMs, ThreadUtils.formatStackTrace(mServingThread));
      }
      mServingThread = null;
      stopMasters();
      LOG.info("Primary stopped");
    }
    startMasters(false);
    LOG.info("Standby started");
  }

  public void restart() {
    LOG.info("Need to restart the Ratis server then recreate the master state. Now the master leadership state is {}",
        mLeaderSelector.getState());

    // If the master was previously primary, stop the services first
    if (mServingThread != null) {
      try {
        stopLeaderServing();
        stopCommonServicesIfNecessary();
      } catch (Exception e) {
        ProcessUtils.fatalError(LOG, e, "Failed to stop master running services during the restart. The process will exit.");
      }
    }

    LOG.info("Best-effort shut down RaftServer, the server is probably already dead:");
    try {
      mJournalSystem.stop();
    } catch (Exception e) {
      ProcessUtils.fatalError(LOG, e, "Failed to stop the existing journal system during the restart. The master will just exit.");
    }
    // Double check the AsyncFlushWriter?

    // If the master was previously primary, the serving thread needs to be recycled
    if (mServingThread != null) {
      try {
        mServingThread.join(mServingThreadTimeoutMs);
        if (mServingThread.isAlive()) {
          ProcessUtils.fatalError(LOG,
                  "Failed to stop serving thread after %dms. Serving thread stack trace:%n%s",
                  mServingThreadTimeoutMs, ThreadUtils.formatStackTrace(mServingThread));
        }
        mServingThread = null;
      } catch (Exception e) {
        ProcessUtils.fatalError(LOG, e, "Failed to stop serving thread during the restart. The process will exit.");
      }
    }
    try {
      stopMasters();
      LOG.info("All master modules stopped");
    } catch (Exception e) {
      ProcessUtils.fatalError(LOG, e, "Failed to stop master modules during the restart. The process will exit.");
    }


    // Move the journal folder to a recycle bin and recreate a clean journal path
    try {
      String journalDir = Configuration.getString(PropertyKey.MASTER_JOURNAL_FOLDER);
      String journalNewPath = journalDir + "_backup_" + new SimpleDateFormat("yyyyMMdd_hhmmss").format(new Date());
      LOG.info("Moving journals from {} to {}", journalDir, journalNewPath);
      FileUtils.move(journalDir, journalNewPath);
      LOG.info("Journal files moved from {} to {}", journalDir, journalNewPath);
      FileUtils.createDir(journalDir);
      LOG.info("Recreated journal dir {}, accessible: {}", journalDir, FileUtils.isStorageDirAccessible(journalDir));
    } catch (Exception e) {
      ProcessUtils.fatalError(LOG, e, "Failed to backup and format journal directory during the restart. The process will exit.");
    }

    // Recreate the JournalSystem
    URI journalLocation = JournalUtils.getJournalLocation();
    JournalSystem newJournalSystem = new JournalSystem.Builder()
            .setLocation(journalLocation).build(CommonUtils.ProcessType.MASTER);
    try {
      newJournalSystem.start();
    } catch (Exception e) {
      ProcessUtils.fatalError(LOG, e, "Failed to restart the raft journal. The process will quit and you will need to restart manually.");
    }

    // At the end, replace the reference with the new one
    LOG.info("Switch the master to the fresh Ratis journal and update the runtime context.");
    mJournalSystem = newJournalSystem;
    // TODO(jiacheng): replace the reference in the CoreMasterContext
    // Create masters.
    String baseDir = Configuration.getString(PropertyKey.MASTER_METASTORE_DIR);
    mContext = CoreMasterContext.newBuilder()
            .setJournalSystem(mJournalSystem)
            .setSafeModeManager(mSafeModeManager)
            .setBackupManager(mBackupManager)
            .setBlockStoreFactory(MasterUtils.getBlockStoreFactory(baseDir))
            .setInodeStoreFactory(MasterUtils.getInodeStoreFactory(baseDir))
            .setStartTimeMs(mStartTimeMs)
            .setPort(NetworkAddressUtils
                    .getPort(NetworkAddressUtils.ServiceType.MASTER_RPC, Configuration.global()))
            .setUfsManager(mUfsManager)
            .build();
    MasterUtils.createMasters(mRegistry, mContext);

    // Restart from the journal
    LOG.info("Restarting all master modules with the new journal.");
    try {
      startMasters(false);
    } catch (Exception e) {
      ProcessUtils.fatalError(LOG, e, "Failed to restart the master modules. The process will quit and you will need to restart manually.");
    }
    LOG.info("All master modules restarted with the new journal and master is now running as standby.");

    // Update the health monitor and everything
    RaftJournalSystem raftJournalSystem = (RaftJournalSystem) mJournalSystem;
    RaftGroupId newId = raftJournalSystem.getCurrentGroup().getGroupId();
    mRatisHealthChecker.setGroupId(newId);
    mRatisHealthChecker.setRaftServer(raftJournalSystem.getServer());
    LOG.info("Updated RAFT groupId and server");
    restarted = true;
    raftJournalSystem.setRecovered();
    LOG.info("Master restarted successfully");
  }

  @Override
  public void stop() throws Exception {
    synchronized (mIsStopped) {
      if (mIsStopped.get()) {
        return;
      }
      LOG.info("Stopping...");
      mRunning = false;
      stopCommonServices();
      if (mLeaderSelector != null) {
        mLeaderSelector.stop();
      }
      mHeartbeatPool.shutdown();
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

  @Override
  public boolean waitForGrpcServerReady(int timeoutMs) {
    try {
      CommonUtils.waitFor(this + " to start", () -> (mServingThread == null || isGrpcServing()),
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
  protected void startCommonServices() {
    boolean standbyMetricsSinkEnabled =
        Configuration.getBoolean(PropertyKey.STANDBY_MASTER_METRICS_SINK_ENABLED);
    boolean standbyWebEnabled =
        Configuration.getBoolean(PropertyKey.STANDBY_MASTER_WEB_ENABLED);
    // Masters will always start from standby state, and later be elected to primary.
    // If standby masters are enabled to start metric sink service,
    // the service will have been started before the master is promoted to primary.
    // Thus, when the master is primary, no need to start metric sink service again.
    //
    // Vice versa, if the standby masters do not start the metric sink service,
    // the master should start the metric sink when it is primacy.
    //
    LOG.info("state is {}, standbyMetricsSinkEnabled is {}, standbyWebEnabled is {}",
        mLeaderSelector.getState(), standbyMetricsSinkEnabled, standbyWebEnabled);
    if ((mLeaderSelector.getState() == State.STANDBY && standbyMetricsSinkEnabled)
        || (mLeaderSelector.getState() == State.PRIMARY && !standbyMetricsSinkEnabled)) {
      LOG.info("Start metric sinks.");
      MetricsSystem.startSinks(
          Configuration.getString(PropertyKey.METRICS_CONF_FILE));
    }

    // Same as the metrics sink service
    if ((mLeaderSelector.getState() == State.STANDBY && standbyWebEnabled)
        || (mLeaderSelector.getState() == State.PRIMARY && !standbyWebEnabled)) {
      LOG.info("Start web server.");
      startServingWebServer();
    }
  }

  void stopCommonServicesIfNecessary() throws Exception {
    if (!Configuration.getBoolean(
        PropertyKey.STANDBY_MASTER_METRICS_SINK_ENABLED)) {
      LOG.info("Stop metric sinks.");
      MetricsSystem.stopSinks();
    }
    if (!Configuration.getBoolean(
        PropertyKey.STANDBY_MASTER_WEB_ENABLED)) {
      LOG.info("Stop web server.");
      stopServingWebServer();
    }
  }
}
