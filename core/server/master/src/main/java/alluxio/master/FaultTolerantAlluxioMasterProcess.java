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
import alluxio.ProcessUtils;
import alluxio.PropertyKey;
import alluxio.master.PrimarySelector.State;
import alluxio.master.journal.JournalSystem;
import alluxio.util.CommonUtils;
import alluxio.util.ThreadUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.interfaces.Scoped;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The fault tolerant version of {@link AlluxioMaster} that uses zookeeper and standby masters.
 */
@NotThreadSafe
final class FaultTolerantAlluxioMasterProcess extends AlluxioMasterProcess {
  private static final Logger LOG =
      LoggerFactory.getLogger(FaultTolerantAlluxioMasterProcess.class);

  private final long mServingThreadTimeoutMs =
      Configuration.getMs(PropertyKey.MASTER_SERVING_THREAD_TIMEOUT);

  private PrimarySelector mLeaderSelector;
  private Thread mServingThread;

  /**
   * Creates a {@link FaultTolerantAlluxioMasterProcess}.
   */
  protected FaultTolerantAlluxioMasterProcess(JournalSystem journalSystem,
      PrimarySelector leaderSelector) {
    super(journalSystem);
    try {
      stopServing();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    mLeaderSelector = Preconditions.checkNotNull(leaderSelector, "leaderSelector");
    mServingThread = null;
  }

  @Override
  public void start() throws Exception {
    mJournalSystem.start();
    try {
      mLeaderSelector.start(getRpcAddress());
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw new RuntimeException(e);
    }

    startMasters(false);
    LOG.info("Secondary started");
    while (!Thread.interrupted()) {
      mLeaderSelector.waitForState(State.PRIMARY);
      if (gainPrimacy()) {
        mLeaderSelector.waitForState(State.SECONDARY);
        losePrimacy();
      }
    }
  }

  /**
   * Upgrades the master to primary mode.
   *
   * If the master loses primacy during the journal upgrade, this method will clean up the partial
   * upgrade, leaving the master in secondary mode.
   *
   * @return whether the master successfully upgraded to primary
   */
  private boolean gainPrimacy() throws Exception {
    // Don't upgrade if this master's primacy is unstable.
    AtomicBoolean unstable = new AtomicBoolean(false);
    try (Scoped scoped = mLeaderSelector.onStateChange(state -> unstable.set(true))) {
      if (mLeaderSelector.getState() != State.PRIMARY) {
        unstable.set(true);
      }
      stopMasters();
      LOG.info("Secondary stopped");
      mJournalSystem.gainPrimacy();
      // We only check unstable here because mJournalSystem.gainPrimacy() is the only slow method
      if (unstable.get()) {
        losePrimacy();
        return false;
      }
    }
    startMasters(true);
    mServingThread = new Thread(() -> {
      try {
        startServing(" (gained leadership)", " (lost leadership)");
      } catch (Throwable t) {
        Throwable root = ExceptionUtils.getRootCause(t);
        if ((root != null && (root instanceof InterruptedException)) || Thread.interrupted()) {
          return;
        }
        ProcessUtils.fatalError(LOG, t, "Exception thrown in main serving thread");
      }
    }, "MasterServingThread");
    mServingThread.start();
    if (!waitForReady(10 * Constants.MINUTE_MS)) {
      ThreadUtils.logAllThreads();
      throw new RuntimeException("Alluxio master failed to come up");
    }
    LOG.info("Primary started");
    return true;
  }

  private void losePrimacy() throws Exception {
    if (mServingThread != null) {
      stopServing();
    }
    // Put the journal in secondary mode ASAP to avoid interfering with the new primary. This must
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
    LOG.info("Secondary started");
  }

  @Override
  public void stop() throws Exception {
    super.stop();
    if (mLeaderSelector != null) {
      mLeaderSelector.stop();
    }
  }

  @Override
  public boolean waitForReady(int timeoutMs) {
    try {
      CommonUtils.waitFor(this + " to start", () -> (mServingThread == null || isServing()),
          WaitForOptions.defaults().setTimeoutMs(timeoutMs));
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (TimeoutException e) {
      return false;
    }
  }
}
