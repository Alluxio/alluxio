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
import alluxio.PropertyKey;
import alluxio.master.PrimarySelector.State;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalSystem.Mode;
import alluxio.util.CommonUtils;
import alluxio.util.ThreadUtils;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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

    while (!Thread.interrupted()) {
      startMasters(false);
      LOG.info("Secondary started");
      mLeaderSelector.waitForState(State.PRIMARY);
      mJournalSystem.setMode(Mode.PRIMARY);
      stopMasters();
      LOG.info("Secondary stopped");

      startMasters(true);
      mServingThread = new Thread(() -> {
        try {
          startServing(" (gained leadership)", " (lost leadership)");
        } catch (Throwable t) {
          Throwable root = ExceptionUtils.getRootCause(t);
          if ((root != null && (root instanceof InterruptedException)) || Thread.interrupted()) {
            return;
          }
          LOG.error("Exception thrown in main serving thread. System exiting.", t);
          System.exit(-1);
        }
      }, "MasterServingThread");
      mServingThread.start();
      waitForReady();
      LOG.info("Primary started");
      mLeaderSelector.waitForState(State.SECONDARY);
      stopServing();
      // Put the journal in secondary mode ASAP to avoid interfering with the new primary. This must
      // happen after stopServing because downgrading the journal system will reset master state,
      // which could cause NPEs for outstanding RPC threads. We need to first close all client
      // sockets in stopServing so that clients don't see NPEs.
      mJournalSystem.setMode(Mode.SECONDARY);
      mServingThread.join(mServingThreadTimeoutMs);
      if (mServingThread.isAlive()) {
        LOG.error(
            "Failed to stop serving thread after {}ms. Printing serving thread stack trace "
                + "and exiting.\n{}",
            mServingThreadTimeoutMs, ThreadUtils.formatStackTrace(mServingThread));
        System.exit(-1);
      }
      mServingThread = null;
      stopMasters();
      LOG.info("Primary stopped");
    }
  }

  @Override
  public void stop() throws Exception {
    super.stop();
    if (mLeaderSelector != null) {
      mLeaderSelector.stop();
    }
  }

  @Override
  public void waitForReady() {
    CommonUtils.waitFor(this + " to start", new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        return (mServingThread == null || isServing());
      }
    });
  }
}
