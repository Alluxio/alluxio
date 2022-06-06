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

import alluxio.master.PrimarySelector.State;
import alluxio.master.journal.JournalSystem;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The fault tolerant version of {@link AlluxioJobMaster} that uses zookeeper and standby masters.
 */
@NotThreadSafe
final class FaultTolerantAlluxioJobMasterProcess extends AlluxioJobMasterProcess {
  private static final Logger LOG =
      LoggerFactory.getLogger(FaultTolerantAlluxioJobMasterProcess.class);

  private final PrimarySelector mLeaderSelector;
  private Thread mServingThread = null;

  /**
   * Creates a {@link FaultTolerantAlluxioJobMasterProcess}.
   */
  FaultTolerantAlluxioJobMasterProcess(JournalSystem journalSystem,
      PrimarySelector leaderSelector) {
    super(journalSystem);
    try {
      stopServing();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    mLeaderSelector = Preconditions.checkNotNull(leaderSelector, "leaderSelector");
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
      if (mServingThread == null) {
        // We are in standby mode. Nothing to do until we become the primary.
        mLeaderSelector.waitForState(State.PRIMARY);
        LOG.info("Transitioning from standby to primary");
        mJournalSystem.gainPrimacy();
        stopMaster();
        LOG.info("Secondary stopped");
        startMaster(true);
        mServingThread = new Thread(() -> startServing(
                " (gained leadership)", " (lost leadership)"), "MasterServingThread");
        mServingThread.start();
        LOG.info("Primary started");
      } else {
        // We are in primary mode. Nothing to do until we become the standby.
        mLeaderSelector.waitForState(State.STANDBY);
        LOG.info("Transitioning from primary to standby");
        stopServing();
        mServingThread.join();
        mServingThread = null;
        stopMaster();
        mJournalSystem.losePrimacy();
        LOG.info("Primary stopped");
        startMaster(false);
        LOG.info("Standby started");
      }
    }
  }

  @Override
  public void stop() throws Exception {
    super.stop();
    mLeaderSelector.stop();
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
}
